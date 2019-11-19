import datetime
import pandas as pd
import re
import shutil

from abc import ABC, abstractmethod
from pathlib import Path


class BiTimestamp:
    """
    A bitemporal timestamp combining as-at time and as-of time.
    """

    # cannot use datetime.min / datetime.max due to limitations
    # see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#representing-out-of-bounds-spans
    start_as_of = pd.Timestamp.min.to_pydatetime(warn=False)
    latest_as_of = pd.Timestamp.max.to_pydatetime(warn=False)

    def __init__(self, as_at_date: datetime.date, as_of_time: datetime.datetime = latest_as_of):
        self.as_at_date = as_at_date
        self.as_of_time = as_of_time
        pass

    def as_at(self) -> datetime.date:
        return self.as_at_date

    def as_of(self) -> datetime.datetime:
        return self.as_of_time

    def __str__(self) -> str:
        return str((self.as_at_date, self.as_of_time))


class Tickstore(ABC):
    """
    Base class for all implementations of tickstores.
    """

    @abstractmethod
    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime = BiTimestamp.latest_as_of) -> pd.DataFrame:
        """
        Selects all ticks between start and end timestamps, optionally restricted to version effective as of as_of_time.
        :return: a DataFrame with the content matching the query
        """
        pass

    @abstractmethod
    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        """
        For a given symbol insert ticks at the given date, either newly creating an entry if none for
        that date or logically overwriting by creating a new version of the ticks for that date.
        """
        pass

    @abstractmethod
    def delete(self, symbol: str, ts: BiTimestamp):
        """
        For a given symbol and date logically delete its content effective now.
        """
        pass

    def flush(self):
        """
        Writes to disk, uploads data or otherwise commits any transient state without fully closing the store.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Releases any resources associated with the tickstore.
        """
        pass

    @abstractmethod
    def destroy(self):
        """
        Destroys the entire tickstore.
        """
        pass


class DataFrameIndex:
    """
    HDF5- and Pandas-based multi-level index used by LocalTickstore.
    """

    def __init__(self, base_path: Path, index_path: Path, table_name: str):
        self.base_path = base_path
        self.index_path = index_path
        self.table_name = table_name
        self.dirty = False

        if not self.index_path.exists():
            # initialize index; for backward compatibility support generating an index
            # from directories and files only. in this mode we also rewrite the paths
            # to support the bitemporal storage engine.
            index_rows = []
            splay_paths = list(base_path.rglob("*.h5"))
            for path in splay_paths:
                # extract date
                parts = path.parts
                year = int(parts[-4])
                month = int(parts[-3])
                day = int(parts[-2])
                splay_date = datetime.date(year, month, day)

                # extract symbol
                filename = parts[-1]
                symbol_search = re.search(r'(.*?)_(\d+)\.h5', filename)
                if symbol_search:
                    symbol = symbol_search.group(1)
                    symbol_version = int(symbol_search.group(2))
                else:
                    # another backward compatibility step: if we don't have a version number
                    # embedded in the filename rename the file to version zero. note we
                    # only support 10K versions per symbol with this convention
                    symbol_search = re.search(r'(.*)\.h5', filename)
                    symbol = symbol_search.group(1)
                    symbol_version = 0
                    path.rename(Path(path.parent.joinpath(Path(symbol + '_0000.h5'))))

                index_rows.append({'symbol': symbol,
                                   'date': splay_date,
                                   'start_time': BiTimestamp.start_as_of,
                                   'end_time': BiTimestamp.latest_as_of,
                                   'version': symbol_version,
                                   'path': str(path)
                                   })

            # build a DataFrame with multi-level index and then save to compressed HDF5. since we will
            # need to build the index after first rows inserted also capture whether it's an empty index.
            if len(index_rows) > 0:
                index_df = pd.DataFrame(index_rows)
            else:
                index_df = pd.DataFrame(columns=['symbol',
                                                 'date',
                                                 'start_time',
                                                 'end_time',
                                                 'version',
                                                 'path'])

            index_df.set_index(['symbol', 'date'], inplace=True)
            self.df = index_df

            self._mark_dirty()
            self.flush()
        else:
            # noinspection PyTypeChecker
            existing_index: pd.DataFrame = pd.read_hdf(str(self.index_path))
            self.df = existing_index

    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime) -> pd.DataFrame:
        # short circuit if symbol missing
        if not self.df.index.get_level_values('symbol').contains(symbol):
            return pd.DataFrame()

        # find all dates in range where as_of_time is between start_time and end_time
        symbol_data = self.df.loc[symbol]
        mask = (symbol_data.index.get_level_values('date') >= start) \
            & (symbol_data.index.get_level_values('date') <= end) \
            & (symbol_data['start_time'] <= as_of_time) \
            & (symbol_data['end_time'] >= as_of_time)
        selected = self.df.loc[symbol][mask]
        return selected

    def insert(self, symbol: str, as_at_date: datetime.date, create_write_path_func) -> Path:
        # if there's at least one entry in the index for this (symbol, as_at_date
        # increment the version and set the start/end times such that the previous
        # version is logically deleted and the next version becomes latest
        if self.df.index.isin([(symbol, as_at_date)]).any():
            all_versions = self.df.loc[[(symbol, as_at_date)]]

            start_time = datetime.datetime.utcnow()
            end_time = BiTimestamp.latest_as_of

            prev_version = all_versions['version'][-1]
            version = prev_version + 1
            all_versions.loc[(all_versions['version'] == prev_version), 'end_time'] = start_time

            self.df.update(all_versions)
        else:
            start_time = BiTimestamp.start_as_of
            end_time = BiTimestamp.latest_as_of
            version = 0

        write_path = create_write_path_func(version)

        # was not able to figure out a way to insert into a MultiIndex without re-setting the
        # index columns and de-duping but I suspect there is a nicer way to do the below
        new_index_row = pd.DataFrame.from_dict({'symbol': [symbol],
                                                'date': [as_at_date],
                                                'start_time': [start_time],
                                                'end_time': [end_time],
                                                'version': [version],
                                                'path': [str(write_path)]
                                                })
        new_index_row.set_index(['symbol', 'date'], inplace=True)
        self.df = self.df.append(new_index_row)

        # selects don't work properly unless you remove duplicates
        self.df = self.df.loc[~self.df.index.duplicated(keep='first')]

        return write_path

    def delete(self, symbol: str, as_at_date: datetime.date):
        if self.df.index.isin([(symbol, as_at_date)]).any():
            all_versions = self.df.loc[[(symbol, as_at_date)]]
            start_time = datetime.datetime.utcnow()

            # logically delete by setting the most recent version to end now; note this means that deletes
            # don't have a version number or row, so may want to think about this
            prev_version = all_versions['version'][-1]
            all_versions.loc[(all_versions['version'] == prev_version), 'end_time'] = start_time

            self.df.update(all_versions)

    def flush(self):
        self.df.to_hdf(str(self.index_path), self.table_name, mode='w', append=False, complevel=9, complib='blosc')
        self._mark_dirty(False)

    def _mark_dirty(self, dirty=True):
        self.dirty = dirty

    def __del__(self):
        self.flush()


class LocalTickstore(Tickstore):
    """
    Tickstore meant to run against local disk for maximum performance.
    """

    def __init__(self, base_path: Path, timestamp_column: str = 'date'):
        self.base_path = base_path.resolve()
        self.timestamp_column = timestamp_column

        # initialize storage location
        self.base_path.mkdir(parents=True, exist_ok=True)

        # initialize and potentially build the index
        # extract table name
        table_name = self.base_path.parts[-1]
        self.index = DataFrameIndex(base_path, base_path.joinpath(Path('index.h5')), table_name)

        # initialize state
        self.closed = False

    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime = BiTimestamp.latest_as_of) -> pd.DataFrame:
        self._check_closed('select')

        # pass 1: grab the list of splays matching the start / end range that are valid for as_of_time
        selected = self.index.select(symbol, start, end, as_of_time)
        if selected.empty:
            return selected

        # load all ticks in range into memory
        loaded_dfs = []
        for index, row in selected.iterrows():
            loaded_dfs.append(pd.read_hdf(row['path']))

        # pass 2: select ticks matching the exact start/end timestamps
        all_ticks = pd.concat(loaded_dfs)
        time_mask = (all_ticks.index.get_level_values(self.timestamp_column) >= start) \
            & (all_ticks.index.get_level_values(self.timestamp_column) <= end)

        # sort the ticks -- probably need to optimize this to sort on paths and sort ticks on ingest
        selected_ticks = all_ticks.loc[time_mask]
        selected_ticks.sort_index(inplace=True)
        return selected_ticks

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        self._check_closed('insert')
        as_at_date = ts.as_at()

        # compose a splay path based on YYYY/MM/DD, symbol and version and pass in as a functor
        # so it can be populated with the bitemporal version
        def create_write_path(version):
            return self.base_path.joinpath('{}/{:02d}/{:02d}/{}_{:04d}.h5'.format(as_at_date.year,
                                                                                  as_at_date.month,
                                                                                  as_at_date.day,
                                                                                  symbol, version))

        write_path = self.index.insert(symbol, as_at_date, create_write_path)

        # do the tick write, with blosc compression
        write_path.parent.mkdir(parents=True, exist_ok=True)
        ticks.to_hdf(str(write_path), 'ticks', mode='w', append=False, complevel=9, complib='blosc')

    def delete(self, symbol: str, ts: BiTimestamp):
        self._check_closed('delete')
        self.index.delete(symbol, ts.as_at_date)

    def destroy(self):
        if self.base_path.exists():
            shutil.rmtree(self.base_path)

    def flush(self):
        self.index.flush()

    def close(self):
        if not self.closed:
            self.index.flush()
            self.closed = True

    def _check_closed(self, operation):
        if self.closed:
            raise Exception('unable to perform operation while closed: ' + operation)

    def __del__(self):
        self.close()


class AzureBlobTickstore(Tickstore):
    """
    Tickstore meant to run against Microsoft's Azure Blob Storage backend, e.g. for archiving purposes. Note this is
    not suitable for concurrent access to the blob because the index is loaded into memory on the local node and only
    written back to the blob on close. We may want to implement blob locking to at least prevent accidents.
    """

    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime = BiTimestamp.latest_as_of) -> pd.DataFrame:
        raise NotImplementedError

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        raise NotImplementedError

    def delete(self, symbol: str, ts: BiTimestamp):
        raise NotImplementedError

    def flush(self):
        pass

    def close(self):
        pass

    def destroy(self):
        pass
