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

    def __init__(self, as_at_date: datetime.date, as_of_time: datetime.datetime = datetime.datetime.max):
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
               as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
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


class LocalTickstore(Tickstore):
    """
    Tickstore meant to run against local disk for maximum performance.
    """

    def __init__(self, base_path: Path):
        self.base_path = base_path.resolve()
        self.index_path = base_path.joinpath(Path('index.h5'))

        # initialize storage location
        self.base_path.mkdir(parents=True, exist_ok=True)

        # extract table name
        self.table_name = self.base_path.parts[-1]

        # initialize state flags
        self.dirty = False
        self.closed = False

        if not self.index_path.exists():
            # initialize index; for backward compatibility support generating an index
            # from directories and files only. in this mode we also rewrite the paths
            # to support the bitemporal storage engine.
            index_rows = []
            splay_paths = list(self.base_path.rglob("*.h5"))
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
                                   'start_time': datetime.datetime.min,
                                   'end_time': datetime.datetime.max,
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
            self.index = index_df

            self._mark_dirty()
            self._save_index()
        else:
            self.index = pd.read_hdf(str(self.index_path))

    # noinspection PyUnresolvedReferences
    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
        self._check_closed('select')

        symbol_data = self.index.loc[symbol]
        mask = (symbol_data.index.get_level_values('date') >= start) \
            & (symbol_data.index.get_level_values('date') <= end) \
            & (symbol_data['end_time'] <= as_of_time)
        selected = self.index.loc[symbol][mask]

        loaded_dfs = []
        for index, row in selected.iterrows():
            loaded_dfs.append(pd.read_hdf(row['path']))

        return pd.concat(loaded_dfs)

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        self._check_closed('insert')
        as_at_date = ts.as_at()

        # if there's at least one entry in the index for this (symbol, as_at_date
        # increment the version and set the start/end times such that the previous
        # version is logically deleted and the next version becomes latest
        if self.index.index.isin([(symbol, as_at_date)]).any():
            all_versions = self.index.loc[[(symbol, as_at_date)]]

            start_time = datetime.datetime.utcnow()
            end_time = datetime.datetime.max

            prev_version = all_versions['version'][-1]
            version = prev_version + 1
            all_versions.loc[(all_versions['version'] == prev_version), 'end_time'] = start_time

            self.index.update(all_versions)
        else:
            start_time = datetime.datetime.min
            end_time = datetime.datetime.max
            version = 0

        # compose a splay path based on YYYY/MM/DD, symbol and version
        write_path = self.base_path.joinpath('{}/{:02d}/{:02d}/{}_{:04d}.h5'.format(as_at_date.year,
                                                                                    as_at_date.month,
                                                                                    as_at_date.day,
                                                                                    symbol, version))
        write_path_txt = str(write_path)

        # was not able to figure out a way to insert into a MultiIndex without setting the
        # index columns but I suspect there is a nicer way to do the below
        new_index_row = pd.DataFrame.from_dict({'symbol': [symbol],
                                                'date': [as_at_date],
                                                'start_time': [start_time],
                                                'end_time': [end_time],
                                                'version': [version],
                                                'path': [write_path_txt]
                                                })
        new_index_row.set_index(['symbol', 'date'], inplace=True)
        self.index = self.index.append(new_index_row)
        self.index = self.index.loc[~self.index.index.duplicated(keep='first')]

        # do the write, with blosc compression
        write_path.parent.mkdir(parents=True, exist_ok=True)
        ticks.to_hdf(write_path_txt, 'ticks', mode='w', append=False, complevel=9, complib='blosc')

        self._mark_dirty()

    def delete(self, symbol: str, ts: BiTimestamp):
        self._check_closed('delete')
        self._mark_dirty()
        raise NotImplementedError

    def destroy(self):
        if self.base_path.exists():
            shutil.rmtree(self.base_path)

    def close(self):
        if self.dirty:
            self._save_index()
            self.closed = True

    def _check_closed(self, operation):
        if self.closed:
            raise Exception('unable to perform operation while closed: ' + operation)

    def _mark_dirty(self, dirty=True):
        self.dirty = dirty

    def _save_index(self):
        # noinspection PyUnresolvedReferences
        self.index.to_hdf(str(self.index_path), self.table_name, mode='w', append=False, complevel=9, complib='blosc')
        self._mark_dirty(False)


class AzureBlobTickstore(Tickstore):
    """
    Tickstore meant to run against Microsoft's Azure Blob Storage backend, e.g. for archiving purposes. Note this is
    not suitable for concurrent access to the blob because the index is loaded into memory on the local node and only
    written back to the blob on close. We may want to implement blob locking to at least prevent accidents.
    """

    def select(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
               as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
        raise NotImplementedError

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        raise NotImplementedError

    def delete(self, symbol: str, ts: BiTimestamp):
        raise NotImplementedError

    def close(self):
        pass

    def destroy(self):
        pass
