import datetime
import pandas as pd

from abc import ABC, abstractmethod


class BiTimestamp:
    """
    A bitemporal timestamp combining as-at time and as-of time.
    """
    def __init__(self, as_at_time: datetime.datetime, as_of_time: datetime.datetime = datetime.datetime.max):
        self.as_at_time = as_at_time
        self.as_of_time = as_of_time
        pass

    def as_at(self) -> datetime.datetime:
        return self.as_at_time

    def as_of(self) -> datetime.datetime:
        return self.as_of_time

    def __str__(self) -> str:
        return str((self.as_at_time, self.as_of_time))


class Tickstore(ABC):
    """
    Base class for all implementations of tickstores.
    """

    @abstractmethod
    def query(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
              as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
        pass

    @abstractmethod
    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        pass

    @abstractmethod
    def delete(self, symbol: str, ts: BiTimestamp):
        pass


class LocalTickstore(Tickstore):
    """
    Tickstore meant to run against local disk for maximum performance.
    """

    def query(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
              as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
        raise NotImplementedError

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        raise NotImplementedError

    def delete(self, symbol: str, ts: BiTimestamp):
        raise NotImplementedError


class AzureBlobTickstore(Tickstore):
    """
    Tickstore meant to run against Microsoft's Azure Blob Storage backend, e.g. for archiving purposes
    """

    def query(self, symbol: str, start: datetime.datetime, end: datetime.datetime,
              as_of_time: datetime.datetime = datetime.datetime.max) -> pd.DataFrame:
        raise NotImplementedError

    def insert(self, symbol: str, ts: BiTimestamp, ticks: pd.DataFrame):
        raise NotImplementedError

    def delete(self, symbol: str, ts: BiTimestamp):
        raise NotImplementedError
