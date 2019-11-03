import datetime
import numpy as np
import pandas as pd

from cloudwall.serenity.mdrecorder.tickstore import LocalTickstore, BiTimestamp
from pathlib import Path
from pytest_mock import MockFixture


def test_tickstore(mocker: MockFixture):
    ts_col_name = 'ts'
    tickstore = LocalTickstore(Path('./COINBASE_PRO_ONE_MIN_BINS'), timestamp_column=ts_col_name)

    for i in range(31):
        start = pd.to_datetime('2019-10-1')
        end = pd.to_datetime('2019-10-30')
        ts_index = random_dates(start, end, 100)
        ts_index.name = ts_col_name
        ticks = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'), index=ts_index)
        tickstore.insert('BTC-USD', BiTimestamp(datetime.date(2019, 10, i+1)), ticks)

    df = tickstore.select('BTC-USD', start=datetime.datetime(2019, 10, 1), end=datetime.datetime(2019, 10, 15))

    # because timestamps are random the number of matches is not deterministic. is there a better way to test this?
    assert df.size > 0

    tickstore.close()
    tickstore.destroy()


def random_dates(start, end, n):
    start_u = start.value//10**9
    end_u = end.value//10**9
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')
