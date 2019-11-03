import datetime
import numpy as np
import pandas as pd

from cloudwall.serenity.mdrecorder.tickstore import LocalTickstore, BiTimestamp
from pathlib import Path
from pytest_mock import MockFixture


def test_tickstore(mocker: MockFixture):
    tickstore = LocalTickstore(Path('./COINBASE_PRO_ONE_MIN_BINS'))

    for i in range(31):
        ticks = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
        tickstore.insert('BTC-USD', BiTimestamp(datetime.date(2019, 10, i+1)), ticks)

    df = tickstore.select('BTC-USD', start=datetime.datetime(2019, 10, 1), end=datetime.datetime(2019, 10, 15))
    assert df.size == 6000

    tickstore.close()
    tickstore.destroy()
