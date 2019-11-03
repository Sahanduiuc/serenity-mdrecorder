import datetime

import pandas as pd

from cloudwall.serenity.mdrecorder.tickstore import LocalTickstore, BiTimestamp
from pathlib import Path
from pytest_mock import MockFixture


def test_tickstore(mocker: MockFixture):
    tickstore = LocalTickstore(Path('./COINBASE_PRO_ONE_MIN_BINS'))

    for i in range(31):
        tickstore.insert('BTC-USD', BiTimestamp(datetime.date(2019, 10, i+1)), pd.DataFrame())

    tickstore.close()
