import datetime
import logging
import pandas as pd

from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.triggers.cron import CronTrigger
from cloudwall.serenity.mdrecorder.journal import Journal
from cloudwall.serenity.mdrecorder.tickstore import LocalTickstore, BiTimestamp
from cloudwall.serenity.mdrecorder.utils import init_logging
from pathlib import Path
from tornado.ioloop import IOLoop


def upload_ticks_daily():
    func_logger = logging.getLogger(__name__)
    upload_date = datetime.datetime.utcnow().date() - datetime.timedelta(1)

    symbol = 'BTC-USD'
    journal = Journal(Path('/behemoth/journals/COINBASE_PRO_TRADES/' + symbol))
    reader = journal.create_reader(upload_date)
    length = reader.get_length()
    records = []
    while reader.get_pos() < length:
        time = reader.read_double()
        sequence = reader.read_long()
        trade_id = reader.read_long()
        product_id = reader.read_string()
        side = 'buy' if reader.read_short() == 0 else 'sell'
        size = reader.read_double()
        price = reader.read_double()

        record = {
            'time': datetime.datetime.fromtimestamp(time),
            'sequence': sequence,
            'trade_id': trade_id,
            'product_id': product_id,
            'side': side,
            'size': size,
            'price': price
        }
        records.append(record)

    func_logger.info("uploading journaled ticks to Behemoth for UTC date " + str(upload_date))
    df = pd.DataFrame(records)
    df.set_index('time', inplace=True)
    func_logger.info("extracted {} records".format(len(df)))

    tickstore = LocalTickstore(Path('/behemoth/db/COINBASE_PRO_TRADES'), 'time')
    tickstore.insert(symbol, BiTimestamp(upload_date), df)
    tickstore.close()

    func_logger.info("inserted {} records".format(len(df)))


if __name__ == '__main__':
    init_logging()

    scheduler = TornadoScheduler()
    scheduler.add_jobstore(MemoryJobStore())
    scheduler.add_executor(TornadoExecutor())

    scheduler.add_job(upload_ticks_daily, CronTrigger(hour=0, minute=15, second=0, timezone='UTC'))
    scheduler.start()

    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        logger = logging.getLogger(__name__)
        logger.info("starting Tornado")
        IOLoop.instance().start()
    except (KeyboardInterrupt, SystemExit):
        pass
