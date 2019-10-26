import datetime
import logging
import pytz

from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.tornado import TornadoScheduler
from arctic import Arctic, TICK_STORE
from cloudwall.serenity.mdrecorder.coinbase import CoinbaseProSnapshotClient
from coinbasepro.exceptions import CoinbaseAPIError
from tornado.ioloop import IOLoop

# initialize logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
console_logger = logging.StreamHandler()
console_logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_logger.setFormatter(formatter)
logger.addHandler(console_logger)

# initialize Arctic
snapshotter = CoinbaseProSnapshotClient()
arctic = Arctic('localhost')
arctic.initialize_library('COINBASE_PRO_ONE_MIN_SNAP', lib_type=TICK_STORE)
tick_lib = arctic['COINBASE_PRO_ONE_MIN_SNAP']


def on_tick():
    tick_logger = logging.getLogger(__name__)
    try:
        last = snapshotter.snap_last_trade('BTC-USD')
        rxt = datetime.datetime.now(pytz.utc)
        px = float(last['price'])
        qty = float(last['size'])
        last_row = [{'index': rxt, 'price': px, 'qty': qty}]
        tick_lib.write('BTC-USD', last_row, metadata={'source': 'CoinbasePro'})
        tick_logger.info("wrote latest trade to Arctic: {} @ {}".format(qty, px))
    except CoinbaseAPIError:
        tick_logger.info("ignoring transient error from Coinbase Pro API; will retry")


if __name__ == '__main__':
    scheduler = TornadoScheduler()
    scheduler.add_jobstore(MemoryJobStore())
    scheduler.add_executor(TornadoExecutor())

    scheduler.add_job(on_tick, 'interval', minutes=1)
    scheduler.start()

    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        logger.info("starting Tornado")
        IOLoop.instance().start()
    except (KeyboardInterrupt, SystemExit):
        pass
