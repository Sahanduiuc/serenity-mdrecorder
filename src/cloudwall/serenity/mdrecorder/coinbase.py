import datetime
import fire
import json
import logging

from cloudwall.serenity.mdrecorder.journal import Journal
from cloudwall.serenity.mdrecorder.utils import init_logging
from pathlib import Path
from tornado import httpclient
from tornado import httputil
from tornado import websocket
from tornado.ioloop import IOLoop, PeriodicCallback


APPLICATION_JSON = 'application/json'
DEFAULT_KEEPALIVE_TIMEOUT_MILLIS = 1000
DEFAULT_CONNECT_TIMEOUT_SEC = 60
DEFAULT_REQUEST_TIMEOUT_SEC = 60


class CoinbaseProSubscriber:
    logger = logging.getLogger(__name__)

    def __init__(self, symbol: str, journal: Journal, loop: IOLoop = IOLoop.instance(),
                 url: str = 'wss://ws-feed.pro.coinbase.com',
                 keep_alive_timeout: int = DEFAULT_KEEPALIVE_TIMEOUT_MILLIS,
                 connect_timeout: int = DEFAULT_CONNECT_TIMEOUT_SEC,
                 request_timeout: int = DEFAULT_REQUEST_TIMEOUT_SEC):
        self.symbol = symbol
        self.appender = journal.create_appender()
        self.url = url
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

        self._ws_connection = None
        self.loop = loop

        # noinspection PyTypeChecker
        PeriodicCallback(self._keep_alive, keep_alive_timeout).start()

    async def connect(self):
        self.logger.info("connecting to {} and subscribing to {} trades".format(self.url, self.symbol))
        headers = httputil.HTTPHeaders({'Content-Type': APPLICATION_JSON})
        request = httpclient.HTTPRequest(url=self.url,
                                         connect_timeout=self.connect_timeout,
                                         request_timeout=self.request_timeout,
                                         headers=headers)

        # noinspection PyAttributeOutsideInit
        self._ws_connection = await websocket.websocket_connect(request)

        subscribe_msg = {
            'type': 'subscribe',
            'product_ids': [self.symbol],
            'channels': ['matches', 'heartbeat']
        }
        self.send(json.dumps(subscribe_msg))

        while True:
            msg = await self._ws_connection.read_message()
            if msg is None:
                self._on_connection_close()
                break

            self._on_message(msg)

    def send(self, data: str):
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is closed.')

        self._ws_connection.write_message(data)

    def close(self):
        if not self.appender:
            self.appender.close()
            self.appender = None

        if not self._ws_connection:
            self._ws_connection.close()
            self._ws_connection = None

    def _on_message(self, msg_txt):
        if msg_txt:
            msg = json.loads(msg_txt)
            if msg['type'] == 'match':
                self.appender.write_double(datetime.datetime.utcnow().timestamp())
                self.appender.write_long(msg['sequence'])
                self.appender.write_long(msg['trade_id'])
                self.appender.write_string(msg['product_id'])
                self.appender.write_short(1 if msg['side'] == 'buy' else 0)
                self.appender.write_double(float(msg['size']))
                self.appender.write_double(float(msg['price']))
        else:
            self._on_connection_close()
        pass

    async def _keep_alive(self):
        if self._ws_connection is None:
            self.logger.info('Disconnected; attempting to reconnect in keep alive timer')
            await self.connect()

    def _on_connection_close(self):
        self._ws_connection = None
        pass


def subscribe_coinbase_trades(journal_path: str = '/behemoth/journals/COINBASE_PRO_TRADES/BTC-USD'):
    logger = logging.getLogger(__name__)

    journal = Journal(Path(journal_path))
    subscriber = CoinbaseProSubscriber('BTC-USD', journal)

    logger.info("journaling ticks to {}".format(journal_path))

    IOLoop.instance().run_sync(subscriber.connect)
    IOLoop.instance().start()


if __name__ == '__main__':
    init_logging()
    fire.Fire(subscribe_coinbase_trades)
