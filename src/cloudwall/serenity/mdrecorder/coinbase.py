from coinbasepro import PublicClient
from tornado import httpclient
from tornado import httputil
from tornado import websocket
from tornado.ioloop import IOLoop

from cloudwall.serenity.mdrecorder.api import MDSnapshotClient

APPLICATION_JSON = 'application/json'
DEFAULT_CONNECT_TIMEOUT = 60
DEFAULT_REQUEST_TIMEOUT = 60


class CoinbaseProSnapshotClient(MDSnapshotClient):
    def __init__(self):
        super().__init__()
        self.client = PublicClient()

    def snap_last_trade(self, symbol: str) -> dict:
        return self.client.get_product_ticker(symbol)


class CoinbaseProSubscriber:
    def __init__(self, url: str = 'wss://ws-feed.pro.coinbase.com', connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
                 request_timeout: int = DEFAULT_REQUEST_TIMEOUT):
        self.url = url
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

    async def connect(self):
        headers = httputil.HTTPHeaders({'Content-Type': APPLICATION_JSON})
        request = httpclient.HTTPRequest(url=self.url,
                                         connect_timeout=self.connect_timeout,
                                         request_timeout=self.request_timeout,
                                         headers=headers)

        # noinspection PyAttributeOutsideInit
        self._ws_connection = await websocket.websocket_connect(request)
        self.send("""
    {
        "type": "subscribe",
        "product_ids": [
            "BTC-USD"
        ],
        "channels": [
            "matches",
            "heartbeat"
        ] 
    }
            """)
        while True:
            msg = await self._ws_connection.read_message()
            if msg is None:
                self._on_connection_close()
                break

            self._on_message(msg)

    def send(self, data: str):
        """
        Send message to the server
        """
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is closed.')

        self._ws_connection.write_message(data)

    def close(self):
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is already closed.')

        self._ws_connection.close()

    # noinspection PyMethodMayBeStatic
    def _on_message(self, msg):
        print(msg)
        pass

    def _on_connection_close(self):
        pass


if __name__ == '__main__':
    subscriber = CoinbaseProSubscriber()
    IOLoop.instance().run_sync(subscriber.connect)
