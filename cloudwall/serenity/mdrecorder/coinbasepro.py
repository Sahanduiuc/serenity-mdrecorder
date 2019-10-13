from coinbasepro import PublicClient
from decimal import Decimal

from cloudwall.serenity.mdrecorder.api import MDSnapshotClient


class CoinbaseProSnapshotClient(MDSnapshotClient):
    def __init__(self):
        super().__init__()
        self.client = PublicClient()

    def snap_last_trade_px(self, symbol: str) -> Decimal:
        ticker_data = self.client.get_product_ticker(symbol)
        return ticker_data['price']
