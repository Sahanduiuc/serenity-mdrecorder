from coinbasepro import PublicClient
from decimal import Decimal

from cloudwall.serenity.mdrecorder.api import MDSnapshotClient


class CoinbaseProSnapshotClient(MDSnapshotClient):
    def __init__(self):
        super().__init__()
        self.client = PublicClient()

    def snap_last_trade(self, symbol: str) -> dict:
        return self.client.get_product_ticker(symbol)
