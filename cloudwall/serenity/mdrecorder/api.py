from abc import ABC
from decimal import Decimal


class MDSnapshotClient(ABC):
    def __init__(self):
        pass

    def snap_last_trade_px(self, symbol: str) -> Decimal:
        pass
