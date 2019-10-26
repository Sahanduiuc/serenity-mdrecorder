from abc import ABC


class MDSnapshotClient(ABC):
    def __init__(self):
        pass

    def snap_last_trade(self, symbol: str) -> dict:
        pass
