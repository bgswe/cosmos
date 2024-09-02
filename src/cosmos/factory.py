from abc import ABC


class Factory(ABC):
    def get(self):
        raise NotImplementedError
