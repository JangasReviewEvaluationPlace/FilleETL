from abc import ABC, abstractclassmethod


class BaseETL(ABC):
    def __init__(self, *args, **kwargs):
        self._load()
        self._transform()
        self._extract()

    @abstractclassmethod
    def _load(cls):
        pass

    @abstractclassmethod
    def _extract(cls):
        pass

    @abstractclassmethod
    def _transform(cls):
        pass
