from abc import ABC, abstractclassmethod


class BaseETL(ABC):
    def __init__(self, is_dummy: bool = False, *args, **kwargs):
        self._load(is_dummy=is_dummy)
        self._transform()
        self._extract()

    @abstractclassmethod
    def _load(cls, is_dummy: bool):
        pass

    @abstractclassmethod
    def _extract(cls):
        pass

    @abstractclassmethod
    def _transform(cls):
        pass
