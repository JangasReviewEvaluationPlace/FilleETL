import os
from abc import ABC, abstractclassmethod, abstractproperty


class BaseETL(ABC):
    def __init__(self, is_dummy: bool = False, *args, **kwargs):
        self.data_dir = os.path.join(self.file_dir, "data")
        assert os.path.isdir(self.data_dir), (
            'A directory named `data` must exist.'
        )
        self.sample_data_dir = os.path.join(self.file_dir, "sample_data")
        assert os.path.isdir(self.sample_data_dir), (
            'A directory named `sample_data_dir` must exist.'
        )

        self.output_dir = os.path.join(self.file_dir, "output")
        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

        self._load(is_dummy=is_dummy)
        self._transform()
        self._extract()

    @abstractproperty
    def file_dir(self):
        pass

    @abstractclassmethod
    def _load(cls, is_dummy: bool):
        pass

    @abstractclassmethod
    def _extract(cls):
        pass

    @abstractclassmethod
    def _transform(cls):
        pass
