import logging
import os
from abc import ABC, abstractmethod, abstractproperty


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

        self._extract(is_dummy=is_dummy)
        self._transform()
        self._load()

    @abstractproperty
    def file_dir(self):
        pass

    @abstractmethod
    def _extract(self, is_dummy: bool):
        pass

    @abstractmethod
    def _transform(self):
        pass

    @abstractmethod
    def _load(self):
        pass


class GenericETLLoggingDecorators:
    @staticmethod
    def extract(data_variable_name: str = "dataframe"):
        def decorator(func):
            def wrapper(*args, **kwargs):
                logging.info("Start to extract Data into ETL")
                func(*args, **kwargs)
                number_of_loaded_df = len(getattr(args[0], data_variable_name))
                logging.info(
                    f"{number_of_loaded_df} Successfull extracted dataframes."
                )
            return wrapper
        return decorator
