import os
from abc import ABC, abstractmethod, abstractproperty


class BaseETL(ABC):
    def __init__(self, *args, **kwargs):
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

    @abstractproperty
    def file_dir(self):
        pass

    @abstractmethod
    def _extract(self, is_dummy: bool):
        pass

    @abstractmethod
    def _transform(self, *args, **kwargs):
        pass

    @abstractmethod
    def _load(self, *args, **kwargs):
        pass

    def run_etl(self, is_dummy: bool):
        self._extract(is_dummy=is_dummy)
        self._transform()
        self._load()

    def run_etl_generator(self, is_dummy: bool):
        df_generator = self._extract(is_dummy=is_dummy)
        for df in df_generator:
            self._transform(df)
            self._load(df)

    @abstractmethod
    def run(is_dummy: bool = False):
        pass


class ETLLogMessages:
    @staticmethod
    def start_etl() -> str:
        return "Start ETL process"

    @staticmethod
    def finish_etl() -> str:
        return "ETL process Done"

    @staticmethod
    def start_extracting() -> str:
        return "Start to extract Data into ETL"

    @staticmethod
    def finish_extracting_single_dataset(rowcount: int) -> str:
        return f"Dataframe Extracted with rowcount: {rowcount}."

    @staticmethod
    def start_transforming() -> str:
        return "Start Transforming the dataset."

    @staticmethod
    def finish_transforming(rowcount: int) -> str:
        return f"Transforming is finalized. {rowcount} lines have survived."

    @staticmethod
    def start_language_evaluation() -> str:
        return "Start language evaluation."

    @staticmethod
    def finish_language_evaluation(english_count, rowcount) -> str:
        return f"Finish language evaluation. {english_count} out of {rowcount} rows are english."

    @staticmethod
    def start_loading() -> str:
        return "Start writing data to output file."

    @staticmethod
    def finish_loading(rowcount: int, file_name: str) -> str:
        return f"{rowcount} lines written into file: {file_name}."
