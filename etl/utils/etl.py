import os
import logging
import multiprocessing
from abc import ABC, abstractmethod, abstractproperty
import pandas as pd


class BaseETL(ABC):
    def __init__(self, is_dummy: bool = False, allowed_threads: int = 1, chunk_size=None,
                 sftp_active: bool = False, *args, **kwargs):
        self.is_dummy = is_dummy
        self.sftp_active = sftp_active
        if not is_dummy:
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

        self.allowed_threads = allowed_threads
        self.chunk_size = chunk_size

    @abstractproperty
    def file_dir(self):
        pass

    @abstractmethod
    def _extract(self):
        pass

    @abstractmethod
    def _transform(self, *args, **kwargs):
        pass

    @abstractmethod
    def _load(self, *args, **kwargs):
        pass

    def run_etl(self):
        self._extract()
        self._transform()
        self._load()

    def transform_load_process_for_given_df(self, df: pd.DataFrame, chunk_index: int,
                                            csv_file_name: str):
        self._transform(df)
        self._load(df, chunk_index=chunk_index, csv_file_name=csv_file_name)

    def run_etl_generator(self):
        df_generator = self._extract()
        chunk_index = 0
        if self.allowed_threads > 1:
            with multiprocessing.Pool(self.allowed_threads) as t:
                for df, csv_file_name in df_generator:
                    logging.info("Schedule df to thread for index")
                    t.apply_async(
                        self.transform_load_process_for_given_df,
                        kwds={"df": df, "chunk_index": chunk_index, "csv_file_name": csv_file_name}
                    )
                    chunk_index += 1
                t.close()
                t.join()
        else:
            for df, csv_file_name in df_generator:
                kwds = {"df": df, "chunk_index": chunk_index, "csv_file_name": csv_file_name}
                self.transform_load_process_for_given_df(**kwds)

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
