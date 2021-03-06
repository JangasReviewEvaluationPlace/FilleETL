import logging
import os
import re
import pandas as pd
from functools import partial
from typing import List, Generator
from utils import BaseETL
from utils.etl import ETLLogMessages
from utils.language_detection import set_not_english_columns_to_null
from utils.sftp import send_file_to_sftp


logger = logging.getLogger(__name__)


class ETL(BaseETL):
    file_dir = os.path.dirname(os.path.realpath(__file__))

    def __get_csv_files(self) -> List[str]:
        df_dir = self.data_dir if not self.is_dummy else self.sample_data_dir
        return [
            os.path.join(df_dir, csv_file)
            for csv_file in os.listdir(df_dir)
            if csv_file.endswith(".csv")
        ]

    def __already_processed_files(self) -> List[str]:
        # I know: Antipattern
        # there are better ways to replace chunks
        chunk_pattern = "__chunk_(.+?)__"
        file_list = []
        for f in os.listdir(self.output_dir):
            chunk_info = re.search(chunk_pattern, f)
            if chunk_info:
                cleaned_file_name = f.replace(f"__chunk_{chunk_info.group(1)}__", "")
                if cleaned_file_name not in file_list:
                    file_list.append(cleaned_file_name)
            else:
                file_list.append(f)
        return file_list

    def _extract(self) -> Generator[pd.DataFrame, None, None]:
        csv_files = self.__get_csv_files()
        already_processed_files = self.__already_processed_files()

        column_names = ('rating', 'header', 'body')
        for csv_file in csv_files:
            csv_file_name = os.path.basename(csv_file)
            if csv_file_name in already_processed_files:
                continue
            logging.info(ETLLogMessages.start_extracting())
            df_reader = partial(pd.read_csv, csv_file, names=column_names, header=None)
            if not self.chunk_size:
                df = df_reader()
                logging.info(ETLLogMessages.finish_extracting_single_dataset(df.shape[0]))
                yield df, csv_file_name
            else:
                with df_reader(chunksize=self.chunk_size) as reader:
                    for df in reader:
                        logging.info(ETLLogMessages.finish_extracting_single_dataset(df.shape[0]))
                        yield df, csv_file_name

    def __set_feedback_type(self, df: pd.DataFrame):
        df.loc[df["rating"] > 3, "feedback_type"] = 'positive'
        df.loc[df["rating"] == 3, "feedback_type"] = 'neutral'
        df.loc[df["rating"] < 3, "feedback_type"] = 'negative'

    def _transform(self, df: pd.DataFrame):
        logging.info(ETLLogMessages.start_transforming())
        initial_shape = df.shape

        # Label rating
        self.__set_feedback_type(df)

        # Language Detection
        logging.info(ETLLogMessages.start_language_evaluation())
        set_not_english_columns_to_null(df)
        df.dropna(inplace=True)
        logging.info(ETLLogMessages.finish_language_evaluation(
            english_count=df.shape[0], rowcount=initial_shape[0])
        )

        # Cleanup and conventions
        df["source"] = "Amazon Reviews"
        df["is_streaming"] = False
        df["language_code"] = 'en'
        df.drop('rating', axis=1, inplace=True)

        logging.info(ETLLogMessages.finish_transforming(rowcount=df.shape[0]))

    def _load(self, df: pd.DataFrame, chunk_index: int, csv_file_name: str):
        logging.info(ETLLogMessages.start_loading())
        if self.chunk_size:
            filename = csv_file_name.split('.')[0]
            output_csv_name = f"{filename}__chunk_{chunk_index}__.csv"
        else:
            output_csv_name = csv_file_name
        output_file = os.path.join(self.output_dir, output_csv_name)
        df.to_csv(output_file, index=False, sep="\t")
        logging.info(ETLLogMessages.finish_loading(
            rowcount=df.shape[0], file_name=output_csv_name)
        )
        if self.sftp_active:
            send_file_to_sftp(path=output_file, filename=output_csv_name)

    def run(self):
        logging.info(ETLLogMessages.start_etl())
        self.run_etl_generator()
        logging.info(ETLLogMessages.finish_etl())
