import logging
import os
import pandas as pd
from typing import List
from utils import BaseETL
from utils.etl import ETLLogMessages
from utils.language_detection import set_not_english_columns_to_null


logger = logging.getLogger(__name__)


class ETL(BaseETL):
    file_dir = os.path.dirname(os.path.realpath(__file__))

    def __get_csv_files(self, is_dummy: bool) -> List[str]:
        df_dir = self.file_dir if not is_dummy else self.sample_data_dir
        return [
            os.path.join(df_dir, csv_file)
            for csv_file in os.listdir(df_dir)
            if csv_file.endswith(".csv")
        ]

    def __already_processed_files(self) -> List[str]:
        return [
            csv_file for csv_file in os.listdir(self.output_dir)
            if csv_file.endswith(".csv")
        ]

    def _extract(self, is_dummy: bool):
        csv_files = self.__get_csv_files(is_dummy=is_dummy)
        already_processed_files = self.__already_processed_files()

        column_names = ('rating', 'header', 'body')
        for csv_file in csv_files:
            self.__csv_file_name = os.path.basename(csv_file)
            if self.__csv_file_name in already_processed_files:
                continue
            logging.info(ETLLogMessages.start_extracting())
            df = pd.read_csv(csv_file, names=column_names, header=None)
            logging.info(ETLLogMessages.finish_extracting_single_dataset(df.shape[0]))
            yield df

    def _transform(self, df):
        logging.info(ETLLogMessages.start_transforming())
        initial_shape = df.shape

        # Label rating
        df.loc[df["rating"] > 3, "type"] = 'positive'
        df.loc[df["rating"] == 3, "type"] = 'neutral'
        df.loc[df["rating"] < 3, "type"] = 'negative'

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
        df.drop('rating', axis=1, inplace=True)

        logging.info(ETLLogMessages.finish_transforming(rowcount=df.shape[0]))

    def _load(self, df):
        logging.info(ETLLogMessages.start_loading())
        # self.__csv_file_name is set inside of _extract method. Ugly codestyle. Sorry for that.
        df.to_csv(os.path.join(self.output_dir, self.__csv_file_name))
        logging.info(ETLLogMessages.finish_loading(
            rowcount=df.shape[0], file_name=self.__csv_file_name)
        )

    def run(self, is_dummy: bool = False):
        logging.info(ETLLogMessages.start_etl())
        self.run_etl_generator(is_dummy=is_dummy)
        logging.info(ETLLogMessages.finish_etl())
