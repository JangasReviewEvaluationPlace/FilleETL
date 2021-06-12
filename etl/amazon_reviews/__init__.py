import logging
import os
import pandas as pd
from typing import List
from utils import BaseETL, GenericETLLoggingDecorators


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

    @GenericETLLoggingDecorators.extract(data_variable_name="dataframes")
    def _extract(self, is_dummy: bool):
        csv_files = self.__get_csv_files(is_dummy=is_dummy)
        already_processed_files = self.__already_processed_files()

        self.dataframes = []
        column_names = ('rating', 'header', 'body')
        for csv_file in csv_files:
            csv_file_name = os.path.basename(csv_file)
            if csv_file_name in already_processed_files:
                continue
            self.dataframes.append(
                pd.read_csv(csv_file, names=column_names, header=None)
            )

    def _transform(self):
        pass

    def _load(self):
        pass
