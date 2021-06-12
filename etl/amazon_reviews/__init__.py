import logging
import os
import pandas as pd
from utils import BaseETL, GenericETLLoggingDecorators


logger = logging.getLogger(__name__)


class ETL(BaseETL):
    file_dir = os.path.dirname(os.path.realpath(__file__))

    @GenericETLLoggingDecorators.extract(data_variable_name="dataframes")
    def _extract(self, is_dummy: bool):
        df_dir = self.file_dir if not is_dummy else self.sample_data_dir
        csv_files = [
            csv_file for csv_file in os.listdir(df_dir)
            if csv_file.endswith(".csv")
        ]
        already_processed_files = [
            csv_file for csv_file in os.listdir(self.output_dir)
            if csv_file.endswith(".csv")
        ]

        self.dataframes = []
        column_names = ('rating', 'header', 'body')
        for csv_file in csv_files:
            if csv_file in already_processed_files:
                continue
            self.dataframes.append(
                pd.read_csv(
                    os.path.join(df_dir, csv_file),
                    names=column_names,
                    header=None
                )
            )

    def _transform(self):
        pass

    def _load(self):
        pass
