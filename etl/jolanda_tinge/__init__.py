import logging
import os
import pandas as pd
from functools import partial
from typing import List, Generator
from utils import BaseETL

logger = logging.getLogger(__name__)


class ETL(BaseETL):
    # Object oriented filesystem path.
    # Implements path objects as first-class entities,
    # allowing common operations on files to be invoked on those path objects directly.
    # Pass to any function taking the filepath as a string.
    file_dir = os.path.dirname(os.path.realpath(__file__))

    def __get_csv_files(self, is_dummy: bool) -> List[str]:
        df_dir = self.data_dir if not is_dummy else self.sample_data_dir
        return [
            os.path.join(df_dir, csv_file)
            for csv_file in os.listdir(df_dir)
            if csv_file.endswith(".csv")
        ]

    def _extract(self, is_dummy: bool) -> Generator[pd.DataFrame, None, None]:
        # convert files with pd
        csv_files = self.__get_csv_files(is_dummy=is_dummy)
        # Read the source data from sample data directory
        column_names = ('rating', 'header', 'body')
        for csv_file in csv_files:
            self.__csv_file_name = os.path.basename(csv_file)
            df = df_reader()
            df_reader = partial(pd.read_csv, csv_file, names=column_names, header=None)
            yield df
        print(csv_file)

    def _transform(self):
        # Transform self.df to required output format
        pass

    def _load(self):
        # Write self.df to csv output file in output directory
        pass

    def run(self):
        self.run_etl()
