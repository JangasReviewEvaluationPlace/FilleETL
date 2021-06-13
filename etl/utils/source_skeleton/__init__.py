import logging
import os
import pandas as pd
from utils import BaseETL


logger = logging.getLogger(__name__)


class ETL(BaseETL):
    file_dir = os.path.dirname(os.path.realpath(__file__))

    def _extract(self):
        # Read the source data from sample data directory
        # self.df = TODO
        pass

    def _transform(self):
        # Transform self.df to required output format
        pass

    def _load(self):
        # Write self.df to csv output file in output directory
        pass

    def run(self):
        pass
