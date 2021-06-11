import logging
import os
import pandas as pd
from utils import BaseETL


logger = logging.getLogger(__name__)


class ETL(BaseETL):
    file_dir = os.path.dirname(os.path.realpath(__file__))

    def _load(cls, is_dummy: bool):
        pass

    def _extract(cls):
        pass

    def _transform(cls):
        pass
