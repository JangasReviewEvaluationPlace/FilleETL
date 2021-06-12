import sys
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent
sys.path.insert(0, os.path.join(BASE_DIR, 'etl'))
