import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
