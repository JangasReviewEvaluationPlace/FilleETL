import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

LANGUAGE_PROPABILITY_TRESHOLD = float(os.getenv("LANGUAGE_PROPABILITY_TRESHOLD", 0.5))
LANGUAGE_DETECTION_REQUIRED = os.getenv("LANGUAGE_DETECTION_REQUIRED", "true").lower() == "true"
STEMMING_REQUIRED = os.getenv("STEMMING_REQUIRED", "true").lower() == "true"

SFTP_CONFIGS = {
    "host": os.getenv("SFTP_HOSTNAME", "localhost"),
    "username": os.getenv("SFTP_USERNAME", "testuser"),
    "password": os.getenv("SFTP_PASSWORD", "password"),
    "port": int(os.getenv("SFTP_PORT", 2222))
}
