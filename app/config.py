import os
from dotenv import load_dotenv

load_dotenv()

# Root folder in Drive where all vendor folders live
ROOT_VENDOR_FOLDER_ID = os.getenv(
    "ROOT_VENDOR_FOLDER_ID",
    "1dxfaohZRRb9HZ8z0MBKTjshfacMI_tbv",  # default, overridden by .env
)

# Service account JSON file (placed in project root)
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_CREDS_FILE", "creds.json")

PROCESSED_FOLDER_NAME = "_PROCESSED"
ERROR_FOLDER_NAME = "_ERRORS"
  