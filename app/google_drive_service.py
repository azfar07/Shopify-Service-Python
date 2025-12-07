from __future__ import annotations

import io
from typing import Callable, Any, Dict, List

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload

from app.config import (
    ROOT_VENDOR_FOLDER_ID,
    SERVICE_ACCOUNT_FILE,
    PROCESSED_FOLDER_NAME,
    ERROR_FOLDER_NAME,
)

# ---------------------------------------------------------
# GOOGLE DRIVE SERVICE INITIALIZATION
# ---------------------------------------------------------
def get_drive_service():
    """
    Create and return a Google Drive API client using a service account.

    WHY?
    - Needed for listing folders/files
    - Needed for downloading files
    - Needed for moving files (_PROCESSED / _ERRORS)
    """
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


# ---------------------------------------------------------
# CREATE OR GET SUBFOLDER
# ---------------------------------------------------------
def get_or_create_folder(parent_id: str, name: str) -> str:
    """
    Find or create a folder named `name` inside parent folder.

    PURPOSE:
    - Ensure every vendor folder contains:
        /vendorEmail/
           _PROCESSED/
           _ERRORS/
    """
    service = get_drive_service()

    # Search for existing folder
    query = (
        f"'{parent_id}' in parents and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false and "
        f"name = '{name}'"
    )
    resp = service.files().list(q=query, fields="files(id,name)").execute()
    items = resp.get("files", [])

    # If exists → return folder ID
    if items:
        return items[0]["id"]

    # Otherwise create new folder
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


# ---------------------------------------------------------
# LIST ALL VENDOR FOLDERS UNDER ROOT
# ---------------------------------------------------------
def list_vendor_folders(root_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all vendor folders directly under the root master folder.

    These folders are created by your Google Apps Script:
        ROOT/
          vendor1@email.com/
          vendor2@email.com/
          ...
    """
    service = get_drive_service()

    resp = service.files().list(
        q=(
            f"'{root_id}' in parents and "
            "mimeType = 'application/vnd.google-apps.folder' and "
            "trashed = false"
        ),
        fields="files(id,name)",
    ).execute()

    return resp.get("files", [])


# ---------------------------------------------------------
# LIST ONLY FILES (NO FOLDERS) INSIDE A VENDOR FOLDER
# ---------------------------------------------------------
def list_files_in_folder(folder_id: str) -> List[Dict[str, Any]]:
    """
    List all non-folder items directly inside the vendor folder.

    IMPORTANT:
    - Does NOT return files inside:
        /vendor/_PROCESSED
        /vendor/_ERRORS
    - Only returns the "new" incoming files.
    """
    service = get_drive_service()

    resp = service.files().list(
        q=(
            f"'{folder_id}' in parents and "
            "trashed = false and "
            "mimeType != 'application/vnd.google-apps.folder'"
        ),
        fields="files(id,name)",
    ).execute()

    return resp.get("files", [])


# ---------------------------------------------------------
# CHECK IF FILE IS CSV/XLS/XLSX
# ---------------------------------------------------------
def is_vendor_file(name: str) -> bool:
    """
    Only process vendor files that are CSV or Excel.
    """
    lower = name.lower()
    return (
        lower.endswith(".csv")
        or lower.endswith(".xls")
        or lower.endswith(".xlsx")
    )


# ---------------------------------------------------------
# DOWNLOAD FILE BYTES (IN-MEMORY)
# ---------------------------------------------------------
def get_file_bytes(file_id: str) -> bytes:
    """
    Download a Google Drive file into memory (BytesIO).

    WHY?
    - No need to save file locally.
    - Pandas can read CSV/Excel from bytes.
    """
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh.read()


# ---------------------------------------------------------
# MOVE FILE TO _PROCESSED OR _ERRORS
# ---------------------------------------------------------
def move_file(file_id: str, new_parent_id: str) -> None:
    """
    Move a file to a different folder:
    - Remove file's current parent
    - Add new parent folder
    """
    service = get_drive_service()

    # Get current parents
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents", []))

    # Move file
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=prev_parents,
        fields="id, parents",
    ).execute()


# ---------------------------------------------------------
# MAIN PROCESSOR — HANDLES ALL VENDORS + ALL FILES
# ---------------------------------------------------------
def sync_all_vendor_files(
    process_single_file_bytes: Callable[[str, str, bytes], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    MAIN DRIVER FUNCTION — orchestrates everything.

    WHAT THIS FUNCTION DOES:
    --------------------------------------
    1. Read the ROOT vendor folder
    2. For each vendor:
         - Ensure _PROCESSED exists
         - Ensure _ERRORS exists
         - List new CSV/XLS/XLSX files
    3. For each file:
         - Download file as bytes
         - Pass bytes to your processor:
              process_single_file_bytes(vendor, file_name, bytes)
         - If success → MOVED to _PROCESSED
         - If error   → MOVED to _ERRORS
    4. Return summary of all activity
    --------------------------------------
    """
    summary: List[Dict[str, Any]] = []

    # Get all vendors under root folder
    vendor_folders = list_vendor_folders(ROOT_VENDOR_FOLDER_ID)

    for vendor_folder in vendor_folders:
        vendor_name = vendor_folder["name"]    # folder name = vendor email
        vendor_id = vendor_folder["id"]

        # Ensure vendor has _PROCESSED and _ERRORS subfolders
        vendor_processed_id = get_or_create_folder(vendor_id, PROCESSED_FOLDER_NAME)
        vendor_error_id = get_or_create_folder(vendor_id, ERROR_FOLDER_NAME)

        # List ONLY new, unprocessed files
        files = list_files_in_folder(vendor_id)

        for f in files:
            file_id = f["id"]
            file_name = f["name"]

            # Skip non-vendor files (images, PDF, etc.)
            if not is_vendor_file(file_name):
                continue

            # Read CSV/XLS/XLSX into memory
            file_bytes = get_file_bytes(file_id)

            try:
                # Pass bytes → your processing logic (Shopify creation)
                result = process_single_file_bytes(vendor_name, file_name, file_bytes)

                # SUCCESS → move to _PROCESSED
                move_file(file_id, vendor_processed_id)

                summary.append(
                    {
                        "vendor": vendor_name,
                        "file": file_name,
                        "status": "processed",
                        "details": result,
                    }
                )

            except Exception as ex:
                # FAILURE → move to _ERRORS
                move_file(file_id, vendor_error_id)

                summary.append(
                    {
                        "vendor": vendor_name,
                        "file": file_name,
                        "status": "error",
                        "error": str(ex),
                    }
                )

    return summary
