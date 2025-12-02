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


def get_drive_service():
    """Create a Google Drive API service using a service account."""
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def get_or_create_folder(parent_id: str, name: str) -> str:
    """
    Find or create a folder named `name` directly under `parent_id`.
    Returns the folder ID.
    """
    service = get_drive_service()

    query = (
        f"'{parent_id}' in parents and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false and "
        f"name = '{name}'"
    )
    resp = service.files().list(q=query, fields="files(id,name)").execute()
    items = resp.get("files", [])
    if items:
        return items[0]["id"]

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def list_vendor_folders(root_id: str) -> List[Dict[str, Any]]:
    """
    List all vendor folders directly under the root.
    Each folder represents a vendor (email as folder name).
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


def list_files_in_folder(folder_id: str) -> List[Dict[str, Any]]:
    """
    List all non-folder items directly inside a folder.
    This will NOT include files in subfolders (e.g. _PROCESSED/_ERRORS).
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


def is_vendor_file(name: str) -> bool:
    """Only process CSV / Excel files."""
    lower = name.lower()
    return (
        lower.endswith(".csv")
        or lower.endswith(".xls")
        or lower.endswith(".xlsx")
    )


def get_file_bytes(file_id: str) -> bytes:
    """Read a Drive file into memory (BytesIO) – no local disk."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh.read()


def move_file(file_id: str, new_parent_id: str) -> None:
    """
    Move a file to a new folder:
    - remove it from its current parent
    - add new_parent_id as parent
    """
    service = get_drive_service()
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents", []))

    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=prev_parents,
        fields="id, parents",
    ).execute()


def sync_all_vendor_files(
    process_single_file_bytes: Callable[[str, str, bytes], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    MAIN DRIVER (no DB):

    Expected Drive structure:

        ROOT_VENDOR_FOLDER_ID/
          vendor1@abc.com/
              _PROCESSED/
              _ERRORS/
              20250101_101010_products.csv
          vendor2@xyz.com/
              _PROCESSED/
              _ERRORS/
              ...

    For each vendor folder:
      - ensure _PROCESSED and _ERRORS exist
      - for each CSV/XLS/XLSX directly in vendor folder:
          - read bytes in memory
          - call process_single_file_bytes(vendor_name, file_name, file_bytes)
          - on success → move to vendor/_PROCESSED
          - on error   → move to vendor/_ERRORS

    Returns a summary list.
    """
    summary: List[Dict[str, Any]] = []

    vendor_folders = list_vendor_folders(ROOT_VENDOR_FOLDER_ID)

    for vendor_folder in vendor_folders:
        vendor_name = vendor_folder["name"]
        vendor_id = vendor_folder["id"]

        # ensure per-vendor _PROCESSED and _ERRORS inside the vendor folder
        vendor_processed_id = get_or_create_folder(vendor_id, PROCESSED_FOLDER_NAME)
        vendor_error_id = get_or_create_folder(vendor_id, ERROR_FOLDER_NAME)

        # "pending" files = top-level files inside vendor folder
        files = list_files_in_folder(vendor_id)

        for f in files:
            file_id = f["id"]
            file_name = f["name"]

            if not is_vendor_file(file_name):
                continue

            file_bytes = get_file_bytes(file_id)

            try:
                result = process_single_file_bytes(vendor_name, file_name, file_bytes)
                move_file(file_id, vendor_processed_id)

                summary.append(
                    {
                        "vendor": vendor_name,
                        "file": file_name,
                        "status": "processed",
                        "details": result,
                    }
                )
            except Exception as ex:  # noqa: BLE001 - we want to catch everything and send to _ERRORS
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
