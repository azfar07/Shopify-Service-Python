from fastapi import FastAPI
from app.google_drive_service import sync_all_vendor_files
from app.processor_products import process_product_file_bytes

app = FastAPI(title="Drive → Shopify Product Sync")


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/sync-products")
def sync_products():
    """
    Scan all vendor folders in Google Drive, process each CSV/XLS/XLSX file,
    create products in Shopify, and move files to _PROCESSED or _ERRORS.
    """
    summary = sync_all_vendor_files(process_product_file_bytes)
    return {"summary": summary}
