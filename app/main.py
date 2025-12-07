import logging
import os

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse

from app.google_drive_service import sync_all_vendor_files
from app.processor_products import process_product_file_bytes

# ---------------------------------------------------------
# Logging setup (global)
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("drive_shopify_sync")

app = FastAPI(
    title="Drive → Shopify Product Sync",
    description="Sync vendor spreadsheets from Google Drive, enrich with scraping, and push to Shopify.",
    version="1.0.0",
)


@app.get("/")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------
# 1) Main sync from Google Drive
# ---------------------------------------------------------
@app.post("/sync-products")
def sync_products():
    """
    Scan all vendor folders in Google Drive, process each CSV/XLS/XLSX file,
    create/update products in Shopify, and move files to _PROCESSED or _ERRORS.
    """
    logger.info("Starting /sync-products job")
    summary = sync_all_vendor_files(process_product_file_bytes)
    logger.info("Finished /sync-products job")
    return {"summary": summary}


# ---------------------------------------------------------
# 2) Manual test endpoint (upload a single file)
# ---------------------------------------------------------
@app.post("/test-upload")
async def test_upload(file: UploadFile = File(...)):
    """
    Manually upload a single CSV/XLS/XLSX file for testing
    (no Google Drive involved). Uses 'manual-test' as vendor name.
    """
    logger.info(f"Received manual test upload: {file.filename}")
    content = await file.read()
    result = process_product_file_bytes("manual-test", file.filename, content)
    return result


# ---------------------------------------------------------
# 3) Dashboard: list enriched output files
# ---------------------------------------------------------
@app.get("/scraped-files", response_class=HTMLResponse)
def list_scraped_files():
    """
    Simple HTML dashboard listing all enriched XLSX files in /output.
    """
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(output_dir) if f.lower().endswith(".xlsx")]

    rows = "".join(
        f'<tr><td><a href="/scraped-files/{f}">{f}</a></td></tr>' for f in files
    )
    html = f"""
    <html>
      <head><title>Scraped Files</title></head>
      <body>
        <h1>Scraped / Enriched Files</h1>
        <table border="1" cellpadding="5" cellspacing="0">
          <tr><th>File</th></tr>
          {rows}
        </table>
      </body>
    </html>
    """
    return html


# ---------------------------------------------------------
# 4) Dashboard: view a specific enriched file as HTML table
# ---------------------------------------------------------
@app.get("/scraped-files/{file_name}", response_class=HTMLResponse)
def view_scraped_file(file_name: str):
    output_dir = os.path.join(os.getcwd(), "output")
    full_path = os.path.join(output_dir, file_name)

    if not os.path.isfile(full_path):
        raise HTTPException(status_code=404, detail="File not found")

    df = pd.read_excel(full_path)
    table_html = df.to_html(index=False, escape=False)

    html = f"""
    <html>
      <head>
        <title>View: {file_name}</title>
        <style>
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ccc; padding: 4px; font-size: 12px; }}
            th {{ background-color: #eee; }}
        </style>
      </head>
      <body>
        <h1>{file_name}</h1>
        {table_html}
      </body>
    </html>
    """
    return html

# from fastapi import FastAPI
# from app.google_drive_service import sync_all_vendor_files
# from app.processor_products import process_product_file_bytes

# app = FastAPI(title="Drive → Shopify Product Sync")


# @app.get("/")
# def health():
#     return {"status": "ok"}


# @app.post("/sync-products")
# def sync_products():
#     """
#     Scan all vendor folders in Google Drive, process each CSV/XLS/XLSX file,
#     create products in Shopify, and move files to _PROCESSED or _ERRORS.
#     """
#     summary = sync_all_vendor_files(process_product_file_bytes)
#     return {"summary": summary}
