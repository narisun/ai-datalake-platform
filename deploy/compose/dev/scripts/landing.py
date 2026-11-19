"""Work with the landing container in Azurite. Run with the platform's Python:

    /opt/datalake/venv/bin/python /opt/datalake/dev/landing.py init
    /opt/datalake/venv/bin/python /opt/datalake/dev/landing.py seed
    /opt/datalake/venv/bin/python /opt/datalake/dev/landing.py put <local file> <path in landing>
    /opt/datalake/venv/bin/python /opt/datalake/dev/landing.py ls [prefix]
    /opt/datalake/venv/bin/python /opt/datalake/dev/landing.py rm <path in landing>

`seed` uploads a small set of sample files (two CSV order files, an Excel customer list
and a Parquet product list), leaving alone any that are already there.
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient

CONTAINER = "landing"
# Azurite's account name and key are fixed, public values built into the emulator.
CONNECTION = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={os.environ['DL_SECRET_AZURITE_KEY']};"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)


def container() -> ContainerClient:
    return BlobServiceClient.from_connection_string(CONNECTION).get_container_client(CONTAINER)


def init() -> None:
    try:
        container().create_container()
        print(f"created container {CONTAINER}")
    except ResourceExistsError:
        print(f"container {CONTAINER} already exists")


def put(data: bytes, path: str) -> None:
    container().upload_blob(path, data, overwrite=True)
    print(f"uploaded {path} ({len(data)} bytes)")


def sample_files() -> dict[str, bytes]:
    orders_1 = (
        "Order ID,Customer ID,Ordered At,Amount\n"
        "10,1,2026-11-01 10:00:00,120.50\n"
        "11,2,2026-11-01 11:15:00,80.00\n"
        "12,1,2026-11-01 16:40:00,42.25\n"
    )
    orders_2 = (
        "Order ID,Customer ID,Ordered At,Amount\n"
        "13,3,2026-11-02 09:05:00,310.00\n"
        "14,2,2026-11-02 13:30:00,15.99\n"
    )

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Customers"
    for row in [
        ["Customer ID", "Full Name", "Email", "Country"],
        [1, "Ada Lovelace", "ADA@example.com", "GB"],
        [2, "Alan Turing", "alan@example.com", "GB"],
        [3, "Grace O'Brien-Hopper", "grace@example.com", "US"],
    ]:
        sheet.append(row)
    customers = io.BytesIO()
    workbook.save(customers)

    products = io.BytesIO()
    pq.write_table(
        pa.table(
            {
                "Product ID": [100, 101, 102],
                "Name": ["Kettle", "Toaster", "Blender"],
                "Price": [24.99, 31.50, 59.00],
                "Added": [datetime(2026, 1, 5), datetime(2026, 3, 9), datetime(2026, 6, 21)],
            }
        ),
        products,
    )
    return {
        "shop/orders/orders_2026-11-01.csv": orders_1.encode(),
        "shop/orders/orders_2026-11-02.csv": orders_2.encode(),
        "shop/customers/customers_2026-11-01.xlsx": customers.getvalue(),
        "shop/products/products_2026-11-01.parquet": products.getvalue(),
    }


def main(argv: list[str]) -> int:
    command = argv[0] if argv else ""
    if command == "init":
        init()
    elif command == "seed":
        init()
        # Never overwrite: a re-saved file has different bytes, and ingestion would
        # rightly treat it as changed and load it again.
        existing = {blob.name for blob in container().list_blobs()}
        for path, data in sample_files().items():
            if path in existing:
                print(f"kept {path} (already there)")
            else:
                put(data, path)
    elif command == "put" and len(argv) == 3:
        put(Path(argv[1]).read_bytes(), argv[2])
    elif command == "ls":
        for blob in container().list_blobs(name_starts_with=argv[1] if len(argv) > 1 else None):
            print(f"{blob.size:>9}  {blob.name}")
    elif command == "rm" and len(argv) == 2:
        container().delete_blob(argv[1])
        print(f"deleted {argv[1]}")
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
