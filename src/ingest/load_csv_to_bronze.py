"""
Bronze ingestion: CSV to MySQL
- Reads a CSV file
- Computes row_hash
- Adds ingestion metadata - ingested_at (DB default), source_file, row_hash
- Inserts into a bronze_* table

Works with the bronze layer because all source fields are stored as VARCHARs.

Requirements: pip install mysql-connector-python

Usage examples:
- python src/ingest/load_csv_to_bronze.py --table bronze_appointments --file data/incoming/appointments_2024_01_15.csv
- python src/ingest/load_csv_to_bronze.py --table bronze_encounters --file data/incoming/encounters_2024_01_15.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import mysql.connector
from mysql.connector import MySQLConnection


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def compute_row_hash(row: Dict[str, str], ordered_cols: List[str]) -> str:

    # Stable hash - Joins values in column order with a separator.
    # Treats missing as an empty string, strips whitespace.
    parts = []
    for c in ordered_cols:
        v = row.get(c,"")
        if v is None:
            v = ""
        parts.append(str(v).strip())
    payload = "|".join(parts)
    return sha256_hex(payload)

def connect_mysql() -> MySQLConnection:

    # Defaults match the Docker MySQL 8.0 setup:
    # host=127.0.0.1, port=3307, user=etl_user, password=etlpass123!, database=clinic_bronze
    # You can override via env vars:
    # MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3307"))
    user = os.getenv("MYSQL_USER", "etl_user")
    password = os.getenv("MYSQL_PASSWORD", "etlpass123!")
    database = os.getenv("MYSQL_DATABASE", "clinic_bronze")

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )

def get_table_columns(conn: MySQLConnection, table: str) -> List[str]:

    # Fetch column names for the target column
    # Will insert into all columns except for:
    # _ bronze_id (auto-increment primary key)
    # - ingested_at (default CURRENT_TIMESTAMP)
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION;
    """

    cur = conn.cursor()
    cur.execute(sql, (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols

def build_insert_sql(table: str, insert_cols: List[str]) -> str:
    placeholders = ", ".join(["%s"] * len(insert_cols))
    col_list = ", ".join(insert_cols)
    return f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

def file_already_processed(conn, file_name: str) -> bool:
    sql = """
        SELECT 1
        FROM clinic_audit.processed_files
        WHERE file_name = %s
        LIMIT 1;
        """
    cur = conn.cursor()
    cur.execute(sql, (file_name,))
    result = cur.fetchone()
    cur.close()
    return result is not None

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _unique_dest_path(dest: Path) -> Path:
    # If dest exists, append a timestamp to avoid overwrite.
    if not dest.exists():
        return dest
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dest.with_name(f"{dest.stem}__{ts}{dest.suffix}")

def move_to_processed(csv_path: Path, subfolder: str = "") -> Path:
    # Move a CSV from data/incoming to data/processed, or a subfolder like duplicates
    # If the parent folder is 'incoming', treat its parent as the data root
    base_dir = csv_path.parent.parent if csv_path.parent.name == "incoming" else csv_path.parent

    dest_dir = base_dir / "processed"
    if subfolder:
        dest_dir = dest_dir / subfolder

    _ensure_dir(dest_dir)
    dest_path = _unique_dest_path(dest_dir / csv_path.name)

    shutil.move(str(csv_path), str(dest_path))
    return dest_path

def load_csv_to_bronze(table: str, csv_path: Path, batch_size: int = 1000) -> Tuple[int, int]:

    # Returns rows_inserted and rows_read
    file_name = csv_path.name

    # If the file was already processed, it may have been moved out of the incoming folder
    # In that case, a missing file should not crash the run
    if not csv_path.exists():
        conn = connect_mysql()
        try:
            if file_already_processed(conn, file_name):
                print(
                    f"File {file_name} already processed and no longer present at {csv_path}. "
                    f"It was likely moved to data/processed or data/processed/duplicates/. Skipping ingestion."
                )
                return 0, 0
        finally:
            conn.close()

        raise FileNotFoundError(
            f"CSV not found: {csv_path}. If you expected a rerun, update --file to the new location under data/processed/."
        )

    conn = connect_mysql()
    if file_already_processed(conn, file_name):
        if csv_path.exists() and csv_path.parent.name == "incoming":
            try:
                new_path = move_to_processed(csv_path, subfolder="duplicates")
                print(f"File {file_name} already processed. Moved to {new_path}")
            except Exception as e:
                print(f"File {file_name} already processed. Could not move to duplicates: {e}")
        else:
            print(f"File {file_name} already processed. Skipping ingestion.")
        conn.close()
        return 0, 0
    try:
        table_cols = get_table_columns(conn, table)
        if not table_cols:
            raise ValueError(f"Table not found in current schema: {table}")

        # Columns that can/should be inserted
        # - Exclude auto pk bronze_id
        # - Exclude ingested_at because DB default handles it
        insertable = [c for c in table_cols if c not in ("bronze_id", "ingested_at")]

        # The CSV headers should match the source columns in the bronze table.
        # Bronze tables also include : source_file, row_hash (they are not in the CSV files)
        required_metadata = {"source_file", "row_hash"}
        if not required_metadata.issubset(set(insertable)):
            raise ValueError(f"{table} is missing required columns: {required_metadata}. "
                             f"Found columns: {table_cols}")

        # Determines which columns are source columns in the CSV
        source_cols = [c for c in insertable if c not in("source_file", "row_hash")]

        insert_sql = build_insert_sql(table, insertable)

        rows_read = 0
        rows_inserted = 0

        cur = conn.cursor()

        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("CSV has no header row")

            # Validates that the CSV headers are a subset of expected source columns
            csv_cols = [h.strip() for h in reader.fieldnames if h and h.strip()]
            unexpected = sorted(set(csv_cols) - set(source_cols))
            missing = sorted(set(source_cols) - set(csv_cols))

            # The ETL should catch schema mismatches early
            if unexpected:
                raise ValueError(f"CSV has unexpected columns for {table}: {unexpected}\n"
                                 f"Expected (source cols): {source_cols}")
            if missing:
                raise ValueError(f"CSV is missing columns for {table}: {missing}\n"
                                 f"CSV columns: {csv_cols}")

            batch: List[Tuple] = []

            for row in reader:
                rows_read += 1

                # If the CSV file had a trailing comma, DictReader creates and empty column name "".
                # Drop it so the schema validation and hashing are stable.
                row.pop("", None)

                # Normalize row dict values to strings
                normalized = {k: (v.strip() if isinstance(v, str) else ("" if v is None else str(v)))
                              for k, v in row.items()}

                row_hash = compute_row_hash(normalized, ordered_cols=source_cols)

                values = []
                for col in insertable:
                    if col == "source_file":
                        values.append(str(csv_path.name))
                    elif col == "row_hash":
                        values.append(row_hash)
                    else:
                        values.append(normalized.get(col, ""))

                batch.append(tuple(values))

                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    rows_inserted += len(batch)
                    batch.clear()

            # Flush remainder
            if batch:
                cur.executemany(insert_sql, batch)
                rows_inserted += len(batch)

        conn.commit()
        # Record the processed file. Idempotent if called twice
        cur2 = conn.cursor()
        cur2.execute(
            "INSERT INTO clinic_audit.processed_files (file_name) VALUES (%s) ON DUPLICATE KEY UPDATE processed_at = processed_at",
            (file_name,),
        )
        conn.commit()
        cur2.close()

        # Move the file out of incoming after a successful load
        if csv_path.exists() and csv_path.parent.name == "incoming":
            try:
                new_path = move_to_processed(csv_path)
                print(f"Moved processed file to {new_path}")
            except Exception as e:
                print(f"Warning: Loaded data but could not move file to processed folder: {e}")

        return rows_inserted, rows_read

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def main() -> None:
    parser = argparse.ArgumentParser(description="Load a CSV file into a MySQL Bronze table.")
    parser.add_argument("--table", required=True, help="Target table name")
    parser.add_argument("--file", required=True, help="CSV file path")
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=1000, help="Insert batch size (default: 1000)")
    args = parser.parse_args()

    table = args.table.strip()
    csv_path = Path(args.file).expanduser().resolve()

    inserted, read = load_csv_to_bronze(table=table, csv_path=csv_path, batch_size=args.batch_size)
    print(f"Loaded {inserted}/{read} rows into clinic_bronze.{table} from {csv_path.name}")

if __name__ == "__main__":
    main()
