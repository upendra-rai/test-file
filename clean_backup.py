#!/usr/bin/env python3
"""
MySQL Backup & Cleanup Script:
- Full DB backup first
- Table-wise incremental backup (based on created column)
- Deletes older rows after successful backup (in 1000-row batches)
- Zips each SQL dump
- Moves each ZIP immediately to ~/glidex_backup
"""

import datetime as dt
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict
import zipfile

import mysql.connector

# ────────────────────── Configuration ──────────────────────
DB_HOST = "10.0.7.212"
DB_USER = "pg_user"
DB_PASS = "Dst0ao88lfxHnkHLkQd5Hf"
DB_NAME = "pgdb"

BACKUP_DIR = Path.home() / "backup"
FINAL_DIR = Path.home() / "glidex_backup"

TABLES = [
    "ismart_create_order",
    "customer_input_details",
    "transaction_details",
    "merchant_request4customer",
    "status_change_logs_model",
    "merchant_balance_sheet",
    "user_details",
    "callbacklog",
    "pginput_from_customer_details",
    "transaction_details_log",
]

RETENTION: Dict[str, int] = {
    "transaction_details_log": 1,
    "user_details": 60
}

DEFAULT_RETENTION_DAYS = 7
BATCH_SIZE = 1000
SLEEP_BETWEEN_BATCHES = 0.5  # seconds

# ────────────────────── Logging ──────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Add logging for script start
log.info("🚀 Starting MySQL Backup & Cleanup Script")


# ────────────────────── Helpers ──────────────────────
def timetag() -> str:
    return dt.datetime.now().strftime("%Y%m%d%H%M%S")


def run_cmd(argv, output_file: Path) -> None:
    """Run a shell command and redirect output to a file."""
    log.info("Running command: %s", ' '.join(argv))
    log.info("Output will be written to: %s", output_file)
    with output_file.open("w") as f:
        try:
            subprocess.run(argv, check=True, stdout=f, stderr=subprocess.PIPE)
            log.info("Command completed successfully: %s", ' '.join(argv))
        except subprocess.CalledProcessError as e:
            log.error("Command failed: %s", e.stderr.decode())
            log.error("Failed command: %s", ' '.join(argv))
            raise


def zip_and_move(source_file: Path, final_name: str) -> Path:
    log.info("Zipping and moving file: %s to %s", source_file, FINAL_DIR / final_name)
    zip_path = FINAL_DIR / final_name
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(source_file, arcname=source_file.name)
    source_file.unlink()
    log.info("📦 Moved and zipped → %s", zip_path)
    return zip_path


def dump_table(table: str, days: int, stamp: str) -> Path:
    sql_file = BACKUP_DIR / f"{table}_{stamp}.sql"
    where = f"created < DATE_SUB(NOW(), INTERVAL {days} DAY)"
    cmd = [
        "mysqldump",
        "-h", DB_HOST,
        "-u", DB_USER,
        f"-p{DB_PASS}",
        DB_NAME,
        table,
        f"--where={where}",
    ]
    log.info("Dumping table: %s, days: %d, file: %s", table, days, sql_file)
    run_cmd(cmd, sql_file)
    log.info("Table dump completed: %s", sql_file)
    return zip_and_move(sql_file, f"{table}_{stamp}.zip")


def dump_full_db(stamp: str) -> Path:
    sql_file = BACKUP_DIR / f"{DB_NAME}_full_{stamp}.sql"
    cmd = [
        "mysqldump",
        "-h", DB_HOST,
        "-u", DB_USER,
        f"-p{DB_PASS}",
        DB_NAME,
    ]
    log.info("Starting full DB dump to: %s", sql_file)
    run_cmd(cmd, sql_file)
    log.info("Full DB dump completed: %s", sql_file)
    return zip_and_move(sql_file, f"{DB_NAME}_full_{stamp}.zip")


def purge_old_rows(table: str, days: int, conn) -> None:
    """Delete old rows in batches of 1000."""
    cursor = conn.cursor()
    total_deleted = 0
    while True:
        log.info("Purging rows from %s older than %d days", table, days)
        cursor.execute(
            f"DELETE FROM {table} WHERE created < DATE_SUB(NOW(), INTERVAL {days} DAY) LIMIT {BATCH_SIZE}"
        )
        deleted = cursor.rowcount
        conn.commit()
        if deleted == 0:
            break
        total_deleted += deleted
        log.info("   • Deleted %d rows from %s (total so far: %d)", deleted, table, total_deleted)
        time.sleep(SLEEP_BETWEEN_BATCHES)
    log.info("   ✓ Finished deleting from %s (%d rows total)", table, total_deleted)


# ────────────────────── Main Workflow ──────────────────────
def main():
    log.info("Ensuring backup directories exist: %s, %s", BACKUP_DIR, FINAL_DIR)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    stamp = timetag()

    # 1. Full DB Backup
    try:
        log.info("📦 Creating full DB backup …")
        dump_full_db(stamp)
        log.info("✅ Full DB backup completed and moved.")
    except Exception as e:
        log.error("❌ Full DB backup failed: %s", e)
        log.error("Exiting script due to full DB backup failure.")
        sys.exit(1)

    # 2. Per-table dump + delete
    try:
        log.info("Connecting to MySQL at %s as %s", DB_HOST, DB_USER)
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            autocommit=False
        )
        log.info("MySQL connection established.")
    except Exception as e:
        log.error("❌ Could not connect to MySQL: %s", e)
        log.error("Exiting script due to MySQL connection failure.")
        sys.exit(1)

    for table in TABLES:
        retention_days = RETENTION.get(table, DEFAULT_RETENTION_DAYS)
        try:
            log.info("📤 Dumping table '%s' older than %d days …", table, retention_days)
            dump_table(table, retention_days, stamp)
        except Exception as e:
            log.error("❌ Dump failed for table '%s': %s", table, e)
            continue

        try:
            log.info("🧹 Purging old rows from '%s' …", table)
            purge_old_rows(table, retention_days, conn)
        except Exception as e:
            log.error("❌ Purge failed for table '%s': %s", table, e)
            continue

    conn.close()
    log.info("🏁 Backup and cleanup operations completed.")


if __name__ == "__main__":
    main()
