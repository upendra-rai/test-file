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
import tempfile
import paramiko
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


def run_remote_cmd_paramiko(ssh_key: Path, user_host: str, remote_cmd: str):
    """Run a command on the remote server via SSH using paramiko."""
    log.info("[paramiko] Running remote command: %s", remote_cmd)
    user, host = user_host.split('@')
    key = paramiko.RSAKey.from_private_key_file(str(ssh_key))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname=host, username=user, pkey=key)
        stdin, stdout, stderr = ssh.exec_command(remote_cmd)
        out = stdout.read().decode()
        err = stderr.read().decode()
        if out:
            log.info("[paramiko] STDOUT: %s", out.strip())
        if err:
            log.error("[paramiko] STDERR: %s", err.strip())
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            log.error("[paramiko] Remote command failed with exit status %d", exit_status)
            raise Exception(f"Remote command failed: {err}")
        else:
            log.info("[paramiko] Remote command completed successfully.")
    finally:
        ssh.close()


def scp_from_remote_paramiko(ssh_key: Path, user_host: str, remote_path: str, local_path: Path):
    """Copy file from remote server to local via paramiko SFTP."""
    log.info("[paramiko] Copying from remote: %s:%s to local: %s", user_host, remote_path, local_path)
    user, host = user_host.split('@')
    key = paramiko.RSAKey.from_private_key_file(str(ssh_key))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname=host, username=user, pkey=key)
        sftp = ssh.open_sftp()
        sftp.get(remote_path, str(local_path))
        log.info("[paramiko] SFTP get completed.")
        sftp.close()
    except Exception as e:
        log.error("[paramiko] SFTP get failed: %s", e)
        raise
    finally:
        ssh.close()


# ────────────────────── Main Workflow ──────────────────────
def main():
    log.info("Ensuring backup directories exist: %s, %s", BACKUP_DIR, FINAL_DIR)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    stamp = timetag()

    SSH_KEY = Path("private_Glidex-DB_key.pem")
    USER_HOST = "ubuntu@10.0.7.212"
    REMOTE_BACKUP_DIR = f"/home/ubuntu/backup_{stamp}"
    REMOTE_ZIP = f"/home/ubuntu/backup_{stamp}.zip"
    REMOTE_SCRIPT_PATH = f"/home/ubuntu/backup_script_{stamp}.sh"

    # 1. Prepare remote backup script (use localhost for DB_HOST)
    remote_script = f'''
set -e
mkdir -p {REMOTE_BACKUP_DIR}
mysqldump -h localhost -u {DB_USER} -p'{DB_PASS}' {DB_NAME} > {REMOTE_BACKUP_DIR}/{DB_NAME}_full_{stamp}.sql
'''
    for table in TABLES:
        days = RETENTION.get(table, DEFAULT_RETENTION_DAYS)
        where = f"created < DATE_SUB(NOW(), INTERVAL {days} DAY)"
        remote_script += f"mysqldump -h localhost -u {DB_USER} -p'{DB_PASS}' {DB_NAME} {table} --where=\"{where}\" > {REMOTE_BACKUP_DIR}/{table}_{stamp}.sql\n"
    remote_script += f"cd /home/ubuntu && zip -r backup_{stamp}.zip backup_{stamp}\n"

    # 2. Write remote script to temp file
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sh") as tf:
        tf.write(remote_script)
        tf.flush()
        local_script_path = tf.name

    # 3. Copy script to remote home dir using paramiko SFTP
    log.info("[paramiko] Copying backup script to remote: %s", REMOTE_SCRIPT_PATH)
    user, host = USER_HOST.split('@')
    key = paramiko.RSAKey.from_private_key_file(str(SSH_KEY))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname=host, username=user, pkey=key)
        sftp = ssh.open_sftp()
        sftp.put(local_script_path, REMOTE_SCRIPT_PATH)
        log.info("[paramiko] Script copied to remote.")
        sftp.close()
    except Exception as e:
        log.error("[paramiko] SFTP put failed: %s", e)
        raise
    finally:
        ssh.close()

    log.info("Script copied via paramiko. Running on remote...")

    backup_success = False
    try:
        # 4. Run script on remote
        run_remote_cmd_paramiko(SSH_KEY, USER_HOST, f"bash {REMOTE_SCRIPT_PATH}")
        backup_success = True
    except Exception as e:
        log.error("Remote backup script failed, aborting: %s", e)

    if not backup_success:
        log.error("Backup failed, skipping download and cleanup. Data will NOT be deleted on remote.")
        return

    # 5. Pull the zip file here
    try:
        scp_from_remote_paramiko(SSH_KEY, USER_HOST, REMOTE_ZIP, FINAL_DIR / f"backup_{stamp}.zip")
        log.info("Pulled backup zip from remote.")
    except Exception as e:
        log.error("Failed to pull backup zip from remote: %s", e)
        log.error("Backup zip not pulled, skipping cleanup. Data will NOT be deleted on remote.")
        return

    # 6. After successful backup and download, purge old rows locally in batches
    try:
        log.info("Connecting to MySQL for purge on %s as %s", DB_HOST, DB_USER)
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            autocommit=False
        )
        for table in TABLES:
            retention_days = RETENTION.get(table, DEFAULT_RETENTION_DAYS)
            try:
                log.info("Purging old rows from '%s' older than %d days...", table, retention_days)
                purge_old_rows(table, retention_days, conn)
            except Exception as e:
                log.error("Failed to purge table '%s': %s", table, e)
        conn.close()
        log.info("All eligible old rows purged after successful backup.")
    except Exception as e:
        log.error("Failed to connect or purge rows after backup: %s", e)
        log.error("Manual cleanup may be required.")

    # 7. Remove remote files after successful pull
    try:
        log.info("Cleaning up remote backup files...")
        cleanup_cmd = f"rm -rf {REMOTE_BACKUP_DIR} {REMOTE_ZIP} {REMOTE_SCRIPT_PATH}"
        run_remote_cmd_paramiko(SSH_KEY, USER_HOST, cleanup_cmd)
        log.info("Remote backup files cleaned up.")
    except Exception as e:
        log.error("Failed to clean up remote files: %s", e)
        log.error("Manual cleanup may be required on remote host.")


if __name__ == "__main__":
    main()
