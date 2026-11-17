#!/usr/bin/env python3
"""Chạy regression SQL trên Docker tạm; không kết nối Azure hay database của người dùng."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from pathlib import Path

# SQL Server 2022 CU28 Developer, pin digest để local và CI dùng cùng engine.
IMAGE = "mcr.microsoft.com/mssql/server@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f"
ROOT = Path(__file__).resolve().parents[1]
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", action="store_true", help="Kiểm thêm seed Python, cần extra ingest")
    args = parser.parse_args()
    container = f"ad-de-04-test-{uuid.uuid4().hex[:12]}"
    password = f"Local!{secrets.token_hex(24)}a1"
    env = os.environ | {"MSSQL_SA_PASSWORD": password, "SQLCMDPASSWORD": password}
    sql = ["docker", "exec", container, SQLCMD, "-S", "localhost", "-U", "sa", "-C", "-b", "-l", "2"]
    try:
        network = "cổng loopback tạm" if args.seed else "không mở cổng"
        print(f"Khởi động SQL Server tạm (2 CPU, 3 GB RAM, {network})...", flush=True)
        # Chỉ truyền tên env qua argv; mật khẩu không nằm trong lệnh/log hay file.
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
                *(["-p", "127.0.0.1::1433"] if args.seed else []),
                "--platform",
                "linux/amd64",
                "--memory",
                "3g",
                "--cpus",
                "2",
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                "MSSQL_PID=Developer",
                "-e",
                "MSSQL_SA_PASSWORD",
                "-e",
                "SQLCMDPASSWORD",
                "-v",
                f"{ROOT}:/workspace:ro",
                "-w",
                "/workspace",
                IMAGE,
            ],
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 180
        while True:
            ready = subprocess.run(sql + ["-Q", "SELECT 1"], capture_output=True, timeout=10)
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("SQL Server không sẵn sàng sau 180 giây; kiểm tra Docker/RAM/giả lập amd64.")
            time.sleep(2)
        subprocess.run(
            sql + ["-Q", "CREATE DATABASE retail_db_test_cdc; CREATE DATABASE retail_db_test_fallback;"], check=True
        )
        for database, fixture in [
            ("retail_db_test_cdc", "tests/sql/test_retail_cdc.sql"),
            ("retail_db_test_fallback", "tests/sql/test_retail_fallback.sql"),
        ]:
            print(f"Đang chạy {fixture}...", flush=True)
            subprocess.run(sql + ["-d", database, "-i", fixture], check=True)
        if args.seed:
            for database, files in [
                ("retail_db_test_seed_cdc", ["01_schema.sql", "02_enable_cdc.sql", "03_etl_control.sql"]),
                ("retail_db_test_seed_fallback", ["01_schema.sql", "04_fallback_watermark.sql"]),
            ]:
                subprocess.run(sql + ["-Q", f"CREATE DATABASE {database};"], check=True)
                for file in files:
                    subprocess.run(sql + ["-d", database, "-i", f"src/ingest/sql/{file}"], check=True)
            address = subprocess.check_output(["docker", "port", container, "1433/tcp"], text=True).strip()
            credentials = {"server": "127.0.0.1", "port": address.rsplit(":", 1)[1], "user": "sa", "password": password}
            subprocess.run(
                [sys.executable, "tests/sql/check_seed_retail_db.py"],
                cwd=ROOT,
                input=json.dumps(credentials),
                text=True,
                check=True,
            )
        print("Đạt: regression SQL" + (" và seed Python" if args.seed else "") + " local.", flush=True)
    finally:
        # Chỉ xóa container có tên ngẫu nhiên vừa tạo, không chạm các container khác/image cache.
        subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
