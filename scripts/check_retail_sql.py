#!/usr/bin/env python3
"""Chạy regression SQL trên Docker tạm; không kết nối Azure hay database của người dùng."""

from __future__ import annotations

import os
import secrets
import subprocess
import time
import uuid
from pathlib import Path

# SQL Server 2022 CU28 Developer, pin digest để local và CI dùng cùng engine.
IMAGE = "mcr.microsoft.com/mssql/server@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f"
ROOT = Path(__file__).resolve().parents[1]
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


def main() -> None:
    container = f"ad-de-04-test-{uuid.uuid4().hex[:12]}"
    password = f"Local!{secrets.token_hex(24)}a1"
    env = os.environ | {"MSSQL_SA_PASSWORD": password, "SQLCMDPASSWORD": password}
    sql = ["docker", "exec", container, SQLCMD, "-S", "localhost", "-U", "sa", "-C", "-b", "-l", "2"]
    try:
        print("Khởi động SQL Server tạm (2 CPU, 3 GB RAM, không mở cổng)...", flush=True)
        # Chỉ truyền tên env qua argv; mật khẩu không nằm trong lệnh/log hay file.
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container,
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
        print("Đạt: regression CDC và fallback trên SQL Server local.", flush=True)
    finally:
        # Chỉ xóa container có tên ngẫu nhiên vừa tạo, không chạm các container khác/image cache.
        subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
