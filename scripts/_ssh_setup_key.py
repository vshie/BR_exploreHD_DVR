#!/usr/bin/env python3
"""One-shot helper: push our local SSH pubkey to pi@192.168.2.2.

After this runs, subsequent ssh/scp invocations won't prompt for a password.
Only used during interactive debugging from the developer workstation.
"""

from __future__ import annotations

import os
import sys

import paramiko


def main() -> int:
    host = "192.168.2.2"
    user = "pi"
    password = "raspberry"
    pubkey_path = os.path.expanduser("~/.ssh/id_ed25519_brdvr.pub")
    with open(pubkey_path, "r", encoding="utf-8") as f:
        pubkey = f.read().strip()
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=15)
    cmd = (
        "mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
        "touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && "
        f"grep -qxF '{pubkey}' ~/.ssh/authorized_keys || echo '{pubkey}' >> ~/.ssh/authorized_keys && "
        "echo OK"
    )
    stdin, stdout, stderr = client.exec_command(cmd, timeout=20)
    out = stdout.read().decode()
    err = stderr.read().decode()
    rc = stdout.channel.recv_exit_status()
    print("rc:", rc)
    print("stdout:", out.strip())
    if err:
        print("stderr:", err.strip())
    client.close()
    return rc


if __name__ == "__main__":
    sys.exit(main())
