#!/usr/bin/env python3
"""Hot-patch the running DVR container with the local app/ files.

Uploads `app/static/index.html` and `app/main.py` to `/tmp` on the BlueOS
Pi via SFTP, then `docker cp`s them into the container and restarts it.
Used only during interactive development on the workstation; not shipped
in the image. Requires either pi/raspberry password or a previously
installed pubkey on the device.
"""

from __future__ import annotations

import os
import sys

import paramiko


HOST = "192.168.2.2"
USER = "pi"
PASSWORD = "raspberry"  # default BlueOS password; pubkey takes precedence if installed


def _resolve_container(client: paramiko.SSHClient) -> str:
    stdin, stdout, stderr = client.exec_command(
        "docker ps --format '{{.Names}}' | grep -Ei 'brexplore|explorehd.*dvr' | head -1",
        timeout=15,
    )
    name = stdout.read().decode().strip()
    if not name:
        raise RuntimeError(
            "No DVR container found. Available: "
            + paramiko_run(client, "docker ps --format '{{.Names}}'").strip()
        )
    return name


def paramiko_run(client: paramiko.SSHClient, cmd: str, *, timeout: int = 60) -> str:
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout, get_pty=False)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    rc = stdout.channel.recv_exit_status()
    if rc != 0:
        raise RuntimeError(f"`{cmd}` exited {rc}\nstdout: {out}\nstderr: {err}")
    if err:
        sys.stderr.write(err)
    return out


def main() -> int:
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # (local_path, container_path) pairs. Everything we expect to iterate on
    # during a development session lives here.
    files = [
        (os.path.join(here, "app", "static", "index.html"), "/app/static/index.html"),
        (os.path.join(here, "app", "main.py"), "/app/main.py"),
        (os.path.join(here, "app", "recorder.py"), "/app/recorder.py"),
    ]
    for local, _ in files:
        if not os.path.isfile(local):
            print(f"missing {local}", file=sys.stderr)
            return 2

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        username=USER,
        password=PASSWORD,
        timeout=15,
        banner_timeout=15,
        auth_timeout=15,
    )
    try:
        print("==> Resolving container ...")
        container = _resolve_container(client)
        print(f"    Container: {container}")

        print("==> Uploading files via SFTP ...")
        sftp = client.open_sftp()
        for local, _ in files:
            tmp = "/tmp/" + os.path.basename(local)
            sftp.put(local, tmp)
        sftp.close()

        print("==> docker cp into container ...")
        for local, remote in files:
            tmp = "/tmp/" + os.path.basename(local)
            paramiko_run(client, f"docker cp {tmp} {container}:{remote}")

        print("==> docker restart ...")
        paramiko_run(client, f"docker restart {container}")

        print("==> Done. Hard-refresh the extension tab (Shift+Reload).")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
