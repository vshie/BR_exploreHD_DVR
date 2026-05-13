#!/usr/bin/env python3
"""Tiny helper: run a shell command on the BlueOS Pi over SSH (pi/raspberry).

Used only during interactive debugging from the developer workstation; not
shipped in the container image. Reads the command from argv (joined with
spaces) or from stdin if "-" is the first arg.
"""

from __future__ import annotations

import sys

import paramiko


def main() -> int:
    host = "192.168.2.2"
    user = "pi"
    password = "raspberry"
    if len(sys.argv) < 2:
        print("usage: _ssh_run.py <cmd...>", file=sys.stderr)
        return 2
    if sys.argv[1] == "-":
        cmd = sys.stdin.read()
    else:
        cmd = " ".join(sys.argv[1:])
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=15, banner_timeout=15, auth_timeout=15)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=120, get_pty=False)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    rc = stdout.channel.recv_exit_status()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)
    client.close()
    return rc


if __name__ == "__main__":
    sys.exit(main())
