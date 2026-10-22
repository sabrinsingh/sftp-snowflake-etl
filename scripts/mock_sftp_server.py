#!/usr/bin/env python3
"""
scripts/mock_sftp_server.py
---------------------------
A lightweight, pure-Python SFTP Server built using Paramiko.
Runs locally on 127.0.0.1:2222 with ZERO Docker installation required.

Default credentials:
  Username: retail_client
  Password: retail_secret_pass
  Root directory: ./sftp_root
"""

import os
import sys
import socket
import logging
import threading
import paramiko
from paramiko import SFTPServer, SFTPServerInterface, SFTP_OK, SFTP_NO_SUCH_FILE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SFTP-SERVER] %(message)s"
)
logger = logging.getLogger("MockSFTPServer")

HOST = "127.0.0.1"
PORT = 2222
USER = "retail_client"
PASS = "retail_secret_pass"
BASE_DIR = os.path.abspath("sftp_root")
KEY_FILE = os.path.join(os.path.dirname(__file__), "mock_host_rsa.key")


def ensure_host_key():
    """Generates a temporary RSA host key for the SSH server if not present."""
    if not os.path.exists(KEY_FILE):
        logger.info(f"Generating new RSA host key at {KEY_FILE}...")
        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file(KEY_FILE)
    return paramiko.RSAKey(filename=KEY_FILE)


class LocalSFTPInterface(SFTPServerInterface):
    def __init__(self, server_ctxt, *args, **kwargs):
        super().__init__(server_ctxt, *args, **kwargs)

    def _resolve(self, path):
        # Normalize and root the virtual path to BASE_DIR
        clean = path.lstrip("/")
        full_path = os.path.abspath(os.path.join(BASE_DIR, clean))
        if not full_path.startswith(BASE_DIR):
            return BASE_DIR
        return full_path

    def list_folder(self, path):
        real_path = self._resolve(path)
        if not os.path.isdir(real_path):
            return SFTP_NO_SUCH_FILE
        out = []
        for name in os.listdir(real_path):
            file_path = os.path.join(real_path, name)
            try:
                st = os.stat(file_path)
                attr = paramiko.SFTPAttributes.from_stat(st)
                attr.filename = name
                out.append(attr)
            except OSError:
                continue
        return out

    def stat(self, path):
        real_path = self._resolve(path)
        try:
            return paramiko.SFTPAttributes.from_stat(os.stat(real_path))
        except OSError:
            return SFTP_NO_SUCH_FILE

    def lstat(self, path):
        return self.stat(path)

    def open(self, path, flags, attr):
        real_path = self._resolve(path)
        try:
            mode = "rb"
            if flags & os.O_WRONLY:
                mode = "wb"
            elif flags & os.O_RDWR:
                mode = "r+b"
            f = open(real_path, mode)
            obj = paramiko.SFTPHandle(flags)
            obj.readfile = f
            obj.writefile = f
            return obj
        except OSError:
            return SFTP_NO_SUCH_FILE


class SSHServer(paramiko.ServerInterface):
    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username, password):
        if username == USER and password == PASS:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "password"


def handle_client(sock, host_key):
    try:
        t = paramiko.Transport(sock)
        t.add_server_key(host_key)
        # Register subsystem handler: SFTPServer with LocalSFTPInterface
        t.set_subsystem_handler("sftp", SFTPServer, LocalSFTPInterface)
        server = SSHServer()
        t.start_server(server=server)
        chan = t.accept(20)
        if chan is None:
            t.close()
            return
        while t.is_active():
            t.join(1.0)
    except Exception as e:
        logger.error(f"Client connection error: {e}")


def start_server():
    os.makedirs(os.path.join(BASE_DIR, "incoming"), exist_ok=True)
    host_key = ensure_host_key()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(10)

    logger.info(f"===========================================================")
    logger.info(f"Mock SFTP Server listening on {HOST}:{PORT}")
    logger.info(f"Username: {USER} | Password: {PASS}")
    logger.info(f"Serving files from directory: {BASE_DIR}")
    logger.info(f"Press Ctrl+C to stop.")
    logger.info(f"===========================================================")

    try:
        while True:
            client_sock, client_addr = listener.accept()
            logger.info(f"Connection from {client_addr}")
            thread = threading.Thread(target=handle_client, args=(client_sock, host_key), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        logger.info("Stopping SFTP Server...")
    finally:
        listener.close()


if __name__ == "__main__":
    start_server()
