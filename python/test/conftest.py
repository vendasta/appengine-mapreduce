import os
import signal
import socket
import subprocess
import time

import pytest
import requests
from google.cloud import ndb


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_emulator(host, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            requests.get(f"http://{host}")
            return True
        except requests.ConnectionError:
            time.sleep(0.3)
    return False


@pytest.fixture(scope="session")
def datastore_emulator():
    port = _find_free_port()
    host = f"localhost:{port}"
    os.environ["DATASTORE_EMULATOR_HOST"] = host

    proc = subprocess.Popen(
        [
            "gcloud", "beta", "emulators", "datastore", "start",
            "--no-store-on-disk",
            "--host-port", host,
            "--project", "testbed-test",
            "--consistency", "1.0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if not _wait_for_emulator(host):
        proc.kill()
        raise RuntimeError("Datastore emulator failed to start")

    yield host

    os.kill(proc.pid, signal.SIGTERM)
    proc.wait(timeout=5)
    del os.environ["DATASTORE_EMULATOR_HOST"]


@pytest.fixture(autouse=True)
def ndb_context(datastore_emulator):
    client = ndb.Client(project="testbed-test")
    with client.context(cache_policy=False) as ctx:
        yield ctx
    requests.post(f"http://{datastore_emulator}/reset")
