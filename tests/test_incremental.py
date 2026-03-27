import pytest
import os

OFFSET_FILE = "/tmp/test_offset.txt"

def get_last_offset():
    if not os.path.exists(OFFSET_FILE):
        return 0
    with open(OFFSET_FILE, "r") as f:
        return int(f.read().strip())

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))

def test_offset_initialization():
    if os.path.exists(OFFSET_FILE):
        os.remove(OFFSET_FILE)

    assert get_last_offset() == 0

def test_offset_update():
    save_offset(1000)
    assert get_last_offset() == 1000
