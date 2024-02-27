#!/usr/bin/env python3
"""
Objstore handler
"""
__author__ = "Gustavo Luvizotto Cesar"
__email__ = "g.luvizottocesar@utwente.nl"

import hashlib

from config import BUF_SIZE


def calculate_checksum(local_filepath):
    sha256 = hashlib.sha256()
    with open(local_filepath, mode='rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256
