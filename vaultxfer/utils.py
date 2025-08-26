#!/usr/bin/env python3

import hashlib, os, sys, time

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def sha256_remote(sftp, path):
    # Fallback: download stream and hash
    h = hashlib.sha256()
    with sftp.file(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def human_bytes(n):
    for unit in ["B","KiB","MiB","GiB","TiB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"

class Progress:
    def __init__(self, total):
        self.total = total
        self.start = time.time()
        self.done = 0

    def update(self, n):
        self.done += n
        elapsed = time.time() - self.start
        speed = self.done / elapsed if elapsed > 0 else 0
        pct = (self.done / self.total * 100) if self.total else 0
        sys.stdout.write(
            f"\r… {human_bytes(self.done)} / {human_bytes(self.total)} "
            f"({pct:.1f}%) {human_bytes(int(speed))}/s"
        )
        sys.stdout.flush()

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()

