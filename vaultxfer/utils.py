#!/usr/bin/env python3

import hashlib, os, sys, time
from datetime import datetime

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


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def sha256_remote(sftp, path):
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

def format_dry_run_header(operation: str, target: str) -> None:
    print(f"\nDRY RUN: {operation}")
    print(f"Target: {target}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 81)

def format_dry_run_footer() -> None:
    print("-" * 81)
    print("DRY RUN COMPLETE: No changes were made\n")

def format_dry_run_section(title: str) -> None:
    print(f"\n{title}:")

def format_dry_run_item(key: str, value: any, indent: int = 0) -> None:
    indent_str = " " * indent
    print(f"{indent_str}{key:<15}: {value}")

def format_dry_run_action(action: str, indent: int = 0) -> None:
    indent_str = " " * indent
    print(f"{indent_str}→ {action}")

