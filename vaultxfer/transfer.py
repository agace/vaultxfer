#!/usr/bin/env python3

import os
import fnmatch
import stat
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from vaultxfer.utils import (
     human_bytes,
     sha256_file,
     format_dry_run_header,
     format_dry_run_footer,
     format_dry_run_section,
     format_dry_run_item,
     format_dry_run_action,
)

def atomic_upload(sftp, local_path, remote_path):
    if remote_path.endswith("/") or os.path.basename(remote_path) == "":
        remote_path = os.path.join(remote_path, os.path.basename(local_path))

    if os.path.basename(remote_path) == "":
        remote_path = remote_path + ".tmp"

    tmp = remote_path + f".tmp"

    try:
        # ensure remote parent directories exist
        remote_dir = os.path.dirname(remote_path)
        if remote_dir and remote_dir != ".":
            try:
                sftp.chdir(remote_dir)
            except IOError:
                parts = Path(remote_dir).parts
                current_path = ""
                for part in parts:
                    current_path = os.path.join(current_path, part) if current_path else part
                    try:
                        sftp.mkdir(current_path)
                    except IOError:
                        pass

        sftp.put(local_path, tmp)

        try:
            sftp.rename(tmp, remote_path)
        except Exception as rename_exc:
            try:
                sftp.remove(remote_path)
            except Exception:
                pass

            try:
                sftp.rename(tmp, remote_path)
            except Exception:
                try:
                    try:
                        sftp.remove(tmp)
                    except Exception:
                        pass
                    sftp.put(local_path, remote_path)
                    return
                except Exception as put_exc:
                    try:
                        sftp.remove(tmp)
                    except Exception:
                       pass
                    raise RuntimeError(f"Failed to move uploaded temp file to final path: rename error: {rename_exc}; fallback put error: {put_exc}")

        print(f"Uploaded {local_path} to {remote_path}")
    except PermissionError:
        print(f"Error: Permission denied for remote path: {remote_path}")
    except FileNotFoundError:
        print(f"Error: Local file not found: {local_path}")
    except Exception as e:
        try:
           sftp.remove(tmp)
        except Exception:
            pass
        print(f"Error uploading {local_path} to {remote_path}: {str(e)}")

def atomic_download(sftp, remote_path, local_path):
    if os.path.isdir(local_path):
        local_path = os.path.join(local_path, os.path.basename(remote_path))

    if local_path.endswith("/") or os.path.basename(local_path) == "":
        local_path = os.path.join(local_path, os.path.basename(remote_path))

    if os.path.basename(local_path) == "":
        local_path = local_path + ".tmp"

    tmp = local_path + ".tmp"

    try:
        local_dir = os.path.dirname(local_path)
        if local_dir and local_dir != ".":
            os.makedirs(local_dir, exist_ok=True)

        sftp.get(remote_path, tmp)
        os.replace(tmp, local_path)
        print(f"Downloaded {remote_path} to {local_path}")

    except FileNotFoundError:
        print(f"Error: Remote file not found: {remote_path}")
    except PermissionError:
        print(f"Error: Permission denied for remote file: {remote_path}")
    except Exception as e:
        print(f"Error downloading {remote_path} to {local_path}: {str(e)}")
        try:
            os.remove(tmp)
        except:
            pass
        raise RuntimeError(f"Failed to download {remote_path} to {local_path}: {e}")

def list_local(path):
    results = {}
    for root, _, files in os.walk(path):
        relroot = os.path.relpath(root, path)
        for f in files:
            lfile = os.path.join(root, f)
            relfile = os.path.normpath(os.path.join(relroot, f))
            if relfile.startswith(".."):
                relfile = f
            st = os.stat(lfile)
            results[relfile] = (st.st_mode, st.st_size, st.st_mtime)
    return results

def list_remote(sftp, path):
    results = {}

    def _walk(rdir, rel=""):
        for attr in sftp.listdir_attr(rdir):
            rfile = os.path.join(rdir, attr.filename)
            relfile = os.path.join(rel, attr.filename)
            if stat.S_ISDIR(attr.st_mode):
                _walk(rfile, relfile)
            else:
                results[rfile] = (attr.st_mode, attr.st_size, attr.st_mtime)

    _walk(path)
    return results

def sync_push(sftp, local_dir, remote_dir, recursive=False, include=None, exclude=None):
    local_files = list_local(local_dir)
    for rel, meta in local_files.items():
        fname = os.path.basename(rel)

        should_include = True
        if include:
            matched = any(fnmatch.fnmatch(fname, pat) or fnmatch.fnmatch(rel_path, pat) for pat in include)
            should_include = matched

        if should_include and exclude:
            excluded = any(fnmatch.fnmatch(fname, pat) or fnmatch.fnmatch(rel_path, pat) for pat in exclude)
            if excluded:
                should_include = False

        lfile = os.path.join(local_dir, rel)
        rfile = os.path.join(remote_dir, rel)

        # create remote directories recursively
        try:
            sftp.chdir(os.path.dirname(rfile))
        except IOError:
            parts = Path(rfile).parents
            for p in reversed(parts):
                try:
                    sftp.mkdir(str(p))
                except IOError:
                    pass

        atomic_upload(sftp, lfile, rfile)

def sync_pull(sftp, remote_dir, local_dir, recursive=False, include=None, exclude=None):
    remote_files = list_remote(sftp, remote_dir)
    remote_dir = remote_dir.rstrip("/")

    for remote_path, meta in remote_files.items():
        fname = os.path.basename(remote_path)
        if include and not any(fnmatch.fnmatch(fname, pat) for pat in include):
            continue
        if exclude and any(fnmatch.fnmatch(fname, pat) for pat in exclude):
            continue

        rel_path = os.path.relpath(remote_path, remote_dir)
        lfile = os.path.join(local_dir, rel_path)
        os.makedirs(os.path.dirname(lfile), exist_ok=True)
        atomic_download(sftp, remote_path, lfile)

def sync_bidirectional(sftp, local_dir, remote_dir, recursive=False, include=None, exclude=None):
    local_files = list_local(local_dir)

    remote_files_raw = list_remote(sftp, remote_dir)

    remote_dir = remote_dir.rstrip("/")
    remote_files = {}
    for path, meta in remote_files_raw.items():
        rel_path = os.path.relpath(path, remote_dir)
        remote_files[rel_path] = meta

    # set of all relative paths
    all_files = set(local_files.keys()) | set(remote_files.keys())

    for rel in all_files:
        lfile = os.path.join(local_dir, rel)
        rfile = os.path.join(remote_dir, rel)

        lmeta = local_files.get(rel)
        rmeta = remote_files.get(rel)
        fname = os.path.basename(rel)

        # apply include/exclude patterns
        if include and not any(fnmatch.fnmatch(fname, pat) for pat in include):
            continue
        if exclude and any(fnmatch.fnmatch(fname, pat) for pat in exclude):
            continue

        if lmeta and not rmeta:
            atomic_upload(sftp, lfile, rfile)
        elif rmeta and not lmeta:
            os.makedirs(os.path.dirname(lfile), exist_ok=True)
            atomic_download(sftp, rfile, lfile)
        else:
            ltime, rtime = lmeta[2], rmeta[2]
            if abs(ltime - rtime) <= 5:
                # conflict: create a .remote copy locally
                atomic_download(sftp, rfile, lfile + ".remote")
                print(f"  conflict: kept {rel}.local and {rel}.remote")
            elif ltime > rtime:
                atomic_upload(sftp, lfile, rfile)
            else:
                atomic_download(sftp, rfile, lfile)

def dry_run_upload(local_path:str, remote_path:str) -> None:
    if remote_path.endswith("/") or os.path.basename(remote_path) == "":
        remote_path = os.path.join(remote_path, os.path.basename(local_path))

    tmp_path = remote_path + ".tmp"

    try:
      file_size = os.path.getsize(local_path)
      file_hash = sha256_file(local_path)
      mod_time = datetime.fromtimestamp(os.path.getmtime(local_path))
      
      format_dry_run_header("File Upload", f"{local_path} → {remote_path}")
      
      format_dry_run_section("File Details")
      format_dry_run_item("Local path", local_path)
      format_dry_run_item("Remote path", remote_path)
      format_dry_run_item("File size", human_bytes(file_size))
      format_dry_run_item("SHA-256", file_hash)
      format_dry_run_item("Modified", mod_time.strftime('%Y-%m-%d %H:%M:%S'))
      
      format_dry_run_section("Operation Plan")
      format_dry_run_action(f"Create temporary file: {tmp_path}")
      format_dry_run_action(f"Transfer data to temporary file")
      format_dry_run_action(f"Verify SHA-256 hash: {file_hash[:16]}...")
      format_dry_run_action(f"Atomically rename: {tmp_path} → {remote_path}")
      format_dry_run_footer()
      
    except FileNotFoundError:
      format_dry_run_header("File Upload", f"{local_path} → {remote_path}")
      format_dry_run_section("Error")
      format_dry_run_item("Status", "FAILED")
      format_dry_run_item("Reason", f"Local file not found: {local_path}")
      format_dry_run_footer()
    except Exception as e:
      format_dry_run_header("File Upload", f"{local_path} → {remote_path}")
      format_dry_run_section("Error")
      format_dry_run_item("Status", "FAILED")
      format_dry_run_item("Reason", f"Error simulating upload: {e}")
      format_dry_run_footer()

def dry_run_download(remote_path: str, local_path: str) -> None:
    if os.path.isdir(local_path):
        local_path = os.path.join(local_path, os.path.basename(remote_path))
    
    tmp_path = local_path + ".tmp"
    
    format_dry_run_header("File Download", f"{remote_path} → {local_path}")
    
    format_dry_run_section("Operation Plan")
    format_dry_run_action(f"Create temporary file: {tmp_path}")
    format_dry_run_action(f"Transfer data to temporary file")
    format_dry_run_action(f"Atomically rename: {tmp_path} → {local_path}")
    
    format_dry_run_section("Note")
    format_dry_run_item("Information", "Remote file details cannot be determined in dry-run mode")
    format_dry_run_footer()

def dry_run_sync_push(local_dir: str, remote_dir:str, recursive: bool = False, include: Optional[List[str]] = None, exclude: Optional[List[str]] = None) -> None:
    format_dry_run_header("Synchronization push", f"{local_dir} → {remote_dir}")

    format_dry_run_section("Operation Parameters")
    format_dry_run_item("Direction", f"{local_dir} → {remote_dir}")
    format_dry_run_item("Recursive", str(recursive))

    if include:
        format_dry_run_item("Include patterns", ", ".join(include))

    if exclude:
        format_dry_run_item("Exclude patterns", ", ".join(exclude))
    
    try:
        local_files = list_local(local_dir)
        file_count = len(local_files)
        total_size = sum(size for _, size, _ in local_files.values())
        
        format_dry_run_section("File Analysis")
        format_dry_run_item("Files found", str(file_count))
        format_dry_run_item("Total size", human_bytes(total_size))
        
        if file_count > 0:
            format_dry_run_section("Files to be transferred")
            transferred_count = 0
            for rel_path, (mode, size, mtime) in local_files.items():
                fname = os.path.basename(rel_path)
                    
                # apply include/exclude patterns 
                should_include = True

                if include:
                    matched = any(fnmatch.fnmatch(fname, pat) or fnmatch.fnmatch(rel_path, pat) for pat in include)
                    should_include = matched

                if should_include and exclude:
                    excluded = any(fnmatch.fnmatch(fname, pat) or fnmatch.fnmatch(rel_path, pat) for pat in exclude)
                    if excluded:
                        should_include = False

                if not should_include:
                    continue
				 
               	''' 
                if include:
                    should_include = any(fnmatch.fnmatch(fname, pat) for pat in include) or any(fnmatch.fnmatch(rel_path, pat) for pat in include)
                
                if exclude and should_include:
                    should_include = not (any(fnmatch.fnmatch(fname, pat) for pat in exclude) or any(fnmatch.fnmatch(rel_path, pat) for pat in exclude))
                
                if not should_include:
                    continue
               	''' 

                transferred_count += 1
                if transferred_count <= 10:  # first 10 files
                    format_dry_run_item(rel_path, human_bytes(size), 2)
            
            if transferred_count > 10:
                format_dry_run_item(f"... and {transferred_count - 10} more", "", 2)
            elif transferred_count == 0:
                format_dry_run_item("No files match the filter criteria", "", 2)
            else:
                format_dry_run_item(f"\nTotal files to transfer", str(transferred_count), 2)
    
    except Exception as e:
        format_dry_run_section("Error")
        format_dry_run_item("Status", "FAILED")
        format_dry_run_item("Reason", f"Error scanning local directory: {e}")
    
    format_dry_run_footer()
