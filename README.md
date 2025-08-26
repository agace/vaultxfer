# VaultXfer

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![SSH Protocol](https://img.shields.io/badge/protocol-SSH%2FSFTP-lightgrey)
![PyPI](https://img.shields.io/pypi/v/vaultxfer)

A secure and atomic file transfer and synchronization tool built on SSH/SFTP protocols.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
- [Advanced Usage](#advanced-usage)
- [Security Considerations](#security-considerations)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

VaultXfer is a robust command-line interface designed for secure file operations between local and remote systems. Leveraging the proven security of SSH/SFTP protocols, it provides atomic operations and intelligent synchronization for critical data transfer scenarios.

## Key Features

- **Atomic File Operations**: Guaranteed data integrity through temporary file staging and atomic moves
- **Multiple Synchronization Modes**: Unidirectional push/pull and bidirectional synchronization with conflict resolution
- **Pattern-Based Filtering**: Advanced include/exclude patterns for selective file operations
- **Progress Monitoring**: Real-time transfer metrics with human-readable formatting
- **Flexible Authentication**: Support for SSH keys, password authentication, and custom known_hosts management
- **Recursive Directory Handling**: Complete directory tree operations with configurable depth

## Installation

### Prerequisites
- Python 3.10+
- SSH server access on target systems
- Appropriate file permissions for source and destination paths

### Package Installation
```bash
pip install vaultxfer
```
### Source Installation
```bash
git clone https://github.com/agace/vaultxfer.git
cd vaultxfer
pip install -e .
```
### Verification

```bash
vaultxfer --version
```

## Quick Start

### Basic File Transfer

```bash
# Upload file to remote host
vaultxfer user@remotehost put local_file.txt /remote/destination/

# Download file from remote host
vaultxfer user@remotehost get /remote/file.txt local_copy.txt
```

### Directory Synchronization

```bash
# One-way synchronization (local → remote)
vaultxfer user@remotehost sync --push ./local_dir /remote/dir/

# Bidirectional synchronization with conflict resolution
vaultxfer user@remotehost sync --bidirectional ./project /remote/project -r
```

### Advanced Authentication

```bash
# Using specific SSH key and custom port
vaultxfer -i ~/.ssh/custom_key -p 2222 user@remotehost put sensitive_data.db /secure/storage/
```
## Command Reference

### Global Options

| Option                  | Description                                      |
|-------------------------|--------------------------------------------------|
| `-p`, `--port PORT`     | SSH port (default: 22)                           |
| `-i`, `--identity FILE` | Private key file path                            |
| `--known-hosts FILE`    | Use a specific `known_hosts` file                |
| `--timeout SECONDS`     | SSH connection timeout (default: 30)            |
| `-v`, `--version`       | Show version                                     |

### Commands

| Command | Usage                                                       | Description          |
|---------|-------------------------------------------------------------|----------------------|
| `put`   | `vaultxfer user@host put file.txt /remote/path/`            | Upload local file    |
| `get`   | `vaultxfer user@host get /remote/path/file.txt local.txt`   | Download remote file |
| `sync`  | `vaultxfer user@host sync [mode] local/ /remote/`           | Synchronize folders  |

### Sync Options 

| Option              | Description                                           |
|---------------------|-------------------------------------------------------|
| `--push`            | Sync local → remote                                   |
| `--pull`            | Sync remote → local                                   |
| `--bidirectional`   | Sync both sides with conflict resolution              |
| `-r`, `--recursive` | Recursive directory processing                        |
| `--include GLOB`    | Include files matching pattern (multiple allowed)     |
| `--exclude GLOB`    | Exclude files matching pattern (multiple allowed)     |

## Advanced Usage

Pattern-Based Filtering

```bash
vaultxfer user@remotehost sync --push -r photos /remote/photos \
  --include '*.jpg' '*.png' '*.gif' \
  --exclude 'temp_*' '.DS_Store'
```

Custom SSH Configuration

```basht
vaultxfer --known-hosts ~/.ssh/custom_known_hosts --timeout 60 user@remotehost get /important/data.db ./backup/
```

Recursive Operations

```bash
vaultxfer user@remotehost sync --bidirectional -r data /remote/data
```
### Atomic Operation Flow

1. Temporary File Creation: Files are transferred to temporary locations

2. Integrity Verification: Hash verification

3. Atomic Commit: Atomic move operation to final destination

4. Cleanup: Temporary file removal upon success or failure

### Conflict Resolution Logic

- Timestamp Comparison: Files within 5-second window are considered conflicting

- Conflict Preservation: Conflicting files are preserved with `.local` and `.remote` extensions

- Newer File Wins: Older files are overwritten by newer versions

## Security Considerations

### Authentication Methods

- SSH Key Authentication (Recommended): Uses public-key cryptography

- Password Authentication: Secure prompt with no echo

- Host Verification: Custom known_hosts file support

### Data Protection

- All transfers occur over encrypted SSH tunnels

- No sensitive data persistence in logs or temporary files

- Temporary files are properly cleaned up after operations

### Best Practices

- Use SSH keys instead of passwords for automation

- Regularly update and manage known_hosts files

- Implement appropriate file permissions on destination systems

- Use include/exclude patterns to limit scope of operations

## Troubleshooting

### “Permission denied” errors

Ensure your remote user has write permissions to target directories. When syncing, both parent directories and intermediate folders must be writable. On the remote host, check ownership and permissions:

```bash
ssh user@host 'ls -ld /path /path/subdir'
```

### “Local file not found” during push

Confirm the local path you pass is correct and that you’re running the command from the expected working directory, or provide absolute paths.

### Confusing path direction in sync

In the CLI:

- The first path is local.

- The second path is remote.

- `--push` and `--pull` decide direction of change propagation.

- `--bidirectional` reconciles both sides.

### Large directories

Use `-r` for deep trees. Consider `--include, --exclude` to limit scope.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
