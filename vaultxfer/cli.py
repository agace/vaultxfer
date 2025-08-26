#!/usr/bin/env python3

import argparse
import sys
import getpass
import textwrap
from typing import Optional
from vaultxfer.ssh_client import get_ssh_client, get_sftp
from vaultxfer.transfer import (
    atomic_upload,
    atomic_download,
    sync_push,
    sync_pull,
    sync_bidirectional,
)

class error_formatter(argparse.ArgumentParser):
    def error(self, message):
        if len(sys.argv) == 1:
          self.print_help(sys.stderr)
          #args = {'message': message}
          self.exit(2) 
        else:
          self.print_usage(sys.stderr)
          self.exit(2, f"error: {message}\n")

class help_formatter(argparse.RawTextHelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        return super()._format_usage(usage, actions, groups, prefix).capitalize()
    
    def _format_action(self, action):
        # custom formatting for subparsers
        if isinstance(action, argparse._SubParsersAction):
            parts = []
            for subaction in action._get_subactions():
                parts.append(f"  {subaction.dest:12} {subaction.help}")
            return "\n".join(parts) + "\n"
        return super()._format_action(action)


def parse_target(target: str) -> tuple[str, str]:
    # parse target argument into user and host components"
    if "@" in target:
        user, host = target.split("@", 1)
        if not user or not host:
          raise ValueError("Target must be in format user@host")
        return user, host
    raise ValueError("Target must be in format user@host")

def main():
    # create main parser with enhanced formatting
    parser = error_formatter(
        description=textwrap.dedent("""
            VaultXfer - Secure File Transfer and Synchronization Tool
            
            Transfer files securely between local and remote systems using SSH/SFTP
            with atomic operations and synchronization capabilities.
        """),
        formatter_class=help_formatter,
        epilog=textwrap.dedent("""
            Examples:
              vaultxfer user@host get /remote/file.txt ./local.txt
              vaultxfer -i ~/.ssh/id_rsa user@host put ./file.txt /remote/path/
              vaultxfer --timeout 10 user@host sync --push ./local_dir /remote_dir
              vaultxfer -p 2222 user@host sync --pull --recursive /remote_dir ./local_dir
        """),
        add_help=False  # add -h/--help manually for better control
    )
    
    # global options
    global_group = parser.add_argument_group("Global Options")
    global_group.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS,
                             help="Show this help message and exit")
    global_group.add_argument("-p", "--port", type=int, default=22, 
                             help="SSH port number (default: %(default)d)")
    global_group.add_argument("-i", "--identity", metavar="FILE", 
                             help="SSH private key file for authentication")
    global_group.add_argument("--known-hosts", metavar="FILE", 
                             help="Custom known_hosts file path")
    global_group.add_argument("--timeout", type=int, default=30, metavar="SECONDS",
                             help="Connection timeout in seconds (default: %(default)d)")
    global_group.add_argument("-v", "--version", action="version", version="%(prog)s 1.0",
                             help="Show program version")
    
    # positional arguments
    parser.add_argument("target", help="Remote target in format [user@]host")
    
    # subcommands
    subparsers = parser.add_subparsers(
        dest="cmd", 
        required=True, 
        title="Commands",
        description="For detailed help on each command, use: vaultxfer TARGET COMMAND --help"
    )
    
    # PUT command
    put_parser = subparsers.add_parser(
        "put", 
        help="Upload file to remote host",
        description="Upload a local file to the remote server with atomic operation",
        formatter_class=help_formatter
    )
    put_parser.add_argument("local", help="Local source file path")
    put_parser.add_argument("remote", help="Remote destination path")
    
    # GET command
    get_parser = subparsers.add_parser(
        "get", 
        help="Download file from remote host",
        description="Download a remote file to the local system with atomic operation",
        formatter_class=help_formatter
    )
    get_parser.add_argument("remote", help="Remote source file path")
    get_parser.add_argument("local", help="Local destination path")
    
    # SYNC command
    sync_parser = subparsers.add_parser(
        "sync", 
        help="Synchronize directories",
        description="Synchronize directories between local and remote systems",
        formatter_class=help_formatter
    )
    sync_mode = sync_parser.add_mutually_exclusive_group(required=True)
    sync_mode.add_argument("--push", action="store_true", 
                          help="Push local changes to remote (one-way sync)")
    sync_mode.add_argument("--pull", action="store_true", 
                          help="Pull remote changes to local (one-way sync)")
    sync_mode.add_argument("--bidirectional", action="store_true", 
                          help="Two-way synchronization")
    sync_parser.add_argument("local_dir", help="Local directory path")
    sync_parser.add_argument("remote_dir", help="Remote directory path")
    sync_parser.add_argument("-r", "--recursive", action="store_true", 
                           help="Recursive synchronization")
    sync_parser.add_argument("--include", nargs="*", metavar="GLOB",
                           help="Include files matching pattern(s)")
    sync_parser.add_argument("--exclude", nargs="*", metavar="GLOB",
                           help="Exclude files matching pattern(s)")
    
    args = parser.parse_args()
    
    try:
        user, host = parse_target(args.target)
    except ValueError:
        print(f"Error: Invalid target format '{args.target}'. Expected user@host")
        sys.exit(1)
    
    # get password if no identity provided
    password = None
    if not args.identity:
        try:
            password = getpass.getpass(f"Password for {user}@{host}: ")
        except KeyboardInterrupt:
            print("\nOperation cancelled")
            sys.exit(1)
    
    # establish connection
    try:
        with get_ssh_client(
            host=host,
            port=args.port,
            user=user,
            keyfile=args.identity,
            password=password,
            known_hosts=args.known_hosts,
            timeout=args.timeout,
        ) as ssh:
            with get_sftp(ssh) as sftp:
                if args.cmd == "put":
                    atomic_upload(sftp, args.local, args.remote)
                elif args.cmd == "get":
                    atomic_download(sftp, args.remote, args.local)
                elif args.cmd == "sync":
                    sync_args = {
                        'recursive': args.recursive,
                        'include': args.include,
                        'exclude': args.exclude,
                    }
                    
                    if args.push:
                        sync_push(sftp, args.local_dir, args.remote_dir, **sync_args)
                    elif args.pull:
                        sync_pull(sftp, args.remote_dir, args.local_dir, **sync_args)
                    elif args.bidirectional:
                        sync_bidirectional(sftp, args.local_dir, args.remote_dir, **sync_args)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
