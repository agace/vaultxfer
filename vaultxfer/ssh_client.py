#!/usr/bin/env python3

import os, paramiko

def get_ssh_client(host, port=22, user=None, keyfile=None, password=None, known_hosts=None, timeout=30):
    ssh = paramiko.SSHClient()
    
    if known_hosts:
        ssh.load_host_keys(known_hosts)
    else:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            hostname=host,
            port=port,
            username=user,
            key_filename=keyfile,
            password=password,
            timeout=timeout,
        )
    except paramiko.AuthenticationException:
        print("Authentication failed: wrong username or password/key")
        exit(1)
    except paramiko.SSHException as e:
        print(f"SSH error: {e}")
        exit(1)
    except Exception as e:
        print(f"Connection error: {e}")
        exit(1)
    
    return ssh

def get_sftp(ssh):
    return ssh.open_sftp()

