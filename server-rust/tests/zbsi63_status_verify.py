#!/usr/bin/env python3
"""Run status HTTP suite against an owned disposable PostgreSQL container."""
import json
import os
import subprocess
import time
import uuid

owner = 'zbsi63-status-' + uuid.uuid4().hex
cid = None

def run(*args, env=None):
    result = subprocess.run(args, env=env, check=False)
    if result.returncode:
        raise RuntimeError(f'{args[0]} failed with {result.returncode}')

try:
    cid = subprocess.check_output([
        'docker', 'run', '-d', '--name', owner, '--label', f'multica.test.owner={owner}',
        '--memory=768m', '--cpus=1', '-p', '127.0.0.1::5432',
        '-e', 'POSTGRES_PASSWORD=synthetic_zbsi63', '-e', 'POSTGRES_DB=postgres',
        'postgres:16',
    ], text=True).strip()
    meta = json.loads(subprocess.check_output(['docker','inspect',cid], text=True))[0]
    assert meta['Config']['Labels']['multica.test.owner'] == owner
    port_entry = meta['NetworkSettings']['Ports']['5432/tcp'][0]
    assert port_entry['HostIp'] == '127.0.0.1' and port_entry['HostPort'] != '5432'
    for _ in range(90):
        if subprocess.run(['docker','exec',cid,'pg_isready','-U','postgres'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode == 0:
            break
        time.sleep(.3)
    else:
        raise RuntimeError('database not ready')
    for db, suite in [('multica_zbsi65', 'account_catalog_write_statuses'), ('multica_zbsi69', 'queries_sprint_pull_requests')]:
        run('docker','exec',cid,'createdb','-U','postgres',db)
        url = f'postgres://postgres:synthetic_zbsi63@127.0.0.1:{port_entry["HostPort"]}/{db}'
        env = dict(os.environ, RUST_DATABASE_URL=url, RUST_DEV_DATABASE_CONFIRM=db,
                   RUST_API_TEST_DATABASE_URL=url, RUST_MIGRATIONS_ROOT=os.getcwd())
        print(f'Verifying {suite} on owned loopback {db}', flush=True)
        run('/home/zbstech/.cargo/bin/cargo','run','--offline','--locked','--','migrate-up',env=env)
        run('/home/zbstech/.cargo/bin/cargo','test','--offline','--locked','--test',suite,
            '--','--ignored','--test-threads=1',env=env)

finally:
    if cid:
        meta = json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
        if meta['Config']['Labels'].get('multica.test.owner') != owner:
            raise RuntimeError('cleanup ownership mismatch')
        run('docker','rm','-f','-v',cid)
