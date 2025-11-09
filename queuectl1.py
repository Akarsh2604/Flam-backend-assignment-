"""
queuectl_app.py

Simple Streamlit-based CLI-style job queue system.
Run with: streamlit run queuectl_app.py

Features:
- Enqueue jobs (JSON or via form)
- Start/stop worker threads (multiple)
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ)
- Persistent storage in SQLite
- "CLI" input box to issue commands like:
    queuectl enqueue {"id":"job1","command":"sleep 2","max_retries":3}
    queuectl worker start --count 2
    queuectl status
    queuectl list --state pending
    queuectl dlq list
    queuectl dlq retry job1
    queuectl config set max_retries 5

NOTE: This is a minimal demo. Running arbitrary shell commands is potentially unsafe.
Use only trusted commands.
"""

import streamlit as st
import sqlite3
import threading
import time
import json
import uuid
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Dict

DB_PATH = "queuectl.db"



def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_run_at TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS dlq (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        failed_at TEXT NOT NULL,
        last_error TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)
    
    c.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('max_retries', '3')")
    c.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('base_backoff_seconds', '2')")
    conn.commit()
    return conn

conn = init_db()
db_lock = threading.Lock()



def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', ''))
    except Exception:
        return None


def get_config(key: str) -> str:
    with db_lock:
        c = conn.cursor()
        c.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = c.fetchone()
        return row[0] if row else ""


def set_config(key: str, value: str):
    with db_lock:
        c = conn.cursor()
        c.execute("REPLACE INTO config(key, value) VALUES (?, ?)", (key, value))
        conn.commit()



def enqueue_job(job: Dict):
    # job must contain id, command. Optional: max_retries
    jid = job.get('id') or str(uuid.uuid4())
    command = job.get('command')
    if not command:
        raise ValueError('Job must have a command')
    max_retries = int(job.get('max_retries', get_config('max_retries') or 3))
    created = now_iso()
    with db_lock:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, next_run_at) VALUES (?, ?, 'pending', 0, ?, ?, ?, NULL)",
                  (jid, command, max_retries, created, created))
        conn.commit()
    return jid


def list_jobs(state: Optional[str] = None):
    with db_lock:
        c = conn.cursor()
        if state:
            c.execute("SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = ? ORDER BY created_at", (state,))
        else:
            c.execute("SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs ORDER BY created_at")
        rows = c.fetchall()
    keys = ['id','command','state','attempts','max_retries','created_at','updated_at','next_run_at']
    return [dict(zip(keys, r)) for r in rows]


def get_job(jid: str):
    with db_lock:
        c = conn.cursor()
        c.execute("SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE id = ?", (jid,))
        row = c.fetchone()
    if not row:
        return None
    keys = ['id','command','state','attempts','max_retries','created_at','updated_at','next_run_at']
    return dict(zip(keys, row))


def update_job_state(jid: str, state: str, attempts: Optional[int] = None, next_run_at: Optional[str] = None):
    with db_lock:
        c = conn.cursor()
        if attempts is None and next_run_at is None:
            c.execute("UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?", (state, now_iso(), jid))
        elif attempts is None:
            c.execute("UPDATE jobs SET state = ?, next_run_at = ?, updated_at = ? WHERE id = ?", (state, next_run_at, now_iso(), jid))
        elif next_run_at is None:
            c.execute("UPDATE jobs SET state = ?, attempts = ?, updated_at = ? WHERE id = ?", (state, attempts, now_iso(), jid))
        else:
            c.execute("UPDATE jobs SET state = ?, attempts = ?, next_run_at = ?, updated_at = ? WHERE id = ?", (state, attempts, next_run_at, now_iso(), jid))
        conn.commit()


def move_to_dlq(jid: str, last_error: str = ''):
    job = get_job(jid)
    if not job:
        return
    with db_lock:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO dlq(id, command, attempts, max_retries, failed_at, last_error) VALUES (?, ?, ?, ?, ?, ?)",
                  (job['id'], job['command'], job['attempts'], job['max_retries'], now_iso(), last_error))
        c.execute("DELETE FROM jobs WHERE id = ?", (jid,))
        conn.commit()


def retry_dlq(jid: str):
    with db_lock:
        c = conn.cursor()
        c.execute("SELECT id, command, attempts, max_retries FROM dlq WHERE id = ?", (jid,))
        row = c.fetchone()
        if not row:
            raise ValueError('DLQ job not found')
        jid, command, attempts, max_retries = row
        # Put back to jobs with attempts so retry will continue
        c.execute("INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, next_run_at) VALUES (?, ?, 'pending', ?, ?, ?, ?, NULL)",
                  (jid, command, attempts, max_retries, now_iso(), now_iso()))
        c.execute("DELETE FROM dlq WHERE id = ?", (jid,))
        conn.commit()


def list_dlq():
    with db_lock:
        c = conn.cursor()
        c.execute("SELECT id, command, attempts, max_retries, failed_at, last_error FROM dlq ORDER BY failed_at")
        rows = c.fetchall()
    keys = ['id','command','attempts','max_retries','failed_at','last_error']
    return [dict(zip(keys, r)) for r in rows]



def handle_cli_command(cmd: str) -> str:
    cmd = cmd.strip()
    if not cmd:
        return ''
    parts = cmd.split()
    if parts[0] != 'queuectl':
        raise ValueError('Commands must start with queuectl')
    if len(parts) == 1:
        return 'Nothing to do'
    action = parts[1]

    if action == 'enqueue':
        # rest is JSON
        payload = cmd[cmd.find('enqueue') + len('enqueue'):].strip()
        job = json.loads(payload)
        jid = enqueue_job(job)
        return f'enqueued {jid}'

    if action == 'worker':
        if len(parts) < 3:
            raise ValueError('worker requires start/stop')
        sub = parts[2]
        if sub == 'start':
            # look for --count N
            cnt = 1
            if '--count' in parts:
                idx = parts.index('--count')
                if idx + 1 < len(parts):
                    cnt = int(parts[idx+1])
            for i in range(cnt):
                ev = threading.Event()
                w = Worker(stop_event=ev, name=f'worker-{len(st.session_state.workers)+1}')
                st.session_state.worker_events.append(ev)
                st.session_state.workers.append(w)
                w.start()
            return f'started {cnt} worker(s)'
        elif sub == 'stop':
            for ev in st.session_state.worker_events:
                ev.set()
            st.session_state.worker_events = []
            st.session_state.workers = []
            return 'stop signal sent to workers'
        else:
            raise ValueError('unknown worker subcommand')

    if action == 'status':
        jobs = list_jobs()
        s = { 'pending':0, 'running':0, 'completed':0 }
        for j in jobs:
            s[j['state']] = s.get(j['state'], 0) + 1
        return json.dumps(s)

    if action == 'list':
        state = None
        if '--state' in parts:
            idx = parts.index('--state')
            if idx+1 < len(parts):
                state = parts[idx+1]
        jobs = list_jobs(state=state)
        return json.dumps(jobs, indent=2)

    if action == 'dlq':
        if len(parts) < 3:
            raise ValueError('dlq requires list/retry')
        sub = parts[2]
        if sub == 'list':
            return json.dumps(list_dlq(), indent=2)
        if sub == 'retry':
            if len(parts) < 4:
                raise ValueError('provide job id')
            jid = parts[3]
            retry_dlq(jid)
            return f'retried {jid}'
        raise ValueError('unknown dlq subcommand')

    if action == 'config':
        if len(parts) < 3:
            raise ValueError('config requires set/get')
        sub = parts[2]
        if sub == 'set':
            if len(parts) < 5:
                raise ValueError('usage: queuectl config set key value')
            key = parts[3]
            value = parts[4]
            set_config(key, value)
            return f'set {key} = {value}'
        if sub == 'get':
            if len(parts) < 4:
                raise ValueError('usage: queuectl config get key')
            key = parts[3]
            return get_config(key)

    raise ValueError('unknown command')



class Worker(threading.Thread):
    def __init__(self, stop_event: threading.Event, name: str = None):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.name = name or f"worker-{str(uuid.uuid4())[:6]}"

    def run(self):
        print(f"{self.name} started")
        while not self.stop_event.is_set():
            job = fetch_next_job()
            if not job:
                time.sleep(0.5)
                continue
            jid = job['id']
            # mark running
            update_job_state(jid, 'running', attempts=job['attempts'])
            try:
                # Execute command
                # Note: using shell=True for simplicity — only run trusted commands
                start = time.time()
                proc = subprocess.run(job['command'], shell=True, capture_output=True, text=True, timeout=300)
                duration = time.time() - start
                if proc.returncode == 0:
                    update_job_state(jid, 'completed', attempts=job['attempts'] + 1)
                else:
                    raise RuntimeError(f"Exit {proc.returncode}: {proc.stderr.strip()}")
            except Exception as e:
                # failed
                attempts = job['attempts'] + 1
                maxr = job['max_retries']
                base_backoff = int(get_config('base_backoff_seconds') or 2)
                if attempts > maxr:
                    # move to DLQ
                    move_to_dlq(jid, last_error=str(e))
                else:
                    backoff = base_backoff * (2 ** (attempts - 1))
                    next_run = (datetime.utcnow() + timedelta(seconds=backoff)).isoformat() + 'Z'
                    update_job_state(jid, 'pending', attempts=attempts, next_run_at=next_run)
            # small sleep to avoid tight loop
            time.sleep(0.01)
        print(f"{self.name} stopped")


def fetch_next_job():
    now = datetime.utcnow().isoformat() + 'Z'
    with db_lock:
        c = conn.cursor()
        c.execute("SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY created_at LIMIT 1", (now,))
        row = c.fetchone()
    if not row:
        return None
    keys = ['id','command','state','attempts','max_retries','created_at','updated_at','next_run_at']
    return dict(zip(keys, row))

# ------------------------ Streamlit App ------------------------

st.set_page_config(page_title='queuectl (Streamlit CLI)', layout='wide')
st.title('queuectl — Streamlit CLI-style Job Queue')

if 'workers' not in st.session_state:
    st.session_state.workers = []
if 'worker_events' not in st.session_state:
    st.session_state.worker_events = []

col1, col2 = st.columns([2,1])

with col1:
    st.header('CLI Input')
    cli_input = st.text_area('Type a queuectl command (examples in header)', height=120)
    if st.button('Run Command'):
        out = ''
        try:
            out = handle_cli_command(cli_input)
            st.success('Command executed')
        except Exception as e:
            st.error(f'Error: {e}')
        if out:
            st.text(out)
    st.markdown('---')
    st.header('Quick Actions')
    with st.expander('Enqueue a job (form)'):
        jid_input = st.text_input('Job ID (optional)')
        cmd_input = st.text_input('Command (shell) e.g. sleep 2 || echo hi')
        maxr_input = st.number_input('Max retries', min_value=0, value=int(get_config('max_retries') or 3))
        if st.button('Enqueue Form Job'):
            job = {'id': jid_input or None, 'command': cmd_input, 'max_retries': int(maxr_input)}
            try:
                jid = enqueue_job(job)
                st.success(f'Enqueued {jid}')
            except Exception as e:
                st.error(str(e))

    st.markdown('---')
    st.header('Workers')
    wcols = st.columns(3)
    with wcols[0]:
        start_count = st.number_input('Start worker count', min_value=1, max_value=20, value=1, key='start_count')
        if st.button('Start Workers'):
            for i in range(int(start_count)):
                ev = threading.Event()
                w = Worker(stop_event=ev, name=f'worker-{len(st.session_state.workers)+1}')
                st.session_state.worker_events.append(ev)
                st.session_state.workers.append(w)
                w.start()
            st.success(f'Started {start_count} worker(s)')
    with wcols[1]:
        if st.button('Stop All Workers'):
            for ev in st.session_state.worker_events:
                ev.set()
            st.session_state.worker_events = []
            st.session_state.workers = []
            st.success('Stop signal sent to workers')
    with wcols[2]:
        if st.button('Restart Workers (1)'):
            # convenience: stop then start 1
            for ev in st.session_state.worker_events:
                ev.set()
            st.session_state.worker_events = []
            st.session_state.workers = []
            ev = threading.Event()
            w = Worker(stop_event=ev, name='worker-1')
            st.session_state.worker_events.append(ev)
            st.session_state.workers.append(w)
            w.start()
            st.success('Restarted 1 worker')

with col2:
    st.header('Config')
    mr = int(get_config('max_retries') or 3)
    bb = int(get_config('base_backoff_seconds') or 2)
    new_mr = st.number_input('max_retries', min_value=0, value=mr, key='cfg_max_retries')
    new_bb = st.number_input('base_backoff_seconds', min_value=1, value=bb, key='cfg_base_backoff')
    if st.button('Save Config'):
        set_config('max_retries', str(int(new_mr)))
        set_config('base_backoff_seconds', str(int(new_bb)))
        st.success('Config saved')

st.markdown('---')

# Status and listings
st.header('Status')
cols = st.columns(4)
all_jobs = list_jobs()
counts = {s: 0 for s in ['pending','running','completed','failed']}
for j in all_jobs:
    counts[j['state']] = counts.get(j['state'], 0) + 1
cols[0].metric('Pending', counts.get('pending', 0))
cols[1].metric('Running', counts.get('running', 0))
cols[2].metric('Completed', counts.get('completed', 0))
cols[3].metric('In DLQ', len(list_dlq()))

st.header('Jobs')
state_filter = st.selectbox('Filter state', options=['all','pending','running','completed'])
if state_filter == 'all':
    jobs = list_jobs()
else:
    jobs = list_jobs(state=state_filter)

for job in jobs:
    with st.expander(f"{job['id']} — {job['state']}"):
        st.write(job)
        col_a, col_b = st.columns(2)
        if col_a.button('Retry Now', key=f"retry-{job['id']}"):
            # reset attempts and next_run to now
            update_job_state(job['id'], 'pending', attempts=0, next_run_at=None)
            st.success('Job scheduled for immediate retry')
        if col_b.button('Delete Job', key=f"del-{job['id']}"):
            with db_lock:
                c = conn.cursor()
                c.execute("DELETE FROM jobs WHERE id = ?", (job['id'],))
                conn.commit()
            st.warning('Job deleted')

st.markdown('---')
st.header('Dead Letter Queue (DLQ)')
dlq = list_dlq()
for e in dlq:
    with st.expander(f"{e['id']} (failed at {e['failed_at']})"):
        st.write(e)
        c1, c2 = st.columns(2)
        if c1.button('Retry DLQ Job', key=f"dlq-retry-{e['id']}"):
            try:
                retry_dlq(e['id'])
                st.success('Moved back to jobs queue')
            except Exception as ex:
                st.error(str(ex))
        if c2.button('Purge DLQ Job', key=f"dlq-purge-{e['id']}"):
            with db_lock:
                c = conn.cursor()
                c.execute("DELETE FROM dlq WHERE id = ?", (e['id'],))
                conn.commit()
            st.warning('DLQ job purged')

st.markdown('---')
st.header('Help / Examples')
st.code('''
queuectl enqueue {"id":"job1","command":"sleep 2","max_retries":3}
queuectl worker start --count 3
queuectl worker stop
queuectl status
queuectl list --state pending
queuectl dlq list
queuectl dlq retry job1
queuectl config set max_retries 5
''')
