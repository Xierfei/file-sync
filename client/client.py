import os
import json
import hashlib
import time
import threading
import requests
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'client_config.json')
CHUNK_SIZE = 1 * 1024 * 1024  # 1MB
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1  # seconds, exponential backoff

# ── Config ──────────────────────────────────────────────────────────────────

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"server_url": ""}


def save_config_file(cfg):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


client_config = load_config()

# ── Sync State ──────────────────────────────────────────────────────────────

sync_lock = threading.Lock()
sync_state = {
    "running": False,
    "stopping": False,
    "pending": [],       # [{relative_path, size, status}]
    "completed": [],     # [{relative_path, size, result}]
    "failed": [],        # [{relative_path, size, error}]
    "current_file": None,
    "current_progress": 0,  # bytes uploaded for current file
    "current_size": 0,
    "speed": 0,           # bytes per second
    "total_files": 0,
    "synced_files": 0,
    "skipped_files": 0,
    "error_msg": None,
}

# Speed tracker
speed_lock = threading.Lock()
speed_samples = []  # [(timestamp, bytes)]


def record_bytes(nbytes):
    with speed_lock:
        now = time.time()
        speed_samples.append((now, nbytes))
        # Keep only last 3 seconds of samples
        cutoff = now - 3.0
        while speed_samples and speed_samples[0][0] < cutoff:
            speed_samples.pop(0)


def get_speed():
    with speed_lock:
        now = time.time()
        cutoff = now - 3.0
        while speed_samples and speed_samples[0][0] < cutoff:
            speed_samples.pop(0)
        if not speed_samples:
            return 0
        total_bytes = sum(b for _, b in speed_samples)
        elapsed = now - speed_samples[0][0] if len(speed_samples) > 1 else 1.0
        if elapsed <= 0:
            elapsed = 1.0
        return total_bytes / elapsed

# ── Helpers ─────────────────────────────────────────────────────────────────

def compute_md5(filepath):
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def add_timestamp_to_filename(relative_path):
    """Add timestamp before extension: file.txt -> file_20260407_153022.txt"""
    base, ext = os.path.splitext(relative_path)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{base}_{ts}{ext}"


def request_with_retry(method, url, max_retries=MAX_RETRIES, **kwargs):
    """HTTP request with exponential backoff retry."""
    kwargs.setdefault('timeout', 30)
    last_error = None
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, **kwargs)
            return resp
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            last_error = e
            if sync_state['stopping']:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            delay = min(delay, 60)  # cap at 60 seconds
            time.sleep(delay)
    raise last_error

# ── Web UI ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

# ── Config APIs ─────────────────────────────────────────────────────────────

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(client_config)


@app.route('/api/config', methods=['POST'])
def set_config():
    data = request.get_json()
    url = data.get('server_url', '').strip().rstrip('/')
    if url:
        client_config['server_url'] = url
        save_config_file(client_config)
    return jsonify(client_config)

# ── Server Proxy APIs ──────────────────────────────────────────────────────

@app.route('/api/server/folders', methods=['GET'])
def server_folders():
    url = client_config.get('server_url', '')
    if not url:
        return jsonify({'error': '未配置服务器地址'}), 400
    try:
        resp = requests.get(f'{url}/api/folders', timeout=5)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({'error': f'连接服务器失败: {str(e)}'}), 502


@app.route('/api/server/test', methods=['GET'])
def test_connection():
    url = client_config.get('server_url', '')
    if not url:
        return jsonify({'error': '未配置服务器地址'}), 400
    try:
        resp = requests.get(f'{url}/api/folders', timeout=5)
        if resp.status_code == 200:
            return jsonify({'ok': True, 'folders': len(resp.json())})
        return jsonify({'error': f'服务器响应异常: {resp.status_code}'}), 502
    except Exception as e:
        return jsonify({'error': f'连接失败: {str(e)}'}), 502

# ── Local Files API ────────────────────────────────────────────────────────

@app.route('/api/local/browse', methods=['POST'])
def local_browse():
    data = request.get_json()
    path = data.get('path', '').strip()
    if not path:
        # Return drives on Windows
        if os.name == 'nt':
            import string
            drives = [f'{d}:\\' for d in string.ascii_uppercase if os.path.exists(f'{d}:\\')]
            return jsonify({'dirs': drives, 'current': ''})
        return jsonify({'dirs': ['/'], 'current': '/'})
    if not os.path.isdir(path):
        return jsonify({'error': '目录不存在'}), 400
    try:
        items = []
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            if os.path.isdir(full) and not name.startswith('.'):
                items.append(name)
        parent = os.path.dirname(path)
        return jsonify({'dirs': items, 'current': path, 'parent': parent if parent != path else None})
    except PermissionError:
        return jsonify({'error': '无访问权限'}), 403

# ── Sync APIs ──────────────────────────────────────────────────────────────

@app.route('/api/sync/start', methods=['POST'])
def sync_start():
    if sync_state['running']:
        return jsonify({'error': '同步正在进行中'}), 409

    data = request.get_json()
    local_dir = data.get('local_dir', '').strip()
    folder_id = data.get('folder_id')

    if not local_dir or not os.path.isdir(local_dir):
        return jsonify({'error': '本地目录不存在'}), 400
    if not folder_id:
        return jsonify({'error': '请选择服务器目标文件夹'}), 400
    if not client_config.get('server_url'):
        return jsonify({'error': '未配置服务器地址'}), 400

    # Reset state
    with sync_lock:
        sync_state.update({
            "running": True,
            "stopping": False,
            "pending": [],
            "completed": [],
            "failed": [],
            "current_file": None,
            "current_progress": 0,
            "current_size": 0,
            "speed": 0,
            "total_files": 0,
            "synced_files": 0,
            "skipped_files": 0,
            "error_msg": None,
        })

    t = threading.Thread(
        target=sync_worker,
        args=(local_dir, folder_id, client_config['server_url']),
        daemon=True
    )
    t.start()
    return jsonify({'ok': True})


@app.route('/api/sync/stop', methods=['POST'])
def sync_stop():
    with sync_lock:
        sync_state['stopping'] = True
    return jsonify({'ok': True})


@app.route('/api/sync/status', methods=['GET'])
def sync_status():
    with sync_lock:
        state = dict(sync_state)
        state['speed'] = get_speed()
        # Limit list sizes for response
        state['pending'] = state['pending'][:100]
        state['completed'] = state['completed'][-100:]
        state['failed'] = state['failed'][-50:]
    return jsonify(state)

# ── Sync Worker ─────────────────────────────────────────────────────────────

def sync_worker(local_dir, folder_id, server_url):
    try:
        # 1. Scan local files
        all_files = []
        for root, dirs, files in os.walk(local_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for fname in files:
                if fname.startswith('.'):
                    continue
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, local_dir)
                try:
                    size = os.path.getsize(full_path)
                except OSError:
                    continue
                all_files.append({
                    'full_path': full_path,
                    'relative_path': rel_path,
                    'size': size,
                })

        with sync_lock:
            sync_state['total_files'] = len(all_files)
            sync_state['pending'] = [
                {'relative_path': f['relative_path'], 'size': f['size']}
                for f in all_files
            ]

        # 2. Process each file
        for file_info in all_files:
            if sync_state['stopping']:
                break

            rel = file_info['relative_path']
            full = file_info['full_path']
            size = file_info['size']

            with sync_lock:
                sync_state['current_file'] = rel
                sync_state['current_progress'] = 0
                sync_state['current_size'] = size

            try:
                result = upload_file(server_url, folder_id, full, rel, size)

                with sync_lock:
                    # Remove from pending
                    sync_state['pending'] = [
                        p for p in sync_state['pending']
                        if p['relative_path'] != rel
                    ]
                    if result == 'skipped':
                        sync_state['skipped_files'] += 1
                        sync_state['completed'].append({
                            'relative_path': rel,
                            'size': size,
                            'result': '已存在，跳过'
                        })
                    else:
                        sync_state['synced_files'] += 1
                        sync_state['completed'].append({
                            'relative_path': rel,
                            'size': size,
                            'result': result
                        })

            except Exception as e:
                with sync_lock:
                    sync_state['pending'] = [
                        p for p in sync_state['pending']
                        if p['relative_path'] != rel
                    ]
                    sync_state['failed'].append({
                        'relative_path': rel,
                        'size': size,
                        'error': str(e)
                    })

    except Exception as e:
        with sync_lock:
            sync_state['error_msg'] = f'同步出错: {str(e)}'
    finally:
        with sync_lock:
            sync_state['running'] = False
            sync_state['stopping'] = False
            sync_state['current_file'] = None


def upload_file(server_url, folder_id, local_path, relative_path, file_size):
    """Upload a single file with MD5 check, chunked transfer, and retry."""

    # 1. Compute MD5
    md5 = compute_md5(local_path)

    # 2. Check with server
    resp = request_with_retry('POST', f'{server_url}/api/check', json={
        'folder_id': folder_id,
        'relative_path': relative_path,
        'md5': md5,
    })
    check_result = resp.json()

    if check_result.get('status') == 'exists':
        return 'skipped'

    # If conflict, rename with timestamp
    upload_path = relative_path
    result_msg = '上传成功'
    if check_result.get('status') == 'conflict':
        upload_path = add_timestamp_to_filename(relative_path)
        result_msg = f'重名冲突，已重命名为 {upload_path}'

    # 3. Init upload session
    resp = request_with_retry('POST', f'{server_url}/api/upload/init', json={
        'folder_id': folder_id,
        'relative_path': upload_path,
        'md5': md5,
        'size': file_size,
    })
    init_data = resp.json()
    upload_id = init_data['upload_id']
    chunk_size = init_data['chunk_size']
    total_chunks = init_data['total_chunks']
    received_chunks = set(init_data.get('received_chunks', []))

    # 4. Upload chunks
    with open(local_path, 'rb') as f:
        for chunk_idx in range(total_chunks):
            if sync_state['stopping']:
                raise Exception('用户停止同步')

            f.seek(chunk_idx * chunk_size)
            chunk_data = f.read(chunk_size)

            # Skip already received chunks (resume)
            if chunk_idx in received_chunks:
                with sync_lock:
                    sync_state['current_progress'] = min(
                        (chunk_idx + 1) * chunk_size, file_size
                    )
                continue

            # Upload with retry
            upload_chunk_with_retry(
                server_url, upload_id, chunk_idx, chunk_data
            )

            with sync_lock:
                sync_state['current_progress'] = min(
                    (chunk_idx + 1) * chunk_size, file_size
                )

    # 5. Complete upload
    resp = request_with_retry('POST', f'{server_url}/api/upload/complete/{upload_id}')
    result = resp.json()
    if 'error' in result:
        raise Exception(result['error'])

    return result_msg


def upload_chunk_with_retry(server_url, upload_id, chunk_index, chunk_data):
    """Upload a single chunk with exponential backoff retry."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            if sync_state['stopping']:
                raise Exception('用户停止同步')

            start_time = time.time()
            resp = requests.post(
                f'{server_url}/api/upload/chunk/{upload_id}/{chunk_index}',
                data=chunk_data,
                headers={'Content-Type': 'application/octet-stream'},
                timeout=60,
            )

            elapsed = time.time() - start_time
            record_bytes(len(chunk_data))

            if resp.status_code == 200:
                return resp.json()
            else:
                last_error = Exception(f'服务器错误 {resp.status_code}: {resp.text}')

        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_error = e

        if sync_state['stopping']:
            raise Exception('用户停止同步')

        delay = RETRY_BASE_DELAY * (2 ** attempt)
        delay = min(delay, 60)
        time.sleep(delay)

    raise Exception(f'分片 {chunk_index} 上传失败 (重试{MAX_RETRIES}次): {last_error}')


if __name__ == '__main__':
    print("文件同步客户端启动: http://0.0.0.0:8081")
    app.run(host='0.0.0.0', port=8081, threaded=True)
