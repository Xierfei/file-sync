import os
import json
import hashlib
import uuid
import time
import threading
import shutil
from pathlib import Path
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server_config.json')
SESSIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'upload_sessions.json')
UPLOAD_TEMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads_temp')
CHUNK_SIZE = 10 * 1024 * 1024  # 10MB

config_lock = threading.Lock()
session_lock = threading.Lock()
scan_status = {}  # folder_id -> {status, scanned, total}

# ── Config ──────────────────────────────────────────────────────────────────

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"folders": [], "next_id": 1}


def save_config(cfg):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


config = load_config()

# ── Upload Sessions ─────────────────────────────────────────────────────────

upload_sessions = {}  # upload_id -> session dict
session_lookup = {}   # (folder_id, relative_path, md5) -> upload_id


def load_sessions():
    global upload_sessions, session_lookup
    if os.path.exists(SESSIONS_FILE):
        with open(SESSIONS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        upload_sessions = data.get('sessions', {})
        for uid, s in upload_sessions.items():
            key = (s['folder_id'], s['relative_path'], s['md5'])
            session_lookup[key] = uid


def save_sessions():
    with open(SESSIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump({'sessions': upload_sessions}, f, ensure_ascii=False, indent=2)


load_sessions()

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


def validate_relative_path(rel_path):
    """Prevent path traversal attacks."""
    normalized = os.path.normpath(rel_path)
    if os.path.isabs(normalized) or normalized.startswith('..'):
        return None
    parts = Path(normalized).parts
    if any(p == '..' for p in parts):
        return None
    if '.md5' in parts:
        return None
    return normalized


def get_folder_by_id(folder_id):
    for f in config['folders']:
        if f['id'] == folder_id:
            return f
    return None


def save_md5_cache(folder_path, relative_path, md5_value):
    cache_path = os.path.join(folder_path, '.md5', relative_path)
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'w', encoding='utf-8') as f:
        f.write(md5_value)


def read_md5_cache(folder_path, relative_path):
    cache_path = os.path.join(folder_path, '.md5', relative_path)
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    return None

# ── Web UI ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

# ── Folder APIs ─────────────────────────────────────────────────────────────

@app.route('/api/folders', methods=['GET'])
def get_folders():
    folders = []
    for f in config['folders']:
        info = dict(f)
        info['exists'] = os.path.isdir(f['path'])
        info['scan_status'] = scan_status.get(f['id'], {'status': 'idle'})
        folders.append(info)
    return jsonify(folders)


@app.route('/api/folders', methods=['POST'])
def add_folder():
    data = request.get_json()
    path = data.get('path', '').strip()
    if not path:
        return jsonify({'error': '路径不能为空'}), 400
    if not os.path.isdir(path):
        return jsonify({'error': f'目录不存在: {path}'}), 400
    # Check duplicate
    for f in config['folders']:
        if os.path.normpath(f['path']) == os.path.normpath(path):
            return jsonify({'error': '该目录已添加'}), 400
    with config_lock:
        folder = {
            'id': config['next_id'],
            'path': os.path.normpath(path),
            'name': os.path.basename(path) or path
        }
        config['next_id'] += 1
        config['folders'].append(folder)
        save_config(config)
    return jsonify(folder), 201


@app.route('/api/folders/<int:folder_id>', methods=['DELETE'])
def remove_folder(folder_id):
    with config_lock:
        config['folders'] = [f for f in config['folders'] if f['id'] != folder_id]
        save_config(config)
    return jsonify({'ok': True})

# ── Scan APIs ───────────────────────────────────────────────────────────────

@app.route('/api/scan/<int:folder_id>', methods=['POST'])
def scan_folder_api(folder_id):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return jsonify({'error': '文件夹不存在'}), 404
    if scan_status.get(folder_id, {}).get('status') == 'scanning':
        return jsonify({'error': '正在扫描中'}), 409
    scan_status[folder_id] = {'status': 'scanning', 'scanned': 0, 'total': 0}
    t = threading.Thread(target=scan_folder_worker, args=(folder_id, folder['path']), daemon=True)
    t.start()
    return jsonify({'status': 'started'})


@app.route('/api/scan/<int:folder_id>/status', methods=['GET'])
def scan_status_api(folder_id):
    return jsonify(scan_status.get(folder_id, {'status': 'idle'}))


def scan_folder_worker(folder_id, folder_path):
    try:
        # Collect all files first
        all_files = []
        for root, dirs, files in os.walk(folder_path):
            # Skip .md5 directory
            dirs[:] = [d for d in dirs if d != '.md5']
            for fname in files:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, folder_path)
                all_files.append((full_path, rel_path))

        scan_status[folder_id] = {'status': 'scanning', 'scanned': 0, 'total': len(all_files)}

        for full_path, rel_path in all_files:
            try:
                md5 = compute_md5(full_path)
                save_md5_cache(folder_path, rel_path, md5)
            except Exception:
                pass  # Skip files that can't be read
            scan_status[folder_id]['scanned'] += 1

        scan_status[folder_id]['status'] = 'done'
    except Exception as e:
        scan_status[folder_id] = {'status': 'error', 'error': str(e)}

# ── File Check API ──────────────────────────────────────────────────────────

@app.route('/api/check', methods=['POST'])
def check_file():
    data = request.get_json()
    folder_id = data.get('folder_id')
    relative_path = data.get('relative_path', '')
    client_md5 = data.get('md5', '')

    folder = get_folder_by_id(folder_id)
    if not folder:
        return jsonify({'error': '文件夹不存在'}), 404

    rel = validate_relative_path(relative_path)
    if rel is None:
        return jsonify({'error': '无效路径'}), 400

    folder_path = folder['path']
    actual_file = os.path.join(folder_path, rel)

    # Check MD5 cache first
    cached_md5 = read_md5_cache(folder_path, rel)
    if cached_md5:
        if cached_md5 == client_md5:
            return jsonify({'status': 'exists'})
        else:
            return jsonify({'status': 'conflict'})

    # No cache, check physical file
    if os.path.exists(actual_file):
        try:
            actual_md5 = compute_md5(actual_file)
            save_md5_cache(folder_path, rel, actual_md5)
            if actual_md5 == client_md5:
                return jsonify({'status': 'exists'})
            else:
                return jsonify({'status': 'conflict'})
        except Exception:
            return jsonify({'status': 'conflict'})

    return jsonify({'status': 'new'})

# ── Upload APIs ─────────────────────────────────────────────────────────────

@app.route('/api/upload/init', methods=['POST'])
def upload_init():
    data = request.get_json()
    folder_id = data.get('folder_id')
    relative_path = data.get('relative_path', '')
    md5 = data.get('md5', '')
    size = data.get('size', 0)

    folder = get_folder_by_id(folder_id)
    if not folder:
        return jsonify({'error': '文件夹不存在'}), 404

    rel = validate_relative_path(relative_path)
    if rel is None:
        return jsonify({'error': '无效路径'}), 400

    total_chunks = max(1, (size + CHUNK_SIZE - 1) // CHUNK_SIZE)

    with session_lock:
        # Check for existing session (resume support)
        key = (folder_id, rel, md5)
        if key in session_lookup:
            uid = session_lookup[key]
            if uid in upload_sessions:
                session = upload_sessions[uid]
                return jsonify({
                    'upload_id': uid,
                    'chunk_size': CHUNK_SIZE,
                    'total_chunks': session['total_chunks'],
                    'received_chunks': session['received_chunks']
                })

        uid = uuid.uuid4().hex
        temp_dir = os.path.join(UPLOAD_TEMP_DIR, uid)
        os.makedirs(temp_dir, exist_ok=True)

        session = {
            'upload_id': uid,
            'folder_id': folder_id,
            'folder_path': folder['path'],
            'relative_path': rel,
            'md5': md5,
            'size': size,
            'chunk_size': CHUNK_SIZE,
            'total_chunks': total_chunks,
            'received_chunks': [],
            'temp_dir': temp_dir,
            'created_at': time.time()
        }
        upload_sessions[uid] = session
        session_lookup[key] = uid
        save_sessions()

    return jsonify({
        'upload_id': uid,
        'chunk_size': CHUNK_SIZE,
        'total_chunks': total_chunks,
        'received_chunks': []
    })


@app.route('/api/upload/chunk/<upload_id>/<int:chunk_index>', methods=['POST'])
def upload_chunk(upload_id, chunk_index):
    with session_lock:
        session = upload_sessions.get(upload_id)
        if not session:
            return jsonify({'error': '上传会话不存在'}), 404

    if chunk_index < 0 or chunk_index >= session['total_chunks']:
        return jsonify({'error': '无效的分片索引'}), 400

    chunk_data = request.get_data()
    if not chunk_data:
        return jsonify({'error': '空分片'}), 400

    chunk_path = os.path.join(session['temp_dir'], f'chunk_{chunk_index}')
    with open(chunk_path, 'wb') as f:
        f.write(chunk_data)

    with session_lock:
        if chunk_index not in session['received_chunks']:
            session['received_chunks'].append(chunk_index)
            session['received_chunks'].sort()
        save_sessions()

    return jsonify({'ok': True, 'received': len(session['received_chunks'])})


@app.route('/api/upload/complete/<upload_id>', methods=['POST'])
def upload_complete(upload_id):
    with session_lock:
        session = upload_sessions.get(upload_id)
        if not session:
            return jsonify({'error': '上传会话不存在'}), 404

    # Check all chunks received
    if len(session['received_chunks']) != session['total_chunks']:
        missing = set(range(session['total_chunks'])) - set(session['received_chunks'])
        return jsonify({'error': '缺少分片', 'missing_chunks': sorted(missing)}), 400

    # Assemble file
    target_path = os.path.join(session['folder_path'], session['relative_path'])
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    try:
        h = hashlib.md5()
        with open(target_path, 'wb') as out:
            for i in range(session['total_chunks']):
                chunk_path = os.path.join(session['temp_dir'], f'chunk_{i}')
                with open(chunk_path, 'rb') as cf:
                    data = cf.read()
                    h.update(data)
                    out.write(data)

        assembled_md5 = h.hexdigest()
        if assembled_md5 != session['md5']:
            os.remove(target_path)
            return jsonify({'error': 'MD5校验失败', 'expected': session['md5'], 'actual': assembled_md5}), 400

        # Update MD5 cache
        save_md5_cache(session['folder_path'], session['relative_path'], assembled_md5)

        # Clean up
        shutil.rmtree(session['temp_dir'], ignore_errors=True)
        with session_lock:
            key = (session['folder_id'], session['relative_path'], session['md5'])
            upload_sessions.pop(upload_id, None)
            session_lookup.pop(key, None)
            save_sessions()

        return jsonify({'ok': True, 'md5': assembled_md5})

    except Exception as e:
        return jsonify({'error': f'组装文件失败: {str(e)}'}), 500


@app.route('/api/upload/status/<upload_id>', methods=['GET'])
def upload_status_api(upload_id):
    with session_lock:
        session = upload_sessions.get(upload_id)
        if not session:
            return jsonify({'error': '上传会话不存在'}), 404
    return jsonify({
        'upload_id': upload_id,
        'total_chunks': session['total_chunks'],
        'received_chunks': session['received_chunks'],
        'size': session['size']
    })

# ── Files listing (for client to browse server folders) ─────────────────────

@app.route('/api/files/<int:folder_id>', methods=['GET'])
def list_files(folder_id):
    folder = get_folder_by_id(folder_id)
    if not folder:
        return jsonify({'error': '文件夹不存在'}), 404
    folder_path = folder['path']
    files = []
    try:
        for root, dirs, fnames in os.walk(folder_path):
            dirs[:] = [d for d in dirs if d != '.md5']
            for fname in fnames:
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, folder_path)
                try:
                    size = os.path.getsize(full)
                except OSError:
                    size = 0
                files.append({'path': rel, 'size': size})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify(files)


if __name__ == '__main__':
    os.makedirs(UPLOAD_TEMP_DIR, exist_ok=True)
    print("文件同步服务端启动: http://0.0.0.0:8080")
    app.run(host='0.0.0.0', port=8080, threaded=True)
