import asyncio
import json
import time
import base64
import os
import re
import sqlite3
import shutil
import subprocess
import urllib.request as urlrequest
import uuid
import zlib
import zipfile
from io import BytesIO
from urllib.parse import parse_qs, urlencode, urlparse
from websockets.legacy.client import connect
from xml.etree import ElementTree as ET

# 配置区域：支持多个 Bot 实例
BOT_CONFIGS = [
    {
        "name": "bot1",
        "url": "ws://127.0.0.1:3001",
        "token": "ghJqKVpnBCG51NL4"
    },
    # 示例：添加第二个 Bot
    # {
        # "name": "Bot2",
        # "url": "ws://127.0.0.1:2345",
        # "token": "X45645"
    # },
]

DATA_FILE = os.path.join(os.path.dirname(__file__), "fwlog_data.json")
DB_FILE = os.path.join(os.path.dirname(__file__), "fwlog.db")

WATCH_GROUPS = []

DOWNLOAD_TIMEOUT_SEC = 180
MAX_FILE_MB = 512
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024
RECENT_FILE_CAPTURE_TTL_SEC = 600
PAINTER_SERVERS = [
    "https://s02.trpgbot.com/s/",
    "https://s03.trpgbot.com/models/",
    "https://api.dice.center/dicelogger/",
]
KOKONA_BASE_URL = "https://dicelogger.s3-accelerate.amazonaws.com/"
URL_RE = re.compile(r"https?://[^\s\]\"']+")
ANGLE_SPEAKER_RE = re.compile(
    r"^\s*[【\[]?\s*<(?P<name>[^>\n]+)>\s*[:：]\s*(?P<content>.*?)[】\]]?\s*$"
)
PLAIN_SPEAKER_RE = re.compile(
    r"^\s*[【\[]?\s*(?P<name>[^:：<>\[\]【】\n][^:：<>\[\]【】\n]{0,79}?)\s*[:：]\s*(?P<content>.*?)[】\]]?\s*$"
)
recent_file_captures = {}

def log(*args):
    print("[fwlog-bot]", *args)

def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

# Database handling
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Table: groups (stores state per group)
    c.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY,
            current_log_name TEXT,
            recording INTEGER DEFAULT 0,
            created_at INTEGER,
            updated_at INTEGER
        )
    ''')
    
    # Table: logs (stores log metadata)
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT,
            name TEXT,
            ended INTEGER DEFAULT 0,
            created_at INTEGER,
            updated_at INTEGER,
            UNIQUE(group_id, name)
        )
    ''')
    
    # Table: items (stores log messages)
    c.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_id INTEGER,
            nickname TEXT,
            im_userid TEXT,
            time INTEGER,
            message TEXT,
            raw_msg_id TEXT,
            FOREIGN KEY(log_id) REFERENCES logs(id)
        )
    ''')
    
    conn.commit()
    conn.close()

def migrate_json_to_sqlite():
    if not os.path.exists(DATA_FILE):
        return
        
    log("正在从 JSON 迁移数据到 SQLite...")
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        conn = get_db_connection()
        c = conn.cursor()
        
        for group_id, g_data in data.items():
            # Insert group
            c.execute('''
                INSERT OR IGNORE INTO groups (group_id, current_log_name, recording, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                group_id,
                g_data.get("current", ""),
                1 if g_data.get("recording") else 0,
                g_data.get("createdAt", 0),
                g_data.get("updatedAt", 0)
            ))
            
            logs = g_data.get("logs", {})
            for log_name, log_data in logs.items():
                # Insert log
                c.execute('''
                    INSERT OR IGNORE INTO logs (group_id, name, ended, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    group_id,
                    log_name,
                    1 if log_data.get("ended") else 0,
                    log_data.get("createdAt", 0),
                    log_data.get("updatedAt", 0)
                ))
                
                # Get log_id
                c.execute('SELECT id FROM logs WHERE group_id = ? AND name = ?', (group_id, log_name))
                log_row = c.fetchone()
                if log_row:
                    log_id = log_row["id"]
                    items = log_data.get("items", [])
                    for item in items:
                        c.execute('''
                            INSERT INTO items (log_id, nickname, im_userid, time, message, raw_msg_id)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            log_id,
                            item.get("nickname", ""),
                            item.get("im_userid", ""),
                            item.get("time", 0),
                            item.get("message", ""),
                            item.get("raw_msg_id", "")
                        ))
        
        conn.commit()
        conn.close()
        
        # Rename old JSON file
        os.rename(DATA_FILE, DATA_FILE + ".bak")
        log("迁移完成，旧数据文件已重命名为 fwlog_data.json.bak")
        
    except Exception as e:
        log(f"迁移失败: {e}")

# Initial setup
init_db()
migrate_json_to_sqlite()

def pad2(n):
    return f"{n:02d}"

def format_time(ts):
    d = time.localtime(ts)
    y = d.tm_year
    m = pad2(d.tm_mon)
    day = pad2(d.tm_mday)
    hh = pad2(d.tm_hour)
    mm = pad2(d.tm_min)
    ss = pad2(d.tm_sec)
    return f"{y}/{m}/{day} {hh}:{mm}:{ss}"

def ensure_group_state(group_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM groups WHERE group_id = ?', (group_id,))
    row = c.fetchone()
    
    if not row:
        now = int(time.time() * 1000)
        c.execute('''
            INSERT INTO groups (group_id, current_log_name, recording, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?)
        ''', (group_id, "", now, now))
        conn.commit()
        c.execute('SELECT * FROM groups WHERE group_id = ?', (group_id,))
        row = c.fetchone()
    
    conn.close()
    return dict(row)

def update_group_state(group_id, **kwargs):
    conn = get_db_connection()
    c = conn.cursor()
    
    updates = []
    values = []
    for k, v in kwargs.items():
        updates.append(f"{k} = ?")
        values.append(v)
    
    values.append(group_id)
    sql = f"UPDATE groups SET {', '.join(updates)} WHERE group_id = ?"
    c.execute(sql, values)
    conn.commit()
    conn.close()

def ensure_log(group_id, name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM logs WHERE group_id = ? AND name = ?', (group_id, name))
    row = c.fetchone()
    
    if not row:
        now = int(time.time() * 1000)
        c.execute('''
            INSERT INTO logs (group_id, name, ended, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?)
        ''', (group_id, name, now, now))
        conn.commit()
        c.execute('SELECT * FROM logs WHERE group_id = ? AND name = ?', (group_id, name))
        row = c.fetchone()
    
    conn.close()
    return dict(row)

def update_log_meta(log_id, **kwargs):
    conn = get_db_connection()
    c = conn.cursor()
    
    updates = []
    values = []
    for k, v in kwargs.items():
        updates.append(f"{k} = ?")
        values.append(v)
    
    values.append(log_id)
    sql = f"UPDATE logs SET {', '.join(updates)} WHERE id = ?"
    c.execute(sql, values)
    conn.commit()
    conn.close()

def add_log_items(log_id, items):
    conn = get_db_connection()
    c = conn.cursor()
    
    # Get count before insert
    c.execute('SELECT COUNT(*) FROM items WHERE log_id = ?', (log_id,))
    old_count = c.fetchone()[0]
    
    for item in items:
        c.execute('''
            INSERT INTO items (log_id, nickname, im_userid, time, message, raw_msg_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            log_id,
            item.get("nickname", ""),
            item.get("im_userid", ""),
            item.get("time", 0),
            item.get("message", ""),
            item.get("raw_msg_id", "")
        ))
    
    # Update log updated_at
    now = int(time.time() * 1000)
    c.execute('UPDATE logs SET updated_at = ? WHERE id = ?', (now, log_id))
    
    conn.commit()
    conn.close()
    
    return old_count, old_count + len(items)

def clear_log_items(log_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('DELETE FROM items WHERE log_id = ?', (log_id,))
    conn.commit()
    conn.close()

def get_log_full(group_id, name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM logs WHERE group_id = ? AND name = ?', (group_id, name))
    log_row = c.fetchone()
    
    if not log_row:
        conn.close()
        return None
        
    log_data = dict(log_row)
    c.execute('SELECT * FROM items WHERE log_id = ? ORDER BY id', (log_data["id"],))
    items = [dict(row) for row in c.fetchall()]
    log_data["items"] = items
    
    conn.close()
    return log_data

def get_logs_list(group_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM logs WHERE group_id = ? ORDER BY created_at DESC', (group_id,))
    logs = [dict(row) for row in c.fetchall()]
    
    # Get item counts
    for l in logs:
        c.execute('SELECT COUNT(*) FROM items WHERE log_id = ?', (l["id"],))
        l["item_count"] = c.fetchone()[0]
        
    conn.close()
    return logs

def delete_log(group_id, name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT id FROM logs WHERE group_id = ? AND name = ?', (group_id, name))
    row = c.fetchone()
    if row:
        log_id = row["id"]
        c.execute('DELETE FROM items WHERE log_id = ?', (log_id,))
        c.execute('DELETE FROM logs WHERE id = ?', (log_id,))
        conn.commit()
    conn.close()

def extract_forward_ids_from_text(text):
    ids = []
    if not text:
        return ids
    # Standard CQ code
    prefix = "[CQ:forward"
    if prefix in text:
        idx = 0
        while True:
            start = text.find(prefix, idx)
            if start == -1:
                break
            end = text.find("]", start)
            if end == -1:
                break
            segment = text[start:end]
            
            # Try common keys for forward ID
            value = ""
            for key in ["id=", "res_id=", "message_id="]:
                pos = segment.find(key)
                if pos != -1:
                    pos += len(key)
                    j = pos
                    while j < len(segment) and segment[j] not in ",]":
                        j += 1
                    value = segment[pos:j]
                    if value:
                        break
            
            if value:
                ids.append(value)
            idx = end + 1
    return ids

def segments_to_text(message):
    if isinstance(message, str):
        return message
    if not isinstance(message, list):
        return str(message or "")
    parts = []
    for seg in message:
        if not isinstance(seg, dict):
            continue
        t = seg.get("type")
        d = seg.get("data") or {}
        if t == "text":
            parts.append(d.get("text", ""))
        elif t == "image":
            file_val = d.get("file", "")
            url_val = d.get("url") or d.get("file_url") or ""
            if file_val and url_val:
                parts.append(f"[CQ:image,file={file_val},url={url_val}]")
            elif file_val:
                parts.append(f"[CQ:image,file={file_val}]")
            elif url_val:
                parts.append(f"[CQ:image,url={url_val}]")
            else:
                parts.append("[图片]")
        elif t == "at":
            qq_val = d.get("qq", "")
            if qq_val:
                parts.append(f"[CQ:at,qq={qq_val}]")
        elif t == "forward":
            # Direct forward segment
            fid = d.get("id")
            if fid:
                parts.append(f"[CQ:forward,id={fid}]")
        else:
            parts.append(f"[{t}]")
    if not parts:
        return "[空消息]"
    return "".join(parts)

def safe_decode_bytes(data):
    if not data:
        return ""
    for encoding in ["utf-8", "utf-8-sig", "gb18030", "gbk", "big5", "utf-16", "latin1"]:
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")

def http_get_bytes(url, timeout=DOWNLOAD_TIMEOUT_SEC, headers=None):
    request_headers = {
        "User-Agent": "fwlog-bot/1.0",
        "Connection": "close",
    }
    if headers:
        request_headers.update(headers)

    req = urlrequest.Request(url=url, headers=request_headers, method="GET")
    total = 0
    chunks = []
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        while True:
            buf = resp.read(1024 * 256)
            if not buf:
                break
            total += len(buf)
            if total > MAX_FILE_BYTES:
                raise RuntimeError(f"文件过大，超过 {MAX_FILE_MB}MB 上限")
            chunks.append(buf)
    return b"".join(chunks)

def http_get_json(url, timeout=DOWNLOAD_TIMEOUT_SEC, headers=None):
    return json.loads(safe_decode_bytes(http_get_bytes(url, timeout=timeout, headers=headers)))

def cleanup_recent_file_captures(now=None):
    now = now or time.time()
    expired = [key for key, ttl in recent_file_captures.items() if ttl <= now]
    for key in expired:
        recent_file_captures.pop(key, None)

def remember_file_capture(session_id, log_name, file_key):
    if not file_key:
        return True
    now = time.time()
    cleanup_recent_file_captures(now)
    cache_key = (session_id, log_name, str(file_key))
    if recent_file_captures.get(cache_key, 0) > now:
        return False
    recent_file_captures[cache_key] = now + RECENT_FILE_CAPTURE_TTL_SEC
    return True

def get_event_target(event):
    msg_type = event.get("message_type")
    if msg_type == "group":
        return "group", str(event.get("group_id"))
    if msg_type == "private":
        return "private", str(event.get("user_id"))

    if (
        str(event.get("post_type") or "").lower() == "notice"
        and str(event.get("notice_type") or "").lower() == "group_upload"
        and event.get("group_id") is not None
    ):
        return "group", str(event.get("group_id"))

    return None, None

def get_event_sender(event):
    sender = event.get("sender") or {}
    user_id = str(sender.get("user_id") or event.get("user_id") or "")
    nickname = sender.get("card") or sender.get("nickname") or (f"QQ:{user_id}" if user_id else "Unknown")
    return nickname, user_id

def make_log_item(nickname, im_userid, ts, message, raw_msg_id):
    return {
        "nickname": nickname or "Unknown",
        "im_userid": str(im_userid or ""),
        "time": safe_int(ts, int(time.time())),
        "message": str(message or ""),
        "raw_msg_id": str(raw_msg_id or ""),
    }

def looks_like_speaker_name(name):
    candidate = str(name or "").strip()
    if not candidate or len(candidate) > 80:
        return False

    lowered = candidate.lower()
    if lowered.startswith("http") or "://" in candidate:
        return False
    if candidate.startswith("CQ:") or "/" in candidate or "\\" in candidate:
        return False
    if re.fullmatch(r"\d+", candidate):
        return False
    return True

def match_speaker_line(line):
    text = str(line or "").strip()
    if not text:
        return None

    match = ANGLE_SPEAKER_RE.match(text)
    if match:
        return match.group("name").strip(), match.group("content").strip()

    match = PLAIN_SPEAKER_RE.match(text)
    if not match:
        return None

    name = match.group("name").strip()
    if not looks_like_speaker_name(name):
        return None

    return name, match.group("content").strip()

def parse_structured_text_to_items(text, fallback_name, fallback_user_id, ts, raw_msg_id):
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip("\n")
    if not normalized.strip():
        return []

    parsed_items = []
    prefix_lines = []
    current_name = None
    current_lines = []
    structured_found = False
    item_index = 0
    base_raw_id = raw_msg_id or f"parsed-{safe_int(ts, int(time.time()))}"

    def flush_current():
        nonlocal item_index, current_name, current_lines
        if current_name is None:
            return
        content = "\n".join(current_lines).strip("\n")
        if content.strip():
            parsed_items.append(
                make_log_item(current_name, "", ts, content, f"{base_raw_id}#{item_index}")
            )
            item_index += 1
        current_name = None
        current_lines = []

    for line in normalized.split("\n"):
        matched = match_speaker_line(line)
        if matched:
            structured_found = True
            flush_current()
            current_name = matched[0]
            current_lines = []
            if matched[1]:
                current_lines.append(matched[1])
            continue

        if current_name is None:
            if line.strip():
                prefix_lines.append(line)
        else:
            current_lines.append(line)

    flush_current()

    if structured_found:
        prefix_text = "\n".join(prefix_lines).strip()
        if prefix_text:
            parsed_items.insert(
                0,
                make_log_item(fallback_name, fallback_user_id, ts, prefix_text, f"{base_raw_id}#preface"),
            )
        return parsed_items

    return [make_log_item(fallback_name, fallback_user_id, ts, normalized.strip(), base_raw_id)]

def parse_cq_params(segment_text):
    params = {}
    for part in str(segment_text or "").split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            params[key] = value
    return params

def extract_file_payloads(event):
    payloads = []
    post_type = str(event.get("post_type") or "").lower()
    notice_type = str(event.get("notice_type") or "").lower()

    if post_type == "notice" and notice_type == "group_upload":
        file_info = event.get("file") or {}
        file_id = file_info.get("id") or file_info.get("file_id")
        if file_id:
            payloads.append(
                {
                    "group_id": str(event.get("group_id") or ""),
                    "user_id": str(event.get("user_id") or ""),
                    "file_id": str(file_id),
                    "name": str(file_info.get("name") or file_info.get("file_name") or "未知文件"),
                    "busid": safe_int(file_info.get("busid"), 0),
                    "url": str(file_info.get("url") or ""),
                }
            )
        return payloads

    message = event.get("message")
    if isinstance(message, list):
        for seg in message:
            if not isinstance(seg, dict) or str(seg.get("type") or "").lower() != "file":
                continue
            data = seg.get("data") or {}
            file_id = data.get("id") or data.get("file_id")
            if not file_id:
                continue
            payloads.append(
                {
                    "group_id": str(event.get("group_id") or ""),
                    "user_id": str(event.get("user_id") or ""),
                    "file_id": str(file_id),
                    "name": str(data.get("name") or data.get("file") or "未知文件"),
                    "busid": safe_int(data.get("busid"), 0),
                    "url": str(data.get("url") or data.get("file_url") or ""),
                }
            )
        return payloads

    if isinstance(message, str) and "[CQ:file" in message:
        idx = 0
        while True:
            start = message.find("[CQ:file", idx)
            if start == -1:
                break
            end = message.find("]", start)
            if end == -1:
                break
            segment = message[start + 1:end]
            param_text = segment.split(",", 1)[1] if "," in segment else ""
            params = parse_cq_params(param_text)
            file_id = params.get("id") or params.get("file_id")
            if file_id:
                payloads.append(
                    {
                        "group_id": str(event.get("group_id") or ""),
                        "user_id": str(event.get("user_id") or ""),
                        "file_id": str(file_id),
                        "name": str(params.get("name") or params.get("file") or "未知文件"),
                        "busid": safe_int(params.get("busid"), 0),
                        "url": str(params.get("url") or params.get("file_url") or ""),
                    }
                )
            idx = end + 1

    return payloads

def decode_text_bytes(data):
    return safe_decode_bytes(data)

def extract_docx_text(data):
    with zipfile.ZipFile(BytesIO(data)) as zf:
        targets = []
        for name in zf.namelist():
            lowered = name.lower()
            if not lowered.startswith("word/"):
                continue
            if any(part in lowered for part in ("document.xml", "header", "footer", "footnotes", "endnotes", "comments")):
                targets.append(name)

        texts = []
        for path in targets:
            raw = zf.read(path)
            try:
                root = ET.fromstring(raw)
            except Exception:
                continue
            for node in root.iter():
                if node.text and node.text.strip():
                    texts.append(node.text.strip())
    return "\n".join(texts)

def _extract_pdf_text_with_python(data):
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(BytesIO(data))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        pass

    try:
        import PyPDF2  # type: ignore

        reader = PyPDF2.PdfReader(BytesIO(data))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        return None

def _extract_pdf_text_with_command(data):
    command = shutil.which("pdftotext")
    if not command:
        return None

    input_path = os.path.join(os.path.dirname(__file__), f"fwlog_{uuid.uuid4().hex}.pdf")
    output_path = os.path.join(os.path.dirname(__file__), f"fwlog_{uuid.uuid4().hex}.txt")
    try:
        with open(input_path, "wb") as fw:
            fw.write(data)
        subprocess.run([command, "-layout", input_path, output_path], check=True, timeout=DOWNLOAD_TIMEOUT_SEC)
        with open(output_path, "rb") as fr:
            return decode_text_bytes(fr.read())
    except Exception:
        return None
    finally:
        for path in (input_path, output_path):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

def extract_pdf_text(data):
    text = _extract_pdf_text_with_python(data)
    if text and text.strip():
        return text

    text = _extract_pdf_text_with_command(data)
    if text and text.strip():
        return text

    raise RuntimeError("PDF解析失败：请安装 pypdf/PyPDF2 或系统命令 pdftotext")

def extract_doc_text(data):
    for command_name in ("antiword", "catdoc"):
        command = shutil.which(command_name)
        if not command:
            continue

        input_path = os.path.join(os.path.dirname(__file__), f"fwlog_{uuid.uuid4().hex}.doc")
        try:
            with open(input_path, "wb") as fw:
                fw.write(data)
            proc = subprocess.run(
                [command, input_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=DOWNLOAD_TIMEOUT_SEC,
                check=False,
            )
            output = proc.stdout.decode("utf-8", errors="replace")
            if output.strip():
                return output
        except Exception:
            continue
        finally:
            try:
                if os.path.exists(input_path):
                    os.remove(input_path)
            except Exception:
                pass

    raise RuntimeError("DOC解析失败：请安装 antiword 或 catdoc")

def extract_text_from_file(filename, data):
    ext = os.path.splitext(str(filename or "").lower())[1]
    if ext in (".txt", ".log", ".json", ".csv", ".md", ".xml", ".yaml", ".yml"):
        return decode_text_bytes(data)
    if ext == ".docx":
        return extract_docx_text(data)
    if ext == ".pdf":
        return extract_pdf_text(data)
    if ext == ".doc":
        return extract_doc_text(data)
    return decode_text_bytes(data)

def fetch_weizaima(key, password=None):
    params = {"key": key}
    if password:
        params["password"] = password
    url = f"https://weizaima.com/dice/api/load_data?{urlencode(params)}"
    payload = http_get_json(url, timeout=30)
    compressed = payload.get("data")
    if not compressed:
        return None
    raw = zlib.decompress(base64.b64decode(compressed)).decode("utf-8")
    return json.loads(raw)

def format_weizaima_text(log_obj):
    if not log_obj:
        return ""
    items = log_obj.get("items", []) or ((log_obj.get("data") or {}).get("items") or [])
    lines = []
    for item in items:
        message = str(item.get("message") or "")
        if not message or "[CQ:image" in message:
            continue
        lines.append(f"{item.get('nickname', '?')}: {message}")
    return "\n".join(lines)

def fetch_trpgbot(full_id):
    sid, log_id = str(full_id).split("-", 1)
    base_url = PAINTER_SERVERS[int(sid)]
    headers = {"Referer": "https://logpainter.trpgbot.com/"}
    meta = http_get_json(
        f"{base_url}logReader.php?m=metaData&id={log_id}&r=0.1",
        timeout=20,
        headers=headers,
    )
    download_url = meta.get("redirectDownloadUrl") or f"{base_url}logReader.php?m=rawData&id={log_id}"
    return safe_decode_bytes(http_get_bytes(download_url, timeout=90, headers=headers))

def fetch_kokona(s3_key):
    return safe_decode_bytes(http_get_bytes(f"{KOKONA_BASE_URL}{s3_key}", timeout=60))

def fetch_raw_url(url):
    return safe_decode_bytes(http_get_bytes(url, timeout=120))

def infer_source_by_key(key):
    value = str(key or "")
    if value.startswith("http://") or value.startswith("https://"):
        return "raw_url"
    if "-" in value and value.split("-", 1)[0].isdigit():
        return "trpgbot"
    if "_" in value or len(value) > 20:
        return "kokona"
    return "weizaima"

def format_raw_text(raw_text):
    if not raw_text:
        return ""
    clean = []
    pattern = re.compile(r"<(.*?)>(.*)")
    for line in str(raw_text).split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        matched = pattern.search(stripped)
        if matched:
            clean.append(f"{matched.group(1)}: {matched.group(2).strip()}")
        else:
            clean.append(stripped)
    return "\n".join(clean)

def fetch_log_text_by_source(key, password=None, source=None):
    resolved_source = source or infer_source_by_key(key)
    if resolved_source == "kokona":
        return format_raw_text(fetch_kokona(key))
    if resolved_source == "trpgbot":
        return format_raw_text(fetch_trpgbot(key))
    if resolved_source == "raw_url":
        return format_raw_text(fetch_raw_url(key))
    return format_weizaima_text(fetch_weizaima(key, password))

def parse_log_target_entry(raw):
    value = str(raw or "").strip()
    if not value:
        return None

    url_match = URL_RE.search(value)
    if url_match:
        value = url_match.group(0)

    if "/bridge/content/" in value:
        return {"key": value, "source": "raw_url", "password": ""}

    parsed = urlparse(value)
    query = parse_qs(parsed.query)
    key = ""
    source = ""
    password = ""

    if query.get("s3"):
        key = query["s3"][0]
        source = "kokona"
    elif query.get("key"):
        key = query["key"][0]
        source = "weizaima"
        if parsed.fragment:
            password = parsed.fragment
    elif parsed.fragment:
        fragment_key = re.sub(r"[^a-zA-Z0-9-_]", "", parsed.fragment)
        if fragment_key:
            key = fragment_key
            if "-" in key:
                source = "trpgbot"
    else:
        key = value

    if not key:
        return None

    if not source:
        source = infer_source_by_key(key)

    return {"key": key, "source": source, "password": password}

async def extract_items_from_text_chunk(text, sender_name, sender_id, ts, raw_msg_id):
    text = str(text or "")
    if not text.strip():
        return []

    items = []
    plain_parts = []
    cursor = 0
    url_index = 0

    for match in URL_RE.finditer(text):
        plain_parts.append(text[cursor:match.start()])
        url = match.group(0)
        target = parse_log_target_entry(url)
        extracted_text = ""
        if target:
            try:
                extracted_text = await asyncio.to_thread(
                    fetch_log_text_by_source,
                    target["key"],
                    target.get("password", ""),
                    target.get("source"),
                )
            except Exception as e:
                log("日志链接提取失败", url, e)
                extracted_text = ""

        if extracted_text and extracted_text.strip():
            plain_text = "".join(plain_parts).strip()
            if plain_text:
                items.extend(
                    parse_structured_text_to_items(
                        plain_text,
                        sender_name,
                        sender_id,
                        ts,
                        f"{raw_msg_id}:text:{url_index}",
                    )
                )
            plain_parts = []
            items.extend(
                parse_structured_text_to_items(
                    extracted_text,
                    sender_name,
                    sender_id,
                    ts,
                    f"{raw_msg_id}:url:{url_index}",
                )
            )
            url_index += 1
        else:
            plain_parts.append(url)

        cursor = match.end()

    plain_parts.append(text[cursor:])
    plain_text = "".join(plain_parts).strip()
    if plain_text:
        items.extend(
            parse_structured_text_to_items(
                plain_text,
                sender_name,
                sender_id,
                ts,
                f"{raw_msg_id}:text:tail",
            )
        )

    return items

async def extract_items_from_forward(client, forward_id):
    response = await client.send_api("get_forward_msg", {"id": forward_id})
    data = response.get("data")
    if response.get("status") != "ok" or not data:
        response = await client.send_api("get_forward_msg", {"message_id": forward_id})
        data = response.get("data")
    if response.get("status") != "ok" or not data:
        return []

    if isinstance(data, dict) and "messages" in data:
        nodes = data["messages"]
    elif isinstance(data, list):
        nodes = data
    else:
        nodes = []

    items = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        sender = node.get("sender") or {}
        sender_id = str(sender.get("user_id") or "")
        sender_name = sender.get("nickname") or (f"QQ:{sender_id}" if sender_id else "Unknown")
        ts = node.get("time") or int(time.time())
        content = segments_to_text(node.get("message") or node.get("content") or "")
        if not str(content or "").strip():
            continue
        raw_id = str(node.get("message_id") or f"forward:{forward_id}:{index}")
        items.append(make_log_item(sender_name, sender_id, ts, content, raw_id))

    return items

async def resolve_group_file_url(client, group_id, file_id, busid):
    response = await client.send_api(
        "get_group_file_url",
        {
            "group_id": str(group_id),
            "file_id": str(file_id),
            "busid": safe_int(busid, 0),
        },
    )
    data = response.get("data") or {}
    return str(data.get("url") or "")

async def extract_items_from_file_payload(client, payload, sender_name, sender_id, ts, raw_msg_id):
    file_url = str(payload.get("url") or "")
    if not file_url:
        group_id = payload.get("group_id")
        file_id = payload.get("file_id")
        if group_id and file_id:
            file_url = await resolve_group_file_url(client, group_id, file_id, payload.get("busid", 0))

    if not file_url:
        raise RuntimeError("未获取到文件下载地址")

    file_bytes = await asyncio.to_thread(http_get_bytes, file_url)
    extracted_text = await asyncio.to_thread(extract_text_from_file, payload.get("name") or "未知文件", file_bytes)
    return parse_structured_text_to_items(extracted_text, sender_name, sender_id, ts, raw_msg_id)

async def handle_recording_event(client, event):
    reply_type, session_id = get_event_target(event)
    if not session_id:
        return

    if WATCH_GROUPS and session_id not in WATCH_GROUPS:
        return

    group_state = ensure_group_state(session_id)
    current_log_name = group_state.get("current_log_name") or ""
    if not group_state.get("recording") or not current_log_name:
        return

    log_obj = ensure_log(session_id, current_log_name)
    sender_name, sender_id = get_event_sender(event)
    event_ts = safe_int(event.get("time"), int(time.time()))
    items = []

    post_type = str(event.get("post_type") or "").lower()
    notice_type = str(event.get("notice_type") or "").lower()

    if post_type == "notice" and notice_type == "group_upload":
        for index, payload in enumerate(extract_file_payloads(event)):
            file_key = payload.get("file_id") or payload.get("name") or f"notice-{index}"
            if not remember_file_capture(session_id, current_log_name, file_key):
                continue
            try:
                items.extend(
                    await extract_items_from_file_payload(
                        client,
                        payload,
                        sender_name,
                        sender_id,
                        event_ts,
                        f"file:{file_key}",
                    )
                )
            except Exception as e:
                log(f"[{client.name}] 文档提取失败", payload.get("name"), e)
    else:
        message = event.get("message")
        message_id = str(event.get("message_id") or f"event:{event_ts}")
        if isinstance(message, list):
            for index, seg in enumerate(message):
                if not isinstance(seg, dict):
                    continue
                seg_type = str(seg.get("type") or "").lower()
                data = seg.get("data") or {}
                if seg_type == "text":
                    items.extend(
                        await extract_items_from_text_chunk(
                            data.get("text") or "",
                            sender_name,
                            sender_id,
                            event_ts,
                            f"{message_id}:text:{index}",
                        )
                    )
                elif seg_type == "forward":
                    forward_id = data.get("id") or data.get("res_id") or data.get("message_id")
                    if not forward_id:
                        continue
                    try:
                        items.extend(await extract_items_from_forward(client, str(forward_id)))
                    except Exception as e:
                        log(f"[{client.name}] 获取转发消息异常", forward_id, e)
                elif seg_type == "file":
                    payloads = extract_file_payloads({
                        "post_type": post_type,
                        "message": [seg],
                        "group_id": event.get("group_id"),
                        "user_id": event.get("user_id"),
                    })
                    for payload in payloads:
                        file_key = payload.get("file_id") or payload.get("name") or f"message-{index}"
                        if not remember_file_capture(session_id, current_log_name, file_key):
                            continue
                        try:
                            items.extend(
                                await extract_items_from_file_payload(
                                    client,
                                    payload,
                                    sender_name,
                                    sender_id,
                                    event_ts,
                                    f"{message_id}:file:{file_key}",
                                )
                            )
                        except Exception as e:
                            log(f"[{client.name}] 文档提取失败", payload.get("name"), e)
        else:
            text = segments_to_text(message).strip()
            if text:
                forward_ids = extract_forward_ids_from_text(text)
                for index, forward_id in enumerate(forward_ids):
                    try:
                        items.extend(await extract_items_from_forward(client, str(forward_id)))
                    except Exception as e:
                        log(f"[{client.name}] 获取转发消息异常", forward_id, e)

                for payload in extract_file_payloads(event):
                    file_key = payload.get("file_id") or payload.get("name")
                    if not remember_file_capture(session_id, current_log_name, file_key):
                        continue
                    try:
                        items.extend(
                            await extract_items_from_file_payload(
                                client,
                                payload,
                                sender_name,
                                sender_id,
                                event_ts,
                                f"{message_id}:file:{file_key}",
                            )
                        )
                    except Exception as e:
                        log(f"[{client.name}] 文档提取失败", payload.get("name"), e)

                cleaned_text = re.sub(r"\[CQ:(?:forward|file)[^\]]*\]", "", text).strip()
                if cleaned_text:
                    items.extend(
                        await extract_items_from_text_chunk(
                            cleaned_text,
                            sender_name,
                            sender_id,
                            event_ts,
                            f"{message_id}:text",
                        )
                    )

    if not items:
        return

    old_count, new_count = add_log_items(log_obj["id"], items)
    log(f"[{client.name}] 已追加 {len(items)} 条 fwlog 内容 (当前共 {new_count} 条)")

    if reply_type and new_count // 1000 > old_count // 1000:
        await client.send_msg(
            reply_type,
            session_id,
            f"【系统提醒】 当前日志 {log_obj['name']} 已记录 {new_count} 条消息。\n"
            "如果记录完毕，请记得发送 .fwlog end 结束记录。",
        )

next_echo_id = 1
message_queue = asyncio.Queue()

def gen_echo():
    global next_echo_id
    echo = f"fwlog-{next_echo_id}"
    next_echo_id += 1
    return echo

class BotClient:
    def __init__(self, config):
        self.name = config.get("name", "UnknownBot")
        self.url = config.get("url")
        self.token = config.get("token")
        self.ws_conn = None
        self.pending = {}
        
    async def send_api(self, action, params=None):
        if params is None:
            params = {}
        if self.ws_conn is None or self.ws_conn.closed:
            raise RuntimeError(f"[{self.name}] WebSocket 未连接")
        echo = gen_echo()
        fut = asyncio.get_running_loop().create_future()
        self.pending[echo] = fut
        payload = {"action": action, "params": params, "echo": echo}
        if self.token:
            payload["token"] = self.token
        
        try:
            await self.ws_conn.send(json.dumps(payload, ensure_ascii=False))
            # Wait for response with timeout
            return await asyncio.wait_for(fut, timeout=20.0)
        except asyncio.TimeoutError:
            self.pending.pop(echo, None)
            raise RuntimeError(f"[{self.name}] API请求超时: {action}")
        except Exception as e:
            self.pending.pop(echo, None)
            raise e

    def handle_api_response(self, msg):
        echo = msg.get("echo")
        if not echo:
            return
        fut = self.pending.pop(echo, None)
        if fut is None:
            return
        if not fut.done():
            fut.set_result(msg)

    async def send_group_msg(self, group_id, text):
        try:
            # Use string group_id for better compatibility with NapCat/OneBot
            await self.send_api(
                "send_group_msg",
                {"group_id": str(group_id), "message": text},
            )
        except Exception as e:
            log(f"[{self.name}] 发送群消息失败", e)

    async def send_private_msg(self, user_id, text):
        try:
            await self.send_api(
                "send_private_msg",
                {"user_id": str(user_id), "message": text},
            )
        except Exception as e:
            log(f"[{self.name}] 发送私聊消息失败", e)

    async def send_msg(self, msg_type, target_id, text):
        if msg_type == "group":
            await self.send_group_msg(target_id, text)
        elif msg_type == "private":
            await self.send_private_msg(target_id, text)

    async def run(self):
        while True:
            try:
                log(f"[{self.name}] 尝试连接到 NapCat WS:", self.url)
                async with connect(
                    self.url,
                    extra_headers=(
                        {"Authorization": f"Bearer {self.token}"}
                        if self.token
                        else None
                    ),
                ) as ws:
                    self.ws_conn = ws
                    log(f"[{self.name}] WS 已连接")
                    async for message in ws:
                        try:
                            data = json.loads(message)
                        except Exception:
                            continue
                        
                        # If it's an API response (echo), handle immediately
                        if isinstance(data, dict) and "echo" in data:
                            self.handle_api_response(data)
                            continue
                            
                        # Otherwise, queue it with client reference
                        if isinstance(data, dict):
                            post_type = str(data.get("post_type") or "").lower()
                            notice_type = str(data.get("notice_type") or "").lower()
                            if post_type == "message" and data.get("message_type") in ["group", "private"]:
                                message_queue.put_nowait((self, data))
                            elif post_type == "notice" and notice_type == "group_upload":
                                message_queue.put_nowait((self, data))
                        
            except Exception as e:
                log(f"[{self.name}] WS 连接出错或关闭", e)
            self.ws_conn = None
            await asyncio.sleep(3)

def generate_log_text(log_obj):
    items = log_obj.get("items", [])
    blocks = []
    for item in items:
        ts = item.get("time", 0)
        dt = format_time(ts)
        name = item.get("nickname", "Unknown")
        uid = item.get("im_userid", "")
        msg = item.get("message", "")
        
        # Header: Name(ID) Time
        header = f"{name}({uid}) {dt}"
        
        # Content: Add leading space to each line
        if msg is None:
            msg = ""
        msg = str(msg)
        content_lines = [f" {line}" for line in msg.splitlines()]
        content_text = "\n".join(content_lines)
        
        # Block: Header + Newline + Content
        blocks.append(f"{header}\n{content_text}")
        
    # Join blocks with an empty line in between
    return "\n\n".join(blocks)

def normalize_fwlog_prefix(text):
    if not text:
        return ""
    # Remove leading whitespace/invisible chars
    t = text.lstrip()
    if not t:
        return ""
    
    # Check for common prefixes
    prefixes = [".", "。", "/", "、"]
    has_prefix = False
    for p in prefixes:
        if t.startswith(p):
            t = t[len(p):].lstrip()
            has_prefix = True
            break
            
    # Check if starts with fwlog (case insensitive)
    if t.lower().startswith("fwlog"):
        # Only allow if prefix was present (User request: strict prefix requirement)
        if has_prefix:
            return ".fwlog" + t[5:]
    
    return text

async def handle_fwlog_command(client, event, text_override=None):
    msg_type = event.get("message_type")
    if msg_type == "group":
        session_id = str(event.get("group_id"))
    elif msg_type == "private":
        session_id = str(event.get("user_id"))
    else:
        return

    sender = event.get("sender") or {}
    user_name = sender.get("card") or sender.get("nickname") or ""
    
    if text_override is not None:
        msg_text = text_override
    else:
        msg_text = segments_to_text(event.get("message")).strip()
    
    # Normalize command
    normalized_text = normalize_fwlog_prefix(msg_text)
    
    if not normalized_text.startswith(".fwlog"):
        return

    body = normalized_text[len(".fwlog") :]
    body = body.replace("\u3000", " ")
    body = body.strip()
    
    if not body:
        sub = "help"
        name_arg = ""
    else:
        parts = body.split()
        sub = parts[0].lower()
        name_arg = parts[1] if len(parts) > 1 else ""

    log(f"[{client.name}] fwlog 子命令解析 ({msg_type}:{session_id}):", msg_text, "=>", sub, name_arg)
    g = ensure_group_state(session_id)

    try:
        if sub == "new":
            name = name_arg
            if not name:
                now = time.localtime()
                name = (
                    "log-"
                    f"{now.tm_year}{pad2(now.tm_mon)}{pad2(now.tm_mday)}-"
                    f"{pad2(now.tm_hour)}{pad2(now.tm_min)}{pad2(now.tm_sec)}"
                )
            
            # Check if log exists, if so clear it (or just use new name)
            # ensure_log creates it if not exists
            log_obj = ensure_log(session_id, name)
            clear_log_items(log_obj["id"])
            
            now_ts = int(time.time() * 1000)
            update_log_meta(log_obj["id"], ended=0, created_at=now_ts, updated_at=now_ts)
            update_group_state(session_id, current_log_name=name, recording=1)

            await client.send_msg(
                msg_type, session_id,
                f"【新建日志】 {user_name} 已新建日志: {name}\n"
                "------------------------------\n"
                "* 记录已开启！请发送【合并转发 / 日志链接 / 文档 / 零碎文字】以提取内容。\n"
                "// 说明：本工具会将以上内容转化为海豹原始格式，用于补充缺失日志。\n"
                "// 正常跑团请直接使用 .log 指令。",
            )
        elif sub == "on":
            name = name_arg or g["current_log_name"]
            if not name:
                await client.send_msg(
                    msg_type, session_id,
                    "当前没有选中的日志，请先使用 .fwlog new <名称> 创建",
                )
                return
            
            log_obj = get_log_full(session_id, name)
            if not log_obj:
                await client.send_msg(msg_type, session_id, f"指定日志不存在: {name}")
                return
                
            now_ts = int(time.time() * 1000)
            update_log_meta(log_obj["id"], ended=0, updated_at=now_ts)
            update_group_state(session_id, current_log_name=name, recording=1)

            await client.send_msg(
                msg_type, session_id,
                f"【继续记录】 {user_name} 已继续记录合并转发日志: {name}\n"
                "请发送【合并转发 / 日志链接 / 文档 / 零碎文字】以提取内容。",
            )
        elif sub == "off":
            if not g["recording"]:
                await client.send_msg(msg_type, session_id, "当前不在记录状态")
            else:
                update_group_state(session_id, recording=0)
                await client.send_msg(msg_type, session_id, "【暂停记录】 已暂停记录当前合并转发日志")
        elif sub == "end":
            name = name_arg or g["current_log_name"]
            log_obj = get_log_full(session_id, name)
            
            if not log_obj:
                await client.send_msg(msg_type, session_id, "指定日志不存在")
                return
            
            if not log_obj.get("items"):
                await client.send_msg(msg_type, session_id, f"指定日志为空: {name}")
                return
            try:
                full_text = generate_log_text(log_obj)
                # Encode to base64
                b64_content = base64.b64encode(full_text.encode("utf-8")).decode("utf-8")
                file_param = f"base64://{b64_content}"
                
                try:
                    # Try upload_file API first (Standard OneBot for files)
                    if msg_type == "group":
                        await client.upload_group_file(session_id, file_param, f"{name}.txt")
                    else:
                        await client.upload_private_file(session_id, file_param, f"{name}.txt")
                    
                    # Update state only if successful
                    now_ts = int(time.time() * 1000)
                    update_log_meta(log_obj["id"], ended=1, updated_at=now_ts)
                    update_group_state(session_id, recording=0)
                    
                    await client.send_msg(msg_type, session_id, "【发送成功】 日志文件已发送")
                
                except Exception as upload_err:
                    log(f"[{client.name}] upload_file 失败，尝试 CQ 码发送: {upload_err}")
                    # Fallback: Send as file using CQ code
                    file_cq = f"[CQ:file,file={file_param},name={name}.txt]"
                    await client.send_msg(msg_type, session_id, file_cq)
                    
                    now_ts = int(time.time() * 1000)
                    update_log_meta(log_obj["id"], ended=1, updated_at=now_ts)
                    update_group_state(session_id, recording=0)
                    
                    await client.send_msg(msg_type, session_id, "【发送成功】 日志文件已发送 (CQ码模式)")

            except Exception as e:
                await client.send_msg(msg_type, session_id, f"【发送失败】 发送日志文件失败: {e}")
        elif sub == "get":
            name = name_arg or g["current_log_name"]
            log_obj = get_log_full(session_id, name)
            
            if not log_obj:
                await client.send_msg(msg_type, session_id, "指定日志不存在")
                return
            if not log_obj.get("items"):
                await client.send_msg(msg_type, session_id, f"指定日志为空: {name}")
                return
            try:
                full_text = generate_log_text(log_obj)
                # Encode to base64
                b64_content = base64.b64encode(full_text.encode("utf-8")).decode("utf-8")
                file_param = f"base64://{b64_content}"
                
                try:
                    # Try upload_file API first
                    if msg_type == "group":
                        await client.upload_group_file(session_id, file_param, f"{name}.txt")
                    else:
                        await client.upload_private_file(session_id, file_param, f"{name}.txt")
                except Exception as upload_err:
                    log(f"[{client.name}] upload_file 失败，尝试 CQ 码发送: {upload_err}")
                    # Fallback: Send as file using CQ code
                    file_cq = f"[CQ:file,file={file_param},name={name}.txt]"
                    await client.send_msg(msg_type, session_id, file_cq)

            except Exception as e:
                await client.send_msg(msg_type, session_id, f"【发送失败】 发送日志文件失败: {e}")
        elif sub == "list":
            logs = get_logs_list(session_id)
            if not logs:
                await client.send_msg(msg_type, session_id, "当前会话没有任何 fwlog 日志")
                return
            lines = ["【日志列表】 本会话 fwlog 列表:"]
            for l in logs:
                name = l["name"]
                is_current = (g["current_log_name"] == name and g["recording"])
                
                if is_current:
                    status = "* [记录中]"
                elif l["ended"]:
                    status = "  [已结束]"
                else:
                    status = "  [已暂停]"
                
                count = l.get("item_count", 0)
                t = time.localtime((l["created_at"] or 0) / 1000)
                time_str = (
                    f"{t.tm_year}-{pad2(t.tm_mon)}-{pad2(t.tm_mday)} "
                    f"{pad2(t.tm_hour)}:{pad2(t.tm_min)}"
                )
                lines.append(f"- {status} {name} ({count}条, 创建于 {time_str})")
            await client.send_msg(msg_type, session_id, "\n".join(lines))
        elif sub == "clear":
            name = name_arg or g["current_log_name"]
            log_obj = get_log_full(session_id, name)
            if not log_obj:
                await client.send_msg(msg_type, session_id, "指定日志不存在")
                return
                
            delete_log(session_id, name)
            
            if g["current_log_name"] == name:
                update_group_state(session_id, current_log_name="", recording=0)
                
            await client.send_msg(msg_type, session_id, f"【清除成功】 日志 {name} 已清除")
        else:
            help_lines = [
                "【fwlog 聊天记录转海豹日志工具】",
                "// 说明：本工具专用于将【合并转发】消息转换为海豹(SealDice)原生日志格式，以便在日志缺失时进行补充。",
                "// 注意：仅解析合并转发内容，不记录实时消息。",
                "// 正常跑团请使用 .log 指令。",
                "",
                "【指令列表】",
                ".fwlog new [名称]   // 新建并开始记录",
                ".fwlog on [名称]    // 继续记录已有日志",
                ".fwlog off          // 暂停当前日志记录",
                ".fwlog end [名称]   // 结束并发送日志文件",
                ".fwlog get [名称]   // 获取指定日志文件",
                ".fwlog list         // 列出当前会话日志",
                ".fwlog clear [名称] // 清除指定日志",
            ]
            await client.send_msg(msg_type, session_id, "\n".join(help_lines))
    except Exception as e:
        log(f"执行 fwlog {sub} 时出错: {e}")
        # Optionally notify group
        # await client.send_group_msg(group_id, f"执行指令出错: {e}")

async def process_messages():
    """Consume messages from the queue asynchronously"""
    log("消息处理循环已启动")
    while True:
        # Unpack tuple (client, message)
        item = await message_queue.get()
        if not isinstance(item, tuple) or len(item) != 2:
            message_queue.task_done()
            continue
            
        client, msg = item
        try:
            text = segments_to_text(msg.get("message")).strip()
            
            # Handle @ mention
            self_id = str(msg.get("self_id", ""))
            if self_id:
                cq_at = f"[CQ:at,qq={self_id}]"
                if text.startswith(cq_at):
                    text = text[len(cq_at):].strip()

            normalized = normalize_fwlog_prefix(text)
            
            if normalized.startswith(".fwlog"):
                log(f"[{client.name}] 检测到 fwlog 指令:", text)
                await handle_fwlog_command(client, msg, text_override=text)
            else:
                await handle_recording_event(client, msg)
        except Exception as e:
            log(f"处理消息时发生错误: {e}")
        finally:
            message_queue.task_done()

async def main_loop():
    # Create clients
    clients = [BotClient(cfg) for cfg in BOT_CONFIGS]
    
    # Start processor
    processor_task = asyncio.create_task(process_messages())
    
    # Start all clients
    tasks = [client.run() for client in clients]
    await asyncio.gather(*tasks, processor_task)

def main():
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        log("程序已停止")

if __name__ == "__main__":
    main()
