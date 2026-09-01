"""
Facebook Messenger Bot — Flask Web App
Run: pip install flask paho-mqtt  →  python main.py
"""

import os, json, uuid, time, random, threading, hashlib
from datetime import datetime
from flask import Flask, request, jsonify, send_file, abort

import paho.mqtt.client as mqtt_lib

app = Flask(__name__)
PERSISTENCE_FILE = "sessions.json"

processes: dict = {}      # stop_key → process dict
proc_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════════
# COOKIE HELPERS
# ══════════════════════════════════════════════════════════════════

def parse_one_cookie_string(raw: str):
    """
    Parse one cookie string (JSON array OR raw 'k=v; k=v;' format).
    Returns (user_id, cookie_header_str) or (None, None) on error.
    """
    t = raw.strip()
    if not t:
        return None, None
    try:
        if t.startswith("["):
            arr = json.loads(t)
            lst = []
            for c in arr:
                k = c.get("key") or c.get("name")
                v = c.get("value")
                if k and v:
                    lst.append({"key": k, "value": v})
        else:
            lst = []
            for part in t.split(";"):
                part = part.strip()
                if "=" in part:
                    k, v = part.split("=", 1)
                    k = k.strip(); v = v.strip()
                    if k and v:
                        lst.append({"key": k, "value": v})

        user_id = next((c["value"] for c in lst if c["key"] == "c_user"), None)
        cookie_str = "; ".join(f"{c['key']}={c['value']}" for c in lst)
        return user_id, cookie_str
    except Exception:
        return None, None


# ══════════════════════════════════════════════════════════════════
# MQTT HELPERS
# ══════════════════════════════════════════════════════════════════

def build_mqtt_client(user_id: str, cookie_str: str):
    """
    Creates a paho-mqtt WebSocket client connected to Facebook edge-chat.
    Returns (client, error_str).  Blocks up to 15s for /t_ms ready signal.
    """
    session_id = random.randint(1, 9007199254740991)
    client_id  = str(uuid.uuid4())

    username_obj = {
        "u": user_id, "s": session_id, "chat_on": True, "fg": True,
        "d": client_id, "ct": "websocket", "aid": "219994525426954",
        "mqtt_sid": "", "cp": 3, "ecp": 10, "st": [], "pm": [], "dc": "",
        "no_auto_fg": True, "gas": None, "pack": []
    }

    headers = {
        "Cookie":     cookie_str,
        "Origin":     "https://www.facebook.com",
        "User-Agent": ("Mozilla/5.0 (Linux; Android 10; Mobile) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Mobile Safari/537.36"),
        "Referer":    "https://www.facebook.com/",
        "Host":       "edge-chat.facebook.com",
    }

    client = mqtt_lib.Client(
        client_id=f"mqttwsclient_{uuid.uuid4().hex[:10]}",
        transport="websockets",
        protocol=mqtt_lib.MQTTv31,
    )
    client.ws_set_options(
        path=f"/chat?sid={session_id}&cid={client_id}",
        headers=headers,
    )
    client.username_pw_set(json.dumps(username_obj))
    client.tls_set()   # wss://

    ready_event  = threading.Event()
    ready_called = [False]
    connect_rc   = [None]

    def _on_connect(c, _ud, _flags, rc):
        connect_rc[0] = rc
        if rc != 0:
            ready_called[0] = True
            ready_event.set()
            return

        topics = [
            "/t_ms", "/thread_typing", "/orca_typing_notifications",
            "/notify_disconnect", "/orca_presence", "/inbox", "/mercury",
            "/messaging_events", "/orca_message_notifications",
        ]
        for tp in topics:
            c.subscribe(tp)

        queue = {
            "sync_api_version": 11, "max_deltas_able_to_process": 100,
            "delta_batch_size": 500, "encoding": "JSON",
            "entity_fbid": user_id,
            "initial_titan_sequence_id": "0", "device_params": None,
        }
        c.publish("/messenger_sync_create_queue", json.dumps(queue), qos=1)
        c.publish("/foreground_state", json.dumps({"foreground": True}), qos=1)

        def _tms_timeout():
            if not ready_called[0]:
                ready_called[0] = True
                ready_event.set()
        threading.Timer(4.0, _tms_timeout).start()

    def _on_message(c, _ud, msg):
        if msg.topic == "/t_ms" and not ready_called[0]:
            ready_called[0] = True
            ready_event.set()

    client.on_connect = _on_connect
    client.on_message = _on_message

    try:
        client.connect_async("edge-chat.facebook.com", 443, keepalive=10)
        client.loop_start()
    except Exception as e:
        return None, str(e)

    ready_event.wait(timeout=15)

    if connect_rc[0] not in (None, 0):
        client.loop_stop()
        return None, f"MQTT connect refused: rc={connect_rc[0]}"

    return client, None


def send_mqtt_message(client, thread_id: str, text: str) -> bool:
    """Publish one Lightspeed /ls_req message.  Waits for QoS-1 ACK (up to 10s)."""
    ts   = int(time.time() * 1000)
    otid = str(random.randint(10**14, 10**15 - 1))

    form = json.dumps({
        "app_id": "2220391788200892",
        "payload": json.dumps({
            "tasks": [
                {
                    "label": "46",
                    "payload": json.dumps({
                        "thread_id": str(thread_id),
                        "otid": otid, "source": 0, "send_type": 1,
                        "sync_group": 1, "text": text,
                        "initiating_source": 1, "skip_url_preview_gen": 0,
                    }),
                    "queue_name": str(thread_id), "task_id": 0, "failure_count": None,
                },
                {
                    "label": "21",
                    "payload": json.dumps({
                        "thread_id": str(thread_id),
                        "last_read_watermark_ts": ts, "sync_group": 1,
                    }),
                    "queue_name": str(thread_id), "task_id": 1, "failure_count": None,
                },
            ],
            "epoch_id": ts * 4194304 + random.randint(0, 4194303),
            "version_id": "6120284488008082",
            "data_trace_id": None,
        }),
        "request_id": random.randint(1, 65535),
        "type": 3,
    })

    ack_event = threading.Event()
    ack_ok    = [False]

    def _on_pub(_c, _ud, _mid):
        ack_ok[0] = True
        ack_event.set()

    old_pub = client.on_publish
    client.on_publish = _on_pub
    client.publish("/ls_req", form, qos=1)
    ack_event.wait(timeout=10)
    client.on_publish = old_pub
    return ack_ok[0]


# ══════════════════════════════════════════════════════════════════
# PERSISTENCE
# ══════════════════════════════════════════════════════════════════

def _serialisable(proc: dict) -> dict:
    """Strip non-serialisable keys before saving."""
    return {
        "stop_key":       proc["stop_key"],
        "cookies_raw":    proc["cookies_raw"],
        "thread_id":      proc["thread_id"],
        "messages":       proc["messages"],
        "speed":          proc["speed"],
        "hater_name":     proc["hater_name"],
        "task_name":      proc.get("task_name", "Unnamed Task"),
        "created_date":   proc.get("created_date", ""),
        "multi_cookie":   proc.get("multi_cookie", False),
        "current_index":  proc.get("current_index", 0),
        "sent_count":     proc.get("sent_count", 0),
        "failed_count":   proc.get("failed_count", 0),
        "total_cookies":  proc.get("total_cookies", 0),
        "start_time":     proc.get("start_time", 0),
        "running":        proc.get("running", True),
        "status":         proc.get("status", "unknown"),
        "last_message":   proc.get("last_message", ""),
    }


def save_state():
    with proc_lock:
        data = {k: _serialisable(v) for k, v in processes.items()}
    with open(PERSISTENCE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_state() -> dict:
    if not os.path.exists(PERSISTENCE_FILE):
        return {}
    try:
        with open(PERSISTENCE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════
# WORKER THREAD — runs one user's send loop
# ══════════════════════════════════════════════════════════════════

def _worker(stop_key: str):
    with proc_lock:
        proc = processes.get(stop_key)
    if not proc:
        return

    cookies_raw  = proc["cookies_raw"]
    thread_id    = proc["thread_id"]
    messages     = proc["messages"]
    speed        = float(proc["speed"])
    hater_name   = proc.get("hater_name", "")
    multi_cookie = proc.get("multi_cookie", False)

    # ── parse cookies ──
    if multi_cookie:
        lines = [l.strip() for l in cookies_raw.split("\n") if l.strip()]
    else:
        lines = [cookies_raw.strip()]

    accounts = []  # list of (user_id, cookie_str)
    for line in lines:
        uid, cstr = parse_one_cookie_string(line)
        if uid and cstr:
            accounts.append((uid, cstr))

    if not accounts:
        with proc_lock:
            proc = processes.get(stop_key)
            if proc:
                proc["running"] = False
                proc["status"]  = "error: no valid cookies"
        save_state()
        return

    # ── store total cookies count ──
    with proc_lock:
        proc = processes.get(stop_key)
        if proc:
            proc["total_cookies"] = len(accounts)

    # ── connect MQTT clients ──
    with proc_lock:
        proc = processes.get(stop_key)
        if proc:
            proc["status"] = "connecting"

    mqtt_clients = []
    for uid, cstr in accounts:
        with proc_lock:
            p = processes.get(stop_key)
            if not p or not p.get("running"):
                break
        client, err = build_mqtt_client(uid, cstr)
        if client:
            mqtt_clients.append(client)

    if not mqtt_clients:
        with proc_lock:
            proc = processes.get(stop_key)
            if proc:
                proc["running"] = False
                proc["status"]  = "error: MQTT connection failed"
        save_state()
        return

    with proc_lock:
        proc = processes.get(stop_key)
        if proc:
            proc["status"]       = "running"
            proc["mqtt_clients"] = mqtt_clients   # not persisted, runtime only

    cli_idx = 0

    # ── send loop ──
    while True:
        with proc_lock:
            p = processes.get(stop_key)
            if not p or not p.get("running"):
                break
            msg_idx = p.get("current_index", 0)

        text      = messages[msg_idx % len(messages)]
        full_text = f"{hater_name} {text}" if hater_name else text
        client    = mqtt_clients[cli_idx % len(mqtt_clients)]

        success = False
        try:
            success = send_mqtt_message(client, thread_id, full_text)
        except Exception:
            pass

        cli_idx = (cli_idx + 1) % len(mqtt_clients)

        with proc_lock:
            p = processes.get(stop_key)
            if p:
                p["current_index"] = (msg_idx + 1) % len(messages)
                if success:
                    p["sent_count"]  = p.get("sent_count", 0) + 1
                else:
                    p["failed_count"] = p.get("failed_count", 0) + 1
                p["last_message"]  = full_text

        save_state()
        time.sleep(speed)

    # ── cleanup ──
    for c in mqtt_clients:
        try:
            c.loop_stop()
            c.disconnect()
        except Exception:
            pass

    with proc_lock:
        p = processes.get(stop_key)
        if p:
            p["running"] = False
            p["status"]  = "stopped"
            p.pop("mqtt_clients", None)
    save_state()


# ══════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/validate", methods=["POST"])
def api_validate():
    """Validate cookie(s) — returns user IDs without connecting MQTT."""
    data        = request.get_json(force=True)
    raw         = data.get("cookies", "")
    multi       = data.get("multi", False)

    lines = [l.strip() for l in raw.split("\n") if l.strip()] if multi else [raw.strip()]

    found = []
    for line in lines[:10]:
        uid, _ = parse_one_cookie_string(line)
        if uid:
            found.append(uid)

    if not found:
        return jsonify({"ok": False, "error": "c_user cookie not found. Check your cookies."}), 400

    return jsonify({"ok": True, "user_ids": found, "count": len(found)})


@app.route("/api/start", methods=["POST"])
def api_start():
    data         = request.get_json(force=True)
    cookies_raw  = data.get("cookies", "").strip()
    thread_id    = data.get("thread_id", "").strip()
    messages_txt = data.get("messages", "").strip()
    speed        = max(0.5, float(data.get("speed", 2)))
    hater_name   = data.get("hater_name", "").strip()
    task_name    = data.get("task_name", "").strip() or "Unnamed Task"
    multi_cookie = bool(data.get("multi_cookie", False))

    if not cookies_raw:
        return jsonify({"ok": False, "error": "Cookies required"}), 400
    if not thread_id:
        return jsonify({"ok": False, "error": "Target UID required"}), 400
    if not messages_txt:
        return jsonify({"ok": False, "error": "Messages required"}), 400

    messages = [m.strip() for m in messages_txt.splitlines() if m.strip()]
    if not messages:
        return jsonify({"ok": False, "error": "No messages found in file"}), 400

    stop_key = str(uuid.uuid4())

    proc = {
        "stop_key":      stop_key,
        "cookies_raw":   cookies_raw,
        "thread_id":     thread_id,
        "messages":      messages,
        "speed":         speed,
        "hater_name":    hater_name,
        "task_name":     task_name,
        "created_date":  datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "multi_cookie":  multi_cookie,
        "current_index": 0,
        "sent_count":    0,
        "failed_count":  0,
        "total_cookies": 0,
        "start_time":    time.time(),
        "running":       True,
        "status":        "starting",
        "last_message":  "",
    }

    with proc_lock:
        processes[stop_key] = proc

    save_state()

    t = threading.Thread(target=_worker, args=(stop_key,), daemon=True)
    t.start()

    return jsonify({
        "ok": True,
        "stop_key": stop_key,
        "task_name": task_name,
        "created_date": proc["created_date"],
    })


@app.route("/api/stop", methods=["POST"])
def api_stop():
    data     = request.get_json(force=True)
    stop_key = data.get("stop_key", "").strip()

    with proc_lock:
        proc = processes.get(stop_key)
        if not proc:
            return jsonify({"ok": False, "error": "Invalid or already stopped stop key"}), 404
        proc["running"] = False

    # Remove from persistence (user explicitly stopped)
    with proc_lock:
        processes.pop(stop_key, None)
    save_state()

    return jsonify({"ok": True, "message": "Process stopped successfully"})


@app.route("/api/status/<stop_key>")
def api_status(stop_key):
    with proc_lock:
        proc = processes.get(stop_key)
    if not proc:
        return jsonify({"ok": False, "error": "Not found"}), 404

    # Calculate uptime
    start_time = proc.get("start_time", 0)
    elapsed    = int(time.time() - start_time) if start_time else 0
    days       = elapsed // 86400
    hours      = (elapsed % 86400) // 3600
    minutes    = (elapsed % 3600) // 60
    seconds    = elapsed % 60
    uptime_str = f"{days} days {hours:02d}:{minutes:02d}:{seconds:02d}"

    return jsonify({
        "ok":             True,
        "task_id":        stop_key,
        "running":        proc.get("running", False),
        "status":         proc.get("status", "unknown"),
        "sent_count":     proc.get("sent_count", 0),
        "failed_count":   proc.get("failed_count", 0),
        "total_cookies":  proc.get("total_cookies", 0),
        "uptime":         uptime_str,
        "last_message":   proc.get("last_message", ""),
        "current_index":  proc.get("current_index", 0),
        "total_messages": len(proc.get("messages", [])),
        "thread_id":      proc.get("thread_id", ""),
        "speed":          proc.get("speed", 0),
    })


@app.route("/api/tasks")
def api_tasks():
    """Return all active (running) processes with stats."""
    result = []
    with proc_lock:
        for stop_key, proc in processes.items():
            start_time = proc.get("start_time", 0)
            elapsed    = int(time.time() - start_time) if start_time else 0
            days       = elapsed // 86400
            hours      = (elapsed % 86400) // 3600
            minutes    = (elapsed % 3600) // 60
            seconds    = elapsed % 60
            uptime_str = f"{days} days {hours:02d}:{minutes:02d}:{seconds:02d}"
            result.append({
                "task_id":        stop_key,
                "task_name":      proc.get("task_name", "Unnamed Task"),
                "created_date":   proc.get("created_date", ""),
                "running":        proc.get("running", False),
                "status":         proc.get("status", "unknown"),
                "sent_count":     proc.get("sent_count", 0),
                "failed_count":   proc.get("failed_count", 0),
                "total_cookies":  proc.get("total_cookies", 0),
                "uptime":         uptime_str,
                "thread_id":      proc.get("thread_id", ""),
                "hater_name":     proc.get("hater_name", ""),
                "speed":          proc.get("speed", 0),
                "total_messages": len(proc.get("messages", [])),
                "last_message":   proc.get("last_message", ""),
                "start_timestamp": proc.get("start_time", 0) * 1000,
            })
    return jsonify({"ok": True, "tasks": result, "count": len(result)})


# ══════════════════════════════════════════════════════════════════
# STARTUP — auto-resume all previously running processes
# ══════════════════════════════════════════════════════════════════

def _resume_saved_processes():
    saved = load_state()
    if not saved:
        return
    print(f"[Resume] Found {len(saved)} saved process(es) — resuming...")
    for stop_key, pdata in saved.items():
        if not pdata.get("running", False):
            continue
        pdata["running"] = True
        pdata["status"]  = "resuming"
        pdata.setdefault("sent_count", 0)
        pdata.setdefault("current_index", 0)
        pdata.setdefault("last_message", "")
        with proc_lock:
            processes[stop_key] = pdata
        t = threading.Thread(target=_worker, args=(stop_key,), daemon=True)
        t.start()
        print(f"  → Resumed: {stop_key[:8]}… | thread={pdata.get('thread_id')} | sent={pdata.get('sent_count')}")


if __name__ == "__main__":
    _resume_saved_processes()
    port = int(os.environ.get("PORT", 21949))
    print(f"[Server] Starting on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
