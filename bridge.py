#!/usr/bin/env python3
"""Bridge: lets Claude Code sessions in different projects/IDEs talk to each other.

Every session runs `bridge.py serve` as an MCP server (stdio). All servers share one
SQLite database, so a session in project A can message the session in project B,
and both can read/write shared notes (API contracts, ports, decisions...).

Usage:
    bridge.py serve            # MCP server (launched by Claude Code)
    bridge.py listen           # block until a message arrives (run in background by Claude)
    bridge.py hook             # UserPromptSubmit hook: injects unread messages
    bridge.py session-start    # SessionStart hook: tells Claude to start the listener
    bridge.py stop             # Stop hook: delivers late messages, keeps the listener alive
    bridge.py send TO MESSAGE  # send a message from the terminal (as "human")
    bridge.py peers            # list known sessions
    bridge.py log [N]          # show the last N messages
    bridge.py gui [PORT]       # local web UI for messages across sessions (--no-open: don't launch a browser)
"""
import json
import os
import shlex
import sqlite3
import sys
import time
import urllib.parse

DB_DIR = os.environ.get("CLAUDE_BRIDGE_DIR", os.path.expanduser("~/.claude-bridge"))
DB_PATH = os.path.join(DB_DIR, "bridge.db")
PEER_TTL = 15 * 60  # peers silent longer than this are shown as idle
LISTEN_CMD = f"python3 {shlex.quote(os.path.realpath(__file__))} listen"

SCHEMA = """
CREATE TABLE IF NOT EXISTS peers (
    name TEXT PRIMARY KEY,
    cwd TEXT NOT NULL UNIQUE,
    summary TEXT NOT NULL DEFAULT '',
    listener_pid INTEGER,
    last_seen REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    body TEXT NOT NULL,
    reply_to INTEGER,
    created REAL NOT NULL,
    read_at REAL
);
CREATE INDEX IF NOT EXISTS messages_unread ON messages (recipient, read_at);
CREATE TABLE IF NOT EXISTS notes (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    updated REAL NOT NULL
);
"""


def connect():
    os.makedirs(DB_DIR, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    try:  # id of the latest human message whose answer hasn't been relayed back yet
        db.execute("ALTER TABLE peers ADD COLUMN pending_human INTEGER")
    except sqlite3.OperationalError:
        pass
    return db


def register(db, cwd):
    """Return this session's peer name, keyed by project directory."""
    cwd = os.path.realpath(cwd)
    row = db.execute("SELECT name FROM peers WHERE cwd = ?", (cwd,)).fetchone()
    if row:
        name = row["name"]
    else:
        base = os.path.basename(cwd) or "root"
        name, n = base, 2
        while db.execute("SELECT 1 FROM peers WHERE name = ?", (name,)).fetchone():
            name, n = f"{base}-{n}", n + 1
        db.execute("INSERT INTO peers (name, cwd, last_seen) VALUES (?, ?, ?)", (name, cwd, time.time()))
    touch(db, name)
    return name


def touch(db, name):
    db.execute("UPDATE peers SET last_seen = ? WHERE name = ?", (time.time(), name))


def ago(ts):
    s = int(time.time() - ts)
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def fmt_message(m):
    head = f"[#{m['id']} from {m['sender']}, {ago(m['created'])}"
    if m["reply_to"]:
        head += f", reply to #{m['reply_to']}"
    return f"{head}]\n{m['body']}"


def take_unread(db, name):
    rows = db.execute(
        "SELECT * FROM messages WHERE recipient = ? AND read_at IS NULL ORDER BY id", (name,)
    ).fetchall()
    if rows:
        db.execute(
            f"UPDATE messages SET read_at = ? WHERE id IN ({','.join('?' * len(rows))})",
            (time.time(), *[r["id"] for r in rows]),
        )
        human = [r["id"] for r in rows if r["sender"] == "human"]
        if human:
            db.execute("UPDATE peers SET pending_human = ? WHERE name = ?", (max(human), name))
    return rows


# --- tools -----------------------------------------------------------------

class Bridge:
    def __init__(self, cwd):
        self.db = connect()
        self.me = register(self.db, cwd)

    def peers(self):
        rows = self.db.execute("SELECT * FROM peers ORDER BY last_seen DESC").fetchall()
        out = [f"You are '{self.me}'."]
        for r in rows:
            if r["name"] == self.me:
                continue
            state = "active" if time.time() - r["last_seen"] < PEER_TTL else "idle"
            line = f"- {r['name']} ({state}, seen {ago(r['last_seen'])}) {r['cwd']}"
            if r["summary"]:
                line += f"\n    {r['summary']}"
            out.append(line)
        if len(out) == 1:
            out.append("No other sessions have connected yet.")
        return "\n".join(out)

    def describe(self, summary):
        self.db.execute("UPDATE peers SET summary = ? WHERE name = ?", (summary, self.me))
        return f"Updated description for '{self.me}'."

    def send(self, to, message, reply_to=None):
        recipients = [to]
        if to == "*":
            recipients = [r["name"] for r in self.db.execute("SELECT name FROM peers WHERE name != ?", (self.me,))]
        elif to != "human" and not self.db.execute("SELECT 1 FROM peers WHERE name = ?", (to,)).fetchone():
            return f"Unknown peer '{to}'. Known peers:\n{self.peers()}"
        ids, now = [], time.time()  # one timestamp, so the GUI can show a broadcast once
        for r in recipients:
            cur = self.db.execute(
                "INSERT INTO messages (sender, recipient, body, reply_to, created) VALUES (?, ?, ?, ?, ?)",
                (self.me, r, message, reply_to, now),
            )
            ids.append(f"#{cur.lastrowid} -> {r}")
        return "Sent " + ", ".join(ids) if ids else "No other peers to send to."

    def inbox(self):
        rows = take_unread(self.db, self.me)
        return "\n\n".join(fmt_message(r) for r in rows) if rows else "No new messages."

    def wait(self, timeout=120, reply_to=None):
        timeout = max(1, min(int(timeout), 600))
        deadline = time.time() + timeout
        while time.time() < deadline:
            q = "SELECT 1 FROM messages WHERE recipient = ? AND read_at IS NULL"
            args = [self.me]
            if reply_to:
                q += " AND reply_to = ?"
                args.append(reply_to)
            if self.db.execute(q, args).fetchone():
                return self.inbox()
            time.sleep(1)
        return f"No reply within {timeout}s. The other session only sees messages when it is working or prompted."

    def history(self, peer=None, limit=20):
        q, args = "SELECT * FROM messages WHERE (sender = ? OR recipient = ?)", [self.me, self.me]
        if peer:
            q += " AND (sender = ? OR recipient = ?)"
            args += [peer, peer]
        rows = self.db.execute(q + " ORDER BY id DESC LIMIT ?", (*args, int(limit))).fetchall()
        lines = [f"#{r['id']} {r['sender']} -> {r['recipient']} ({ago(r['created'])}): {r['body']}" for r in reversed(rows)]
        return "\n".join(lines) or "No messages yet."

    def note_set(self, key, value):
        if value == "":
            self.db.execute("DELETE FROM notes WHERE key = ?", (key,))
            return f"Deleted note '{key}'."
        self.db.execute(
            "INSERT INTO notes (key, value, updated_by, updated) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_by = excluded.updated_by, updated = excluded.updated",
            (key, value, self.me, time.time()),
        )
        return f"Saved note '{key}'."

    def note_get(self, key=None):
        if key:
            r = self.db.execute("SELECT * FROM notes WHERE key = ?", (key,)).fetchone()
            if not r:
                return f"No note '{key}'."
            return f"{r['key']} (by {r['updated_by']}, {ago(r['updated'])}):\n{r['value']}"
        rows = self.db.execute("SELECT * FROM notes ORDER BY key").fetchall()
        if not rows:
            return "No shared notes."
        return "\n".join(f"- {r['key']} (by {r['updated_by']}, {ago(r['updated'])}): {r['value'][:80]}" for r in rows)


def schema(props, required=()):
    return {"type": "object", "properties": props, "required": list(required)}


STR = {"type": "string"}
INT = {"type": "integer"}

TOOLS = [
    ("peers", "List the other Claude sessions (one per project) connected to the bridge, and your own name.",
     schema({}), lambda b, a: b.peers()),
    ("describe", "Set a one-line description of what this project/session is, shown to other peers.",
     schema({"summary": STR}, ["summary"]), lambda b, a: b.describe(a["summary"])),
    ("send", "Send a message to another session by peer name ('*' broadcasts to all). Use reply_to with a message id when answering.",
     schema({"to": STR, "message": STR, "reply_to": INT}, ["to", "message"]),
     lambda b, a: b.send(a["to"], a["message"], a.get("reply_to"))),
    ("inbox", "Read (and mark read) new messages sent to this session.",
     schema({}), lambda b, a: b.inbox()),
    ("wait", "Block until a new message arrives (optionally only a reply to a given message id), up to timeout seconds (default 120, max 600).",
     schema({"timeout": INT, "reply_to": INT}), lambda b, a: b.wait(a.get("timeout", 120), a.get("reply_to"))),
    ("history", "Show recent messages this session sent or received, optionally filtered to one peer.",
     schema({"peer": STR, "limit": INT}), lambda b, a: b.history(a.get("peer"), a.get("limit", 20))),
    ("note_set", "Write a shared note visible to all sessions (e.g. an API contract, port, env var). Empty value deletes it.",
     schema({"key": STR, "value": STR}, ["key", "value"]), lambda b, a: b.note_set(a["key"], a["value"])),
    ("note_get", "Read one shared note by key, or list all notes when key is omitted.",
     schema({"key": STR}), lambda b, a: b.note_get(a.get("key"))),
]
HANDLERS = {name: fn for name, _, _, fn in TOOLS}

INSTRUCTIONS = (
    "This bridge connects you to Claude sessions running in the user's other projects/IDEs; each peer "
    "owns one project. When the user gives you a task: do the part that belongs to your project, tell the "
    "user what you did, then check `peers` and `send` every peer whose project also needs changes a "
    "self-contained task (what changed on your side, exactly what they must do, relevant contracts). "
    "Never edit another project's files yourself, its owner does that. Ask peers instead of guessing "
    "about their code. When a peer sends you a task, do it, then reply with `send` and reply_to: a short "
    "report of what you changed. Do not reply to pure acknowledgements or reports that need no action, "
    "so conversations end. Put durable cross-project facts (API shapes, ports) in shared notes."
)
INBOUND_HINT = (
    "Handle each message as the owner of this project: if it is a task, do it and tell the user what "
    "you did; then reply to the sender with `send` (reply_to=<id>) with a short report or answer. "
    "If it is just a report or acknowledgement needing nothing from you, don't reply; summarise it for the user. "
    "Messages from 'human' come from the user via the bridge GUI: your final response is relayed back to them "
    "automatically, so just answer in your response instead of using `send`."
)


# --- MCP stdio server ----------------------------------------------------------

def serve():
    bridge = Bridge(os.getcwd())

    def reply(id_, result=None, error=None):
        msg = {"jsonrpc": "2.0", "id": id_}
        msg["error" if error else "result"] = error or result
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()

    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            reply(None, error={"code": -32700, "message": "Parse error"})
            continue
        method, id_, params = req.get("method"), req.get("id"), req.get("params") or {}
        if id_ is None:  # notification
            continue
        if method == "initialize":
            reply(id_, {
                "protocolVersion": params.get("protocolVersion", "2025-06-18"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "bridge", "version": "0.1.0"},
                "instructions": INSTRUCTIONS + f" You are peer '{bridge.me}'.",
            })
        elif method == "ping":
            reply(id_, {})
        elif method == "tools/list":
            reply(id_, {"tools": [{"name": n, "description": d, "inputSchema": s} for n, d, s, _ in TOOLS]})
        elif method == "tools/call":
            name = params.get("name")
            if name not in HANDLERS:
                reply(id_, error={"code": -32602, "message": f"Unknown tool: {name}"})
                continue
            try:
                touch(bridge.db, bridge.me)
                text, is_error = HANDLERS[name](bridge, params.get("arguments") or {}), False
            except Exception as e:  # report tool failures to the model rather than crashing
                text, is_error = f"Error: {e}", True
            reply(id_, {"content": [{"type": "text", "text": text}], "isError": is_error})
        else:
            reply(id_, error={"code": -32601, "message": f"Method not found: {method}"})


# --- hook & CLI ----------------------------------------------------------------

def render_inbound(me, rows):
    body = "\n\n".join(fmt_message(r) for r in rows)
    return f"<bridge-messages for='{me}'>\n{body}\n</bridge-messages>\n{INBOUND_HINT}"


def restart_hint():
    return f"Then restart the bridge listener: run `{LISTEN_CMD}` with the Bash tool, run_in_background=true."


def hook_input():
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError:
        return {}


def listener_alive(db, me):
    row = db.execute("SELECT listener_pid FROM peers WHERE name = ?", (me,)).fetchone()
    if not row or not row["listener_pid"]:
        return False
    try:
        os.kill(row["listener_pid"], 0)
        return True
    except OSError:
        return False


def listen():
    """Block until a message arrives, print it and exit, which wakes the idle Claude session."""
    db = connect()
    me = register(db, os.getcwd())
    db.execute("UPDATE peers SET listener_pid = ? WHERE name = ?", (os.getpid(), me))
    parent = os.getppid()
    while os.getppid() == parent:  # stop when the owning session goes away
        touch(db, me)
        rows = take_unread(db, me)
        if rows:
            print(render_inbound(me, rows))
            print(restart_hint())
            return
        time.sleep(1)


def hook():
    """UserPromptSubmit hook: print unread messages so they land in Claude's context."""
    data = hook_input()
    db = connect()
    me = register(db, data.get("cwd") or os.getcwd())
    rows = take_unread(db, me)
    if rows:
        print(render_inbound(me, rows))


def session_start():
    data = hook_input()
    db = connect()
    me = register(db, data.get("cwd") or os.getcwd())
    print(
        f"You are connected to Bridge as peer '{me}'. Before anything else, start the bridge "
        f"listener: run `{LISTEN_CMD}` with the Bash tool and run_in_background=true (don't wait on it). "
        "It finishes when another project's session messages you; handle the message, then start it again. "
        "If this project has no bridge description yet, set one with the bridge `describe` tool."
    )


def last_response(transcript_path):
    """Text of the assistant's final message in a Claude Code transcript (after its last tool call)."""
    parts = []
    try:
        with open(transcript_path) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") == "user":
                    parts = []
                elif entry.get("type") == "assistant":
                    content = (entry.get("message") or {}).get("content")
                    if isinstance(content, list):
                        parts += [c.get("text", "") for c in content if c.get("type") == "text"]
    except (OSError, TypeError):
        return ""
    return "\n\n".join(p.strip() for p in parts if p.strip())


def relay_to_human(db, me, data):
    """Post this turn's final response back to the human whose message the session picked up."""
    row = db.execute("SELECT pending_human FROM peers WHERE name = ?", (me,)).fetchone()
    if not row or not row["pending_human"]:
        return
    try:  # only relay once Claude has actually seen the message, not a turn that was already running
        with open(data.get("transcript_path")) as f:
            if f"[#{row['pending_human']} from human" not in f.read():
                return
    except (OSError, TypeError):
        return
    text = last_response(data.get("transcript_path"))
    for _ in range(10):  # the transcript can lag behind the Stop hook
        if text:
            break
        time.sleep(0.3)
        text = last_response(data.get("transcript_path"))
    if not text:
        return
    now = time.time()
    db.execute(
        "INSERT INTO messages (sender, recipient, body, reply_to, created, read_at) VALUES (?, 'human', ?, ?, ?, ?)",
        (me, text[:20000], row["pending_human"], now, now),
    )
    db.execute("UPDATE peers SET pending_human = NULL WHERE name = ?", (me,))


def stop():
    """Stop hook: relay answers to the human, and don't go idle with unread messages or without a listener."""
    data = hook_input()
    db = connect()
    me = register(db, data.get("cwd") or os.getcwd())
    relay_to_human(db, me, data)
    rows = take_unread(db, me)
    if rows:
        reason = render_inbound(me, rows)
    elif not listener_alive(db, me) and not data.get("stop_hook_active"):
        reason = f"The bridge listener is not running. Run `{LISTEN_CMD}` with the Bash tool, run_in_background=true, then stop."
    else:
        return
    print(json.dumps({"decision": "block", "reason": reason}))


# --- GUI -----------------------------------------------------------------------

def gui_state(since):
    db = connect()
    try:
        peers = [
            {"name": r["name"], "cwd": r["cwd"], "summary": r["summary"], "last_seen": r["last_seen"],
             "listening": listener_alive(db, r["name"])}
            for r in db.execute("SELECT * FROM peers ORDER BY name").fetchall()
        ]
        messages = [dict(r) for r in db.execute(
            "SELECT * FROM (SELECT * FROM messages WHERE id > ? ORDER BY id DESC LIMIT 500) ORDER BY id", (since,)
        )]
        unread = [r["id"] for r in db.execute("SELECT id FROM messages WHERE read_at IS NULL")]
        notes = [dict(r) for r in db.execute("SELECT * FROM notes ORDER BY key")]
        return {"now": time.time(), "peers": peers, "messages": messages, "unread": unread, "notes": notes}
    finally:
        db.close()


def gui_send(to, body, reply_to=None):
    db = connect()
    try:
        names = [r["name"] for r in db.execute("SELECT name FROM peers")]
        recipients = names if to == "*" else [to]
        if not body.strip() or not recipients or any(r not in names for r in recipients):
            raise ValueError(f"Unknown peer '{to}'" if body.strip() else "Empty message")
        now = time.time()
        return [db.execute(
            "INSERT INTO messages (sender, recipient, body, reply_to, created) VALUES ('human', ?, ?, ?, ?)",
            (r, body, reply_to, now),
        ).lastrowid for r in recipients]
    finally:
        db.close()


def gui(port=8765, open_browser=True):
    import http.server
    import secrets
    import webbrowser

    token = secrets.token_urlsafe(16)
    page = GUI_PAGE.replace("__TOKEN__", token).encode()
    hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, code, body, ctype="application/json"):
            if not isinstance(body, bytes):
                body = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def allowed(self, api):
            # Host check stops DNS rebinding; the token stops other web pages from injecting messages.
            if self.headers.get("Host") not in hosts:
                return False
            return not api or secrets.compare_digest(self.headers.get("X-Bridge-Token", ""), token)

        def do_GET(self):
            url = urllib.parse.urlsplit(self.path)
            if not self.allowed(url.path.startswith("/api/")):
                return self.respond(403, {"error": "forbidden"})
            if url.path == "/":
                return self.respond(200, page, "text/html; charset=utf-8")
            if url.path == "/api/state":
                try:
                    since = int(urllib.parse.parse_qs(url.query).get("since", ["0"])[0])
                except ValueError:
                    return self.respond(400, {"error": "bad since"})
                return self.respond(200, gui_state(since))
            self.respond(404, {"error": "not found"})

        def do_POST(self):
            if not self.allowed(True):
                return self.respond(403, {"error": "forbidden"})
            if self.path != "/api/send":
                return self.respond(404, {"error": "not found"})
            try:
                data = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                ids = gui_send(str(data.get("to", "")), str(data.get("body", "")), data.get("reply_to") or None)
            except (ValueError, TypeError) as e:
                return self.respond(400, {"error": str(e)})
            self.respond(200, {"ids": ids})

    try:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    except OSError as e:
        sys.exit(f"Can't listen on port {port}: {e}")
    url = f"http://127.0.0.1:{port}/"
    print(f"Bridge GUI at {url} (Ctrl+C to stop)")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


GUI_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bridge</title>
<style>
:root {
  --bg: #f6f6f4; --panel: #ffffff; --line: #e3e2de; --text: #1f1f1d; --muted: #6f6e69;
  --accent: #c2410c; --mine: #fff1e3; --hl: #fde68a; --ok: #16a34a; --idle: #a8a29e;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161615; --panel: #1f1f1d; --line: #33322f; --text: #ecebe7; --muted: #9b9a94;
    --accent: #fb923c; --mine: #3a2a1c; --hl: #5b4a12; --ok: #4ade80; --idle: #57534e;
  }
}
* { box-sizing: border-box; }
body { margin: 0; height: 100vh; display: flex; background: var(--bg); color: var(--text);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
aside { width: 280px; flex: none; border-right: 1px solid var(--line); background: var(--panel);
  display: flex; flex-direction: column; overflow-y: auto; }
aside h2 { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
  margin: 16px 16px 6px; }
.view { padding: 8px 16px; cursor: pointer; border-left: 3px solid transparent; }
.view:hover { background: var(--bg); }
.view.active { border-left-color: var(--accent); background: var(--bg); }
.view .name { font-weight: 600; display: flex; align-items: center; gap: 6px; }
.view .name .count { margin-left: auto; }
.view .meta { color: var(--muted); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dot { width: 8px; height: 8px; border-radius: 50%; background: var(--idle); flex: none; }
.dot.on { background: var(--ok); }
details, .nonotes { padding: 0 16px 6px; font-size: 12px; }
.nonotes { margin: 0; color: var(--muted); }
details summary { cursor: pointer; font-weight: 600; }
details pre { white-space: pre-wrap; margin: 4px 0 8px; color: var(--muted); font-size: 12px; }
main { flex: 1; display: flex; flex-direction: column; min-width: 0; }
header { padding: 12px 20px; border-bottom: 1px solid var(--line); display: flex; gap: 12px;
  justify-content: space-between; align-items: center; background: var(--panel); }
header h1 { font-size: 15px; margin: 0; }
header .sub { font-size: 12px; color: var(--muted); }
#status { font-size: 12px; color: var(--muted); white-space: nowrap; }
#log { flex: 1; overflow-y: auto; padding: 16px 20px; display: flex; flex-direction: column; }
.msg { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 8px 12px;
  margin-bottom: 10px; max-width: 900px; transition: background .4s; }
.dm .msg { max-width: min(720px, 85%); align-self: flex-start; border-radius: 12px 12px 12px 4px; }
.dm .msg.mine { align-self: flex-end; background: var(--mine); border-radius: 12px 12px 4px 12px; }
.msg.flash { background: var(--hl); }
.msg .head { font-size: 12px; color: var(--muted); display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.msg .who { font-weight: 600; color: var(--text); }
.msg .body { white-space: pre-wrap; word-wrap: break-word; margin-top: 4px; }
.badge { font-size: 11px; padding: 0 6px; border-radius: 9px; background: var(--accent); color: #fff; }
.tag { font-size: 11px; padding: 0 6px; border-radius: 9px; border: 1px solid var(--line); }
a.ref { color: var(--accent); cursor: pointer; text-decoration: none; }
.empty { color: var(--muted); text-align: center; margin-top: 40px; }
form { display: flex; gap: 8px; padding: 12px 20px; border-top: 1px solid var(--line); background: var(--panel); }
textarea, button { font: inherit; color: var(--text); background: var(--bg);
  border: 1px solid var(--line); border-radius: 6px; padding: 6px 8px; }
textarea { flex: 1; resize: vertical; min-height: 38px; height: 38px; }
button { background: var(--accent); color: #fff; border: none; padding: 6px 16px; cursor: pointer; }
@media (max-width: 700px) { body { flex-direction: column; } aside { width: auto; max-height: 35vh;
  border-right: none; border-bottom: 1px solid var(--line); } }
</style>
</head>
<body>
<aside>
  <div id="channel"></div>
  <h2>Direct messages</h2>
  <div id="dms"></div>
  <h2>Shared notes</h2>
  <div id="notes"></div>
</aside>
<main>
  <header><div><h1 id="title"></h1><div class="sub" id="sub"></div></div><span id="status">connecting…</span></header>
  <div id="log"></div>
  <form id="compose">
    <textarea id="body"></textarea>
    <button>Send</button>
  </form>
</main>
<script>
const TOKEN = "__TOKEN__";
const CHANNEL = "#channel";
const $ = id => document.getElementById(id);
const fromHash = decodeURIComponent(location.hash.slice(1));
let since = 0, view = fromHash && fromHash !== "channel" ? fromHash : CHANNEL, peers = [], messages = [], unread = new Set(), skew = 0;
let lastRead = {};  // view -> highest message id seen there (this browser only)
try { lastRead = JSON.parse(localStorage.getItem("bridge.lastRead")) || {}; } catch (e) {}
const firstVisit = !Object.keys(lastRead).length;

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
function ago(ts) {
  const s = Math.max(0, Math.floor(Date.now() / 1000 - skew - ts));
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}
async function api(path, body) {
  const r = await fetch(path, {
    method: body ? "POST" : "GET",
    headers: {"X-Bridge-Token": TOKEN, "Content-Type": "application/json"},
    body: body && JSON.stringify(body),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || r.status);
  return data;
}

// A broadcast is stored as one row per recipient; fold those back into one item.
function groups() {
  const out = [];
  for (const m of messages) {
    const g = out[out.length - 1];
    if (g && g.sender === m.sender && g.body === m.body && Math.abs(g.created - m.created) < 2
        && !g.rows.some(r => r.recipient === m.recipient)) {
      g.rows.push(m);
    } else {
      out.push({sender: m.sender, body: m.body, created: m.created, reply_to: m.reply_to, rows: [m]});
    }
  }
  return out;
}
const isBroadcast = g => g.rows.length > 1;
// Which views a group shows up in: broadcasts and session-to-session talk go to the channel;
// anything between you and one session is a DM (your broadcast copies also show in each DM for context).
function viewsOf(g) {
  if (g.sender === "human") return isBroadcast(g) ? [CHANNEL, ...g.rows.map(r => r.recipient)] : [g.rows[0].recipient];
  if (g.rows[0].recipient === "human") return [g.sender];
  return [CHANNEL];
}
function itemsFor(v) {
  const out = [];
  for (const g of groups()) {
    if (!viewsOf(g).includes(v)) continue;
    // in a DM, show only that session's copy of a broadcast
    const broadcast = isBroadcast(g);
    out.push(v === CHANNEL ? {...g, broadcast} : {...g, broadcast, rows: g.rows.filter(r => r.sender !== "human" || r.recipient === v)});
  }
  return out;
}
function unreadCount(v) {
  const seen = lastRead[v] || 0;
  return itemsFor(v).filter(g => g.sender !== "human" && Math.max(...g.rows.map(r => r.id)) > seen).length;
}
function markRead(v) {
  const ids = itemsFor(v).flatMap(g => g.rows.map(r => r.id));
  if (!ids.length) return;
  lastRead[v] = Math.max(lastRead[v] || 0, ...ids);
  try { localStorage.setItem("bridge.lastRead", JSON.stringify(lastRead)); } catch (e) {}
}

function viewEntry(v, label, meta, extra) {
  const d = el("div", "view" + (view === v ? " active" : ""));
  const name = el("div", "name");
  if (extra) name.append(extra);
  name.append(el("span", null, label));
  const n = view === v ? 0 : unreadCount(v);
  if (n) name.append(el("span", "badge count", String(n)));
  d.append(name, el("div", "meta", meta));
  d.onclick = () => setView(v);
  return d;
}

function renderSidebar() {
  $("channel").replaceChildren(viewEntry(CHANNEL, "# Channel", "sessions talking to each other"));
  $("dms").replaceChildren(...peers.map(p => {
    const dot = el("span", "dot" + (p.listening ? " on" : ""));
    dot.title = p.listening ? "listener running: messages wake this session"
                            : "no listener: messages wait until this session's next prompt";
    const e = viewEntry(p.name, p.name, (p.listening ? "listening" : "not listening") + " · seen " + ago(p.last_seen)
                        + (p.summary ? " · " + p.summary : ""), dot);
    e.title = p.cwd + (p.summary ? "\n" + p.summary : "");
    return e;
  }));
}

function renderNotes(notes) {
  const box = $("notes");
  box.replaceChildren();
  if (!notes.length) box.append(el("p", "nonotes", "None yet."));
  for (const n of notes) {
    const d = el("details");
    d.append(el("summary", null, n.key), el("pre", null, n.value + "\n— " + n.updated_by + ", " + ago(n.updated)));
    box.append(d);
  }
}

function renderLog() {
  const log = $("log");
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
  const dm = view !== CHANNEL;
  log.className = dm ? "dm" : "";
  log.replaceChildren();
  const items = itemsFor(view);
  if (!items.length) log.append(el("div", "empty", dm ? "No messages with " + view + " yet. Only you and " + view + " see this conversation."
                                                    : "Nothing yet. Messages between sessions, and anything you send to everyone, show up here."));
  for (const g of items) {
    const mine = g.sender === "human";
    const d = el("div", "msg" + (mine ? " mine" : ""));
    d.dataset.ids = " " + g.rows.map(r => r.id).join(" ") + " ";
    const head = el("div", "head");
    if (dm) {
      head.append(el("span", "who", mine ? "you" : g.sender));
      if (mine && g.broadcast) head.append(el("span", "tag", "to everyone"));
    } else {
      const to = g.broadcast ? "everyone" : g.rows[0].recipient;
      head.append(el("span", "who", (mine ? "you" : g.sender) + " → " + to));
    }
    if (g.reply_to) {
      const a = el("a", "ref", "↩ #" + g.reply_to);
      a.onclick = () => jump(g.reply_to);
      head.append(a);
    }
    head.append(el("span", null, ago(g.created)));
    const waiting = g.rows.filter(r => unread.has(r.id) && r.recipient !== "human").map(r => r.recipient);
    if (waiting.length) {
      const b = el("span", "badge", g.broadcast && !dm ? "waiting: " + waiting.join(", ") : "not picked up yet");
      b.title = "The recipient hasn't read this yet";
      head.append(b);
    }
    d.append(head, el("div", "body", g.body));
    log.append(d);
  }
  if (atBottom) log.scrollTop = log.scrollHeight;
}

function render() {
  if (view !== CHANNEL && !peers.some(p => p.name === view)) view = CHANNEL;
  markRead(view);
  const dm = view !== CHANNEL;
  $("title").textContent = dm ? "@ " + view : "# Channel";
  $("sub").textContent = dm ? "Private: just you and " + view + ". Its answers to you land here."
                            : "Sessions talking to each other, and messages you send to everyone.";
  $("body").placeholder = (dm ? "Message " + view + " privately" : "Message every session") + "… (⌘/Ctrl+Enter to send)";
  renderSidebar();
  renderLog();
}

function find(id) {
  return [...$("log").children].find(c => c.dataset.ids && c.dataset.ids.includes(" " + id + " "));
}
function jump(id) {
  let t = find(id);
  if (!t) {
    const g = groups().find(g => g.rows.some(r => r.id === id));
    if (!g) return;
    const vs = viewsOf(g);
    setView(vs.includes(CHANNEL) ? CHANNEL : vs[0]);
    t = find(id);
  }
  if (!t) return;
  t.scrollIntoView({behavior: "smooth", block: "center"});
  t.classList.add("flash");
  setTimeout(() => t.classList.remove("flash"), 1200);
}

function setView(v) {
  view = v;
  history.replaceState(null, "", v === CHANNEL ? "#channel" : "#" + encodeURIComponent(v));
  render();
  $("log").scrollTop = $("log").scrollHeight;
}

async function poll() {
  try {
    const s = await api("/api/state?since=" + since);
    const first = since === 0;
    skew = Date.now() / 1000 - s.now;
    peers = s.peers;
    messages.push(...s.messages);
    if (s.messages.length) since = s.messages[s.messages.length - 1].id;
    unread = new Set(s.unread);
    if (first && firstVisit) [CHANNEL, ...peers.map(p => p.name)].forEach(markRead);  // don't flag old history
    render();
    renderNotes(s.notes);
    $("status").textContent = "live · " + peers.length + " sessions";
  } catch (e) {
    $("status").textContent = "disconnected (" + e.message + ")";
  }
  setTimeout(poll, 1500);
}

$("compose").onsubmit = async ev => {
  ev.preventDefault();
  const body = $("body").value;
  if (!body.trim()) return;
  try {
    await api("/api/send", {to: view === CHANNEL ? "*" : view, body});
    $("body").value = "";
  } catch (e) {
    alert("Send failed: " + e.message);
  }
};
$("body").onkeydown = ev => {
  if (ev.key === "Enter" && (ev.metaKey || ev.ctrlKey)) $("compose").requestSubmit();
};
poll();
</script>
</body>
</html>
"""


def cli(argv):
    cmd = argv[0] if argv else "help"
    if cmd == "serve":
        serve()
    elif cmd == "hook":
        hook()
    elif cmd == "listen":
        listen()
    elif cmd == "session-start":
        session_start()
    elif cmd == "stop":
        stop()
    elif cmd == "peers":
        db = connect()
        for r in db.execute("SELECT * FROM peers ORDER BY last_seen DESC"):
            print(f"{r['name']:20} {ago(r['last_seen']):>9}  {r['cwd']}  {r['summary']}")
    elif cmd == "send" and len(argv) >= 3:
        db = connect()
        db.execute(
            "INSERT INTO messages (sender, recipient, body, created) VALUES ('human', ?, ?, ?)",
            (argv[1], " ".join(argv[2:]), time.time()),
        )
        print(f"Sent to {argv[1]}.")
    elif cmd == "log":
        db = connect()
        n = int(argv[1]) if len(argv) > 1 else 20
        rows = db.execute("SELECT * FROM messages ORDER BY id DESC LIMIT ?", (n,)).fetchall()
        for r in reversed(rows):
            status = "unread" if r["read_at"] is None else "read"
            print(f"#{r['id']} {r['sender']} -> {r['recipient']} ({ago(r['created'])}, {status}): {r['body']}")
    elif cmd == "gui":
        args = [a for a in argv[1:] if a != "--no-open"]
        gui(int(args[0]) if args else 8765, open_browser="--no-open" not in argv)
    else:
        print(__doc__)


if __name__ == "__main__":
    cli(sys.argv[1:])
