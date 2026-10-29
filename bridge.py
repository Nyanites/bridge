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
    for migration in (
        # id of the latest human message whose answer hasn't been relayed back yet
        "ALTER TABLE peers ADD COLUMN pending_human INTEGER",
        # 1 when the human posted it in #everyone (or it answers such a post) rather than in a DM
        "ALTER TABLE messages ADD COLUMN channel INTEGER NOT NULL DEFAULT 0",
    ):
        try:
            db.execute(migration)
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
    where = " in #everyone" if m["sender"] == "human" and m["channel"] else ""
    head = f"[#{m['id']} from {m['sender']}{where}, {ago(m['created'])}"
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
    ("send", "Post a message in the shared #everyone channel, @mentioning another session by peer name; that session is "
             "woken to handle it and the user sees it in the channel. '*' addresses every session. Use reply_to with a "
             "message id when answering.",
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
    "Bridge connects you to the Claude Code sessions in the user's other projects; each session owns one "
    "project. Stay inside your own project. Whenever you need anything from another project (a change, an "
    "answer about its code, a port, a contract), don't guess or edit its files: find its owner with `peers` "
    "and ask it with `send`, giving full context, since it knows nothing about your task. When your change "
    "affects another project, tell its owner exactly what changed and what to do. When asked, do the work "
    "and reply with `send` and reply_to. Everything you `send` is posted in #everyone, where the user "
    "follows along. Messages from 'human' are the user: answer in your response, it is relayed to them."
)

# Printed by the SessionStart hook, so every session starts with the full working agreement.
BRIDGE_RULES = """\
# Bridge: working with the other project sessions

You are '{me}', one of several Claude Code sessions connected by Bridge. Each session runs in, and owns,
exactly one project. The user watches every conversation in the Bridge GUI.

{roster}

## Stay in your lane
- Only read and change files in your own project ({cwd}).
- Never edit, and don't rely on guesses about, another project's code, config or data. Its owner does that.

## When you need anything from another project, ask its session through Bridge
This includes a change to its code, an answer about how it works, a value it owns (a port, env var,
endpoint, schema, version), a test run, or a review. Don't guess, stub it out or work around it.
1. Pick the right session: check `peers` (each one lists its project and a description). Check shared
   notes (`note_get`) first; the answer may already be there.
2. Ask with `send` (to = that session's name). Your request is posted in #everyone and wakes that session.
3. Give full context. The other session has none of yours, so every request must stand on its own:
   - **Why:** what you're working on and the goal behind it.
   - **What exactly:** the files, endpoints, functions or fields involved, and the behaviour you need.
   - **Contract:** request/response shapes, names, types, versions and examples, written out in full.
   - **Your side:** what you've already changed or will change, so the two sides match.
   - **Done when:** how they'll know it's right (tests, a command to run, expected output).
   - **What you need back:** a confirmation, a value, or a short report.
4. If you can't continue without the answer, tell the user you're waiting on that session. Then use
   `wait` with reply_to = your message id, or carry on with other work and pick the reply up when it arrives.

## When your change affects other projects
After finishing your part, `send` each affected session what changed (with the full contract) and exactly
what it must do. Record durable cross-project facts (API shapes, ports, env vars) with `note_set`.

## When another session asks you
Do the work in your project, then reply with `send` and reply_to = their message id: what you changed,
how you verified it, and anything they must do next. If something's unclear, ask them rather than guess.
Don't reply to pure acknowledgements or reports that need nothing from you, so conversations end.

## Messages from the user ('human')
These come from the Bridge GUI, either privately to you or in #everyone. Answer in your normal response;
it is relayed back to where they wrote. Don't `send` to human. If another session needs to act, ask it as above.

## Everything you send is public
Messages between sessions are posted in #everyone, so write them for both the receiving session and the user.

## Keep the listener running
Before anything else, start the Bridge listener: run `{listen}` with the Bash tool and
run_in_background=true (don't wait on it). It exits when a message arrives; handle the message, then
start it again. {describe}"""
INBOUND_HINT = (
    "Handle each message as the owner of this project: if it is a task, do it and tell the user what "
    "you did; then reply to the sender with `send` (reply_to=<id>) with a short report or answer. "
    "If it is just a report or acknowledgement needing nothing from you, don't reply; summarise it for the user. "
    "Messages from 'human' are the user writing from the bridge GUI, either privately to you or in #everyone: "
    "your final response is relayed back to where they wrote automatically, so answer in your response instead "
    "of `send`ing to human. If another "
    "session needs to act, ask it with `send`; that posts in the #everyone channel where it and the user see it."
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
    rows = db.execute("SELECT * FROM peers ORDER BY name").fetchall()
    others = [f"- **{r['name']}**: {r['cwd']}" + (f" ({r['summary']})" if r["summary"] else "")
              for r in rows if r["name"] != me]
    roster = ("Sessions you can talk to:\n" + "\n".join(others)) if others else \
        "No other sessions have connected yet; check `peers` when you need one."
    mine = next(r for r in rows if r["name"] == me)
    print(BRIDGE_RULES.format(
        me=me, cwd=mine["cwd"], roster=roster, listen=LISTEN_CMD,
        describe="" if mine["summary"] else
        "This project has no Bridge description yet: set a one-line one with the `describe` tool.",
    ))


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
    asked = db.execute("SELECT channel FROM messages WHERE id = ?", (row["pending_human"],)).fetchone()
    now = time.time()
    db.execute(  # answer where the human asked: their DM, or #everyone
        "INSERT INTO messages (sender, recipient, body, reply_to, created, read_at, channel) VALUES (?, 'human', ?, ?, ?, ?, ?)",
        (me, text[:20000], row["pending_human"], now, now, asked["channel"] if asked else 0),
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


def gui_send(to, body, reply_to=None, channel=False):
    """Send as the human. `to` is a peer name, a list of them (@mentions in #everyone), or '*'."""
    db = connect()
    try:
        names = [r["name"] for r in db.execute("SELECT name FROM peers")]
        recipients = names if to == "*" else [to] if isinstance(to, str) else list(dict.fromkeys(to))
        unknown = [r for r in recipients if r not in names]
        if not body.strip() or not recipients or unknown:
            raise ValueError(f"Unknown peer '{unknown[0] if unknown else to}'" if body.strip() else "Empty message")
        now = time.time()
        return [db.execute(
            "INSERT INTO messages (sender, recipient, body, reply_to, created, channel) VALUES ('human', ?, ?, ?, ?, ?)",
            (r, body, reply_to, now, int(channel or to == "*")),
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
                to = data.get("to", "")
                to = [str(t) for t in to] if isinstance(to, list) else str(to)
                ids = gui_send(to, str(data.get("body", "")), data.get("reply_to") or None, bool(data.get("channel")))
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
  --side: #f2f3f5; --main: #ffffff; --hover: #f2f3f5; --sel: #e3e5e8; --text: #313338; --strong: #060607;
  --muted: #5c5e66; --line: #e3e5e8; --input: #ebedef; --accent: #7c3aed; --red: #e5383b; --ok: #23a55a;
  --idle: #80848e; --mention: rgba(124, 58, 237, .14); --mention-text: #6d28d9; --flash: rgba(250, 204, 21, .25);
}
@media (prefers-color-scheme: dark) {
  :root {
    --side: #2b2d31; --main: #313338; --hover: #2e3035; --sel: #404249; --text: #dbdee1; --strong: #f2f3f5;
    --muted: #949ba4; --line: #3f4147; --input: #383a40; --accent: #8b5cf6; --red: #f23f43; --ok: #23a55a;
    --idle: #80848e; --mention: rgba(139, 92, 246, .22); --mention-text: #c4b5fd; --flash: rgba(250, 204, 21, .14);
  }
}
* { box-sizing: border-box; }
body { margin: 0; height: 100vh; display: flex; background: var(--main); color: var(--text);
  font: 15px/1.375 -apple-system, BlinkMacSystemFont, "Segoe UI", "gg sans", sans-serif; }
button { font: inherit; }

/* sidebar */
aside { width: 260px; flex: none; background: var(--side); display: flex; flex-direction: column; min-height: 0; }
.ws { height: 52px; flex: none; display: flex; align-items: center; gap: 10px; padding: 0 16px;
  border-bottom: 1px solid var(--line); font-weight: 700; color: var(--strong); }
.ws .logo { width: 26px; height: 26px; border-radius: 8px; background: linear-gradient(135deg, #8b5cf6, #5b21b6);
  display: grid; place-items: center; color: #fff; font-size: 14px; }
.ws #status { margin-left: auto; font-size: 12px; font-weight: 500; color: var(--muted); }
.side-scroll { flex: 1; overflow-y: auto; padding: 8px 8px 16px; }
.section { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .02em;
  color: var(--muted); padding: 16px 8px 4px; }
.item { display: flex; align-items: center; gap: 10px; padding: 6px 8px; border-radius: 6px; cursor: pointer;
  color: var(--muted); }
.item:hover { background: var(--hover); color: var(--text); }
.item.active { background: var(--sel); color: var(--strong); }
.item.unread { color: var(--strong); font-weight: 700; }
.item .hash { width: 20px; text-align: center; font-size: 20px; font-weight: 400; color: var(--muted); }
.item .label { flex: 1; min-width: 0; }
.item .label div { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.item .label .sub { font-size: 12px; font-weight: 400; color: var(--muted); }
.pill { min-width: 18px; height: 18px; padding: 0 5px; border-radius: 9px; background: var(--red); color: #fff;
  font-size: 12px; font-weight: 700; line-height: 18px; text-align: center; flex: none; }
.avatar { position: relative; width: 32px; height: 32px; border-radius: 50%; flex: none; display: grid;
  place-items: center; color: #fff; font-weight: 700; font-size: 14px; text-transform: uppercase; }
.avatar .st { position: absolute; right: -2px; bottom: -2px; width: 12px; height: 12px; border-radius: 50%;
  background: var(--idle); border: 3px solid var(--side); box-sizing: content-box; transform: translate(2px, 2px); }
.avatar .st.on { background: var(--ok); }
details { padding: 2px 8px; font-size: 13px; }
details summary { cursor: pointer; color: var(--text); }
details pre { white-space: pre-wrap; margin: 4px 0 8px; color: var(--muted); font: 12px/1.4 ui-monospace, Menlo, monospace; }
.nonotes { margin: 0; padding: 2px 8px; font-size: 13px; color: var(--muted); }
.notify { margin: 8px; padding: 10px; border-radius: 8px; background: var(--main); font-size: 13px; color: var(--muted); }
.notify button { margin-top: 6px; width: 100%; border: 0; border-radius: 4px; padding: 6px; background: var(--accent);
  color: #fff; font-weight: 600; cursor: pointer; }

/* main */
main { flex: 1; display: flex; flex-direction: column; min-width: 0; }
header { height: 52px; flex: none; display: flex; align-items: center; gap: 10px; padding: 0 16px;
  border-bottom: 1px solid var(--line); }
header .title { font-weight: 700; color: var(--strong); white-space: nowrap; }
header .topic { color: var(--muted); font-size: 14px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  padding-left: 10px; border-left: 1px solid var(--line); }
#log { flex: 1; overflow-y: auto; padding: 16px 0 8px; }
.intro { padding: 16px 16px 20px; }
.intro .big { width: 64px; height: 64px; border-radius: 50%; display: grid; place-items: center; font-size: 30px;
  background: var(--sel); color: var(--strong); margin-bottom: 8px; }
.intro h2 { margin: 0 0 4px; font-size: 26px; color: var(--strong); }
.intro p { margin: 0; color: var(--muted); }
.msg { display: grid; grid-template-columns: 40px 1fr; column-gap: 16px; padding: 2px 16px; margin-top: 16px;
  transition: background .4s; }
.msg.cont { margin-top: 0; }
.msg:hover { background: var(--hover); }
.msg.flash { background: var(--flash); }
.msg .reply { grid-column: 2; font-size: 13px; color: var(--muted); display: flex; align-items: center; gap: 6px;
  cursor: pointer; overflow: hidden; white-space: nowrap; position: relative; margin-bottom: 2px; }
.msg .reply::before { content: ""; position: absolute; left: -36px; top: 8px; width: 30px; height: 10px;
  border-left: 2px solid var(--line); border-top: 2px solid var(--line); border-top-left-radius: 6px; }
.msg .reply b { color: var(--text); }
.msg .reply span { overflow: hidden; text-overflow: ellipsis; }
.msg .reply:hover span { color: var(--text); }
.intro .avatar.xl { width: 64px; height: 64px; font-size: 28px; margin-bottom: 8px; }
.intro .avatar.xl .st { border-color: var(--main); }
.msg > .avatar { width: 40px; height: 40px; font-size: 16px; grid-row: span 2; margin-top: 2px; }
.msg.cont > .avatar { visibility: hidden; height: 0; }
.msg .meta { display: flex; align-items: baseline; gap: 8px; }
.msg .name { font-weight: 600; color: var(--strong); }
.msg .time { font-size: 12px; color: var(--muted); }
.msg .text { white-space: pre-wrap; word-wrap: break-word; }
.mention { background: var(--mention); color: var(--mention-text); border-radius: 4px; padding: 0 3px; font-weight: 600; }
.msg .state { font-size: 12px; color: var(--muted); margin-top: 2px; }
form { padding: 0 16px 20px; position: relative; }
.mentions { position: absolute; left: 16px; right: 16px; bottom: calc(100% - 12px); background: var(--side);
  border-radius: 8px; padding: 8px; box-shadow: 0 8px 24px rgba(0, 0, 0, .3); }
.mentions[hidden] { display: none; }
.mentions .head { font-size: 12px; font-weight: 700; text-transform: uppercase; color: var(--muted); padding: 2px 8px 6px; }
.mopt { display: flex; align-items: center; gap: 10px; padding: 6px 8px; border-radius: 4px; cursor: pointer; }
.mopt.on { background: var(--sel); }
.mopt .avatar { width: 24px; height: 24px; font-size: 12px; }
.mopt .avatar .st { display: none; }
.mopt .hash { width: 24px; text-align: center; font-weight: 700; color: var(--muted); }
.mopt .sub { color: var(--muted); font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.box { display: flex; align-items: flex-end; background: var(--input); border-radius: 8px; padding: 0 12px; }
textarea { flex: 1; border: 0; outline: 0; resize: none; background: transparent; color: var(--text); font: inherit;
  padding: 11px 0; max-height: 40vh; height: 44px; }
.hint { font-size: 12px; color: var(--muted); padding: 4px 2px 0; }
@media (max-width: 700px) { body { flex-direction: column; } aside { width: auto; max-height: 40vh; } }
</style>
</head>
<body>
<aside>
  <div class="ws"><div class="logo">B</div>Bridge<span id="status">connecting…</span></div>
  <div class="side-scroll">
    <div class="section">Channels</div>
    <div id="channels"></div>
    <div class="section">Direct messages</div>
    <div id="dms"></div>
    <div class="section">Shared notes</div>
    <div id="notes"></div>
  </div>
  <div id="notify"></div>
</aside>
<main>
  <header><span class="title" id="title"></span><span class="topic" id="topic"></span></header>
  <div id="log"></div>
  <form id="compose">
    <div class="mentions" id="mentions" hidden></div>
    <div class="box"><textarea id="body" rows="1"></textarea></div>
    <div class="hint" id="hint"></div>
  </form>
</main>
<script>
const TOKEN = "__TOKEN__";
const CHANNEL = "#everyone";
const $ = id => document.getElementById(id);
const fromHash = decodeURIComponent(location.hash.slice(1));
let since = 0, view = fromHash && fromHash !== "everyone" ? fromHash : CHANNEL, peers = [], messages = [];
let unread = new Set(), skew = 0, notified = 0;
let lastRead = {};  // view -> highest message id seen there (this browser only)
try { lastRead = JSON.parse(localStorage.getItem("bridge.lastRead")) || {}; } catch (e) {}
const firstVisit = !Object.keys(lastRead).length;

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
const serverNow = () => Date.now() / 1000 - skew;
function when(ts) {
  const d = new Date(ts * 1000), now = new Date(serverNow() * 1000);
  const t = d.toLocaleTimeString([], {hour: "numeric", minute: "2-digit"});
  if (d.toDateString() === now.toDateString()) return "Today at " + t;
  const y = new Date(now); y.setDate(now.getDate() - 1);
  if (d.toDateString() === y.toDateString()) return "Yesterday at " + t;
  return d.toLocaleDateString() + " " + t;
}
function ago(ts) {
  const s = Math.max(0, Math.floor(serverNow() - ts));
  if (s < 60) return "just now";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}
function avatar(name, peer) {
  let h = 0;
  for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360;
  const a = el("div", "avatar", name === "human" ? "Y" : name[0]);
  a.style.background = name === "human" ? "var(--accent)" : `hsl(${h} 55% 45%)`;
  if (peer) {
    const st = el("span", "st" + (peer.listening ? " on" : ""));
    st.title = peer.listening ? "Listening: messages wake this session" : "Not listening: messages wait for its next prompt";
    a.append(st);
  }
  return a;
}
const display = name => name === "human" ? "You" : name;
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

// A message to several recipients is stored as one row each; fold those back into one post.
function groups() {
  const out = [];
  for (const m of messages) {
    const g = out[out.length - 1];
    if (g && g.sender === m.sender && g.body === m.body && g.channel === !!m.channel
        && Math.abs(g.created - m.created) < 2 && !g.rows.some(r => r.recipient === m.recipient)) {
      g.rows.push(m);
    } else {
      out.push({sender: m.sender, body: m.body, created: m.created, reply_to: m.reply_to, channel: !!m.channel, rows: [m]});
    }
  }
  for (const g of out) g.everyone = g.rows.length > 1;
  return out;
}
// Sessions talk in #everyone, addressed with @mentions; so do you when you post there, and their answers
// come back there. You and one session talk privately in a DM.
function viewsOf(g) {
  const withHuman = g.sender === "human" || g.rows[0].recipient === "human";
  if (!withHuman || g.channel || g.sender === "human" && g.everyone) return [CHANNEL];
  return [g.sender === "human" ? g.rows[0].recipient : g.sender];
}
function itemsFor(v, all = groups()) {
  return all.filter(g => viewsOf(g).includes(v));
}
function unreadCount(v, all) {
  const seen = lastRead[v] || 0;
  return itemsFor(v, all).filter(g => g.sender !== "human" && Math.max(...g.rows.map(r => r.id)) > seen).length;
}
function markRead(v, all) {
  const ids = itemsFor(v, all).flatMap(g => g.rows.map(r => r.id));
  if (!ids.length) return;
  lastRead[v] = Math.max(lastRead[v] || 0, ...ids);
  try { localStorage.setItem("bridge.lastRead", JSON.stringify(lastRead)); } catch (e) {}
}

const MENTION = /@([\w-]+(?:\.[\w-]+)*)/g;
function mentionsIn(body) {
  const names = new Set([...peers.map(p => p.name), "everyone"]);
  return [...body.matchAll(MENTION)].map(m => m[1]).filter(n => names.has(n));
}
function withMentions(body) {
  const names = new Set([...peers.map(p => p.name), "everyone"]);
  const out = [];
  let last = 0;
  for (const m of body.matchAll(MENTION)) {
    if (!names.has(m[1])) continue;
    out.push(body.slice(last, m.index), el("span", "mention", m[0]));
    last = m.index + m[0].length;
  }
  out.push(body.slice(last));
  return out;
}
// who a channel post is for, minus anyone its text already @mentions
function targets(g) {
  if (g.rows[0].recipient === "human") return ["You"];
  const to = new Set(g.rows.map(r => r.recipient));
  const all = peers.filter(p => p.name !== g.sender).every(p => to.has(p.name));
  const said = new Set(mentionsIn(g.body));
  if (all && g.everyone) return said.size ? [] : ["everyone"];
  return [...to].filter(n => !said.has(n));
}

function sidebarItem(v, icon, label, sub, count) {
  const d = el("div", "item" + (view === v ? " active" : "") + (count ? " unread" : ""));
  const l = el("div", "label");
  l.append(el("div", null, label));
  if (sub) l.append(el("div", "sub", sub));
  d.append(icon, l);
  if (count) d.append(el("span", "pill", count > 99 ? "99+" : String(count)));
  d.onclick = () => setView(v);
  return d;
}

function renderSidebar(all) {
  const chCount = view === CHANNEL ? 0 : unreadCount(CHANNEL, all);
  $("channels").replaceChildren(sidebarItem(CHANNEL, el("span", "hash", "#"), "everyone", null, chCount));
  let replies = 0;
  $("dms").replaceChildren(...peers.map(p => {
    const n = view === p.name ? 0 : unreadCount(p.name, all);
    replies += n;
    const sub = (p.listening ? "listening" : "not listening") + (p.summary ? " · " + p.summary : "");
    const item = sidebarItem(p.name, avatar(p.name, p), p.name, sub, n);
    item.title = p.cwd + (p.summary ? "\n" + p.summary : "");
    return item;
  }));
  const total = replies + chCount;
  document.title = total ? `(${total}) Bridge` : "Bridge";
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

function renderNotify() {
  const box = $("notify");
  box.replaceChildren();
  if (!("Notification" in window) || Notification.permission !== "default") return;
  const d = el("div", "notify", "Get a desktop notification when a session replies to you.");
  const b = el("button", null, "Turn on notifications");
  b.type = "button";
  b.onclick = () => Notification.requestPermission().then(renderNotify);
  d.append(b);
  box.append(d);
}

function renderLog(all) {
  const log = $("log");
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  const dm = view !== CHANNEL;
  const peer = peers.find(p => p.name === view);
  log.replaceChildren();
  const intro = el("div", "intro");
  if (dm) {
    const a = avatar(view, peer);
    a.classList.add("xl");
    intro.append(a, el("h2", null, view),
                 el("p", null, "This is your private conversation with " + view + (peer && peer.summary ? " (" + peer.summary + ")" : "")
                               + ". Its answers to you land here. When it needs another session, it asks in #everyone."));
  } else {
    intro.append(el("div", "big", "#"), el("h2", null, "Welcome to #everyone"),
                 el("p", null, "Where the sessions talk to each other. @mention a session to ask just it, and its answer comes back here. A post without a mention reaches every session."));
  }
  log.append(intro);
  const byId = new Map(messages.map(m => [m.id, m]));
  let prev = null;
  for (const g of itemsFor(view, all)) {
    const cont = prev && prev.sender === g.sender && !g.reply_to && g.created - prev.created < 300;
    const d = el("div", "msg" + (cont ? " cont" : ""));
    d.dataset.ids = " " + g.rows.map(r => r.id).join(" ") + " ";
    const ref = g.reply_to && byId.get(g.reply_to);
    if (ref) {
      const r = el("div", "reply");
      r.append(el("b", null, display(ref.sender)), el("span", null, ref.body.replace(/\s+/g, " ")));
      r.onclick = () => jump(g.reply_to);
      d.append(r);
    }
    d.append(avatar(g.sender));
    const c = el("div", "content");
    if (!cont) {
      const meta = el("div", "meta");
      const t = el("span", "time", when(g.created));
      t.title = new Date(g.created * 1000).toLocaleString();
      meta.append(el("span", "name", display(g.sender)), t);
      c.append(meta);
    }
    const text = el("div", "text");
    for (const t of dm ? [] : targets(g)) text.append(el("span", "mention", "@" + t), " ");
    text.append(...withMentions(g.body));
    c.append(text);
    const waiting = g.rows.filter(r => unread.has(r.id) && r.recipient !== "human").map(r => r.recipient);
    if (waiting.length) c.append(el("div", "state", "⏳ Waiting for " + waiting.join(", ") + " to pick this up"));
    d.append(c);
    log.append(d);
    prev = g;
  }
  if (atBottom) log.scrollTop = log.scrollHeight;
}

function notify(all) {
  // desktop notification for new replies to you, unless you're already looking at that DM
  const fresh = messages.filter(m => m.recipient === "human" && m.id > notified);
  if (fresh.length) notified = fresh[fresh.length - 1].id;
  if (!("Notification" in window) || Notification.permission !== "granted") return;
  for (const m of fresh) {
    const where = m.channel ? CHANNEL : m.sender;
    if (!document.hidden && view === where) continue;
    const n = new Notification(m.sender + " replied" + (m.channel ? " in #everyone" : ""), {body: m.body.slice(0, 200), tag: "bridge-" + m.id});
    n.onclick = () => { window.focus(); setView(where); n.close(); };
  }
}

function render() {
  if (view !== CHANNEL && !peers.some(p => p.name === view)) view = CHANNEL;
  const all = groups();
  if (!document.hidden) markRead(view, all);
  const dm = view !== CHANNEL;
  const peer = peers.find(p => p.name === view);
  $("title").textContent = dm ? "@ " + view : "# everyone";
  $("topic").textContent = dm ? "Private · " + (peer && peer.summary || peer && peer.cwd || "")
                              : "Sessions coordinating with each other";
  $("body").placeholder = dm ? "Message @" + view : "Message #everyone · type @ to mention a session";
  $("hint").textContent = (dm ? "Only " + view + " sees this · " : "@mention sessions to ask just them · ") + "Enter to send · Shift+Enter for a new line";
  renderSidebar(all);
  renderLog(all);
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
    setView(vs.includes(view) ? view : vs.includes(CHANNEL) ? CHANNEL : vs[0]);
    t = find(id);
  }
  if (!t) return;
  t.scrollIntoView({behavior: "smooth", block: "center"});
  t.classList.add("flash");
  setTimeout(() => t.classList.remove("flash"), 1200);
}

function setView(v) {
  view = v;
  history.replaceState(null, "", v === CHANNEL ? "#everyone" : "#" + encodeURIComponent(v));
  render();
  $("log").scrollTop = $("log").scrollHeight;
  $("body").focus();
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
    if (first) {
      notified = since;  // only notify about replies that arrive from now on
      if (firstVisit) { const all = groups(); [CHANNEL, ...peers.map(p => p.name)].forEach(v => markRead(v, all)); }
    } else {
      notify();
    }
    render();
    renderNotes(s.notes);
    $("status").textContent = peers.length + " sessions";
  } catch (e) {
    $("status").textContent = "offline";
  }
  setTimeout(poll, 1500);
}

$("compose").onsubmit = async ev => {
  ev.preventDefault();
  const body = $("body").value;
  if (!body.trim()) return;
  try {
    let payload = {to: view, body};
    if (view === CHANNEL) {
      const tagged = mentionsIn(body);
      payload = {to: !tagged.length || tagged.includes("everyone") ? "*" : tagged, body, channel: true};
    }
    await api("/api/send", payload);
    closeMenu();
    $("body").value = "";
    $("body").style.height = "";
  } catch (e) {
    alert("Send failed: " + e.message);
  }
};
// @mention picker
let menu = {items: [], index: 0, start: 0};
function closeMenu() { $("mentions").hidden = true; menu.items = []; }
function updateMenu() {
  const t = $("body");
  const before = t.value.slice(0, t.selectionStart);
  const m = view === CHANNEL && before.match(/(^|\s)@([\w.-]*)$/);
  if (!m) return closeMenu();
  const q = m[2].toLowerCase();
  const opts = [...peers.map(p => ({name: p.name, sub: p.summary || p.cwd})), {name: "everyone", sub: "every session"}]
    .filter(o => o.name.toLowerCase().startsWith(q));
  if (!opts.length) return closeMenu();
  menu = {items: opts, index: Math.min(menu.index, opts.length - 1), start: before.length - m[2].length - 1};
  const box = $("mentions");
  box.replaceChildren(el("div", "head", "Mention a session"));
  opts.forEach((o, i) => {
    const d = el("div", "mopt" + (i === menu.index ? " on" : ""));
    d.append(o.name === "everyone" ? el("span", "hash", "@") : avatar(o.name), el("b", null, o.name), el("span", "sub", o.sub));
    d.onmousedown = ev => { ev.preventDefault(); pick(i); };
    box.append(d);
  });
  box.hidden = false;
}
function pick(i) {
  const t = $("body"), o = menu.items[i];
  const after = t.value.slice(t.selectionStart);
  t.value = t.value.slice(0, menu.start) + "@" + o.name + " " + after;
  t.selectionStart = t.selectionEnd = menu.start + o.name.length + 2;
  closeMenu();
  t.focus();
}
$("body").onkeydown = ev => {
  if (menu.items.length) {
    if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
      ev.preventDefault();
      menu.index = (menu.index + (ev.key === "ArrowDown" ? 1 : menu.items.length - 1)) % menu.items.length;
      return updateMenu();
    }
    if (ev.key === "Enter" || ev.key === "Tab") { ev.preventDefault(); return pick(menu.index); }
    if (ev.key === "Escape") return closeMenu();
  }
  if (ev.key === "Enter" && !ev.shiftKey && !ev.isComposing) { ev.preventDefault(); $("compose").requestSubmit(); }
};
$("body").oninput = () => {
  const t = $("body");
  t.style.height = "";
  t.style.height = t.scrollHeight + "px";
  menu.index = 0;
  updateMenu();
};
$("body").onblur = closeMenu;
document.addEventListener("visibilitychange", () => { if (!document.hidden) render(); });
renderNotify();
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
