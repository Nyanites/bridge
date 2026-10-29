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
    bridge.py post TO MESSAGE [--image PATH]... [--reply-to ID]
                               # send as this folder's session, with images (sessions can run it via Bash)
    bridge.py peers            # list known sessions
    bridge.py log [N]          # show the last N messages
    bridge.py gui [PORT]       # local web UI for messages across sessions (--no-open: don't launch a browser)
"""
import base64
import binascii
import json
import os
import runpy
import shlex
import shutil
import sqlite3
import sys
import time
import urllib.parse
import uuid

DB_DIR = os.environ.get("CLAUDE_BRIDGE_DIR", os.path.expanduser("~/.claude-bridge"))
DB_PATH = os.path.join(DB_DIR, "bridge.db")
FILES_DIR = os.path.join(DB_DIR, "files")  # copies of images attached to messages
IMAGE_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp"}
MAX_ATTACHMENTS, MAX_ATTACHMENT_BYTES = 5, 10 * 1024 * 1024
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
        # JSON list of image paths under FILES_DIR
        "ALTER TABLE messages ADD COLUMN attachments TEXT",
    ):
        try:
            db.execute(migration)
        except sqlite3.OperationalError:
            pass
    return db


def register(db, cwd, exact=False):
    """Return this session's peer name, keyed by project directory.

    Claude Code's project root wins when it is known (hooks get CLAUDE_PROJECT_DIR). Otherwise, unless `exact`,
    a folder inside an already registered project resolves to that project, so a shell that cd'd into a
    subfolder doesn't turn into a new peer. The MCP server and session start run in the project root, so they're exact.
    """
    root = os.environ.get("CLAUDE_PROJECT_DIR")
    if root:
        cwd, exact = root, True
    cwd = os.path.realpath(cwd)
    row = db.execute("SELECT name FROM peers WHERE cwd = ?", (cwd,)).fetchone()
    if not row and not exact:
        owners = [r for r in db.execute("SELECT name, cwd FROM peers")
                  if r["cwd"] != os.sep and cwd.startswith(r["cwd"].rstrip(os.sep) + os.sep)]
        row = max(owners, key=lambda r: len(r["cwd"]), default=None)  # innermost project
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
    text = f"{head}]\n{m['body']}"
    files = json.loads(m["attachments"] or "[]")
    if files:
        text += "\nAttached images (open each with the Read tool to see it):\n" + "\n".join(f"- {f}" for f in files)
    return text


def store_attachments(paths):
    """Copy images into FILES_DIR so they outlive the sender's own files; returns the stored paths."""
    if not paths:
        return []
    paths = [paths] if isinstance(paths, str) else list(paths)
    if len(paths) > MAX_ATTACHMENTS:
        raise ValueError(f"At most {MAX_ATTACHMENTS} images per message.")
    os.makedirs(FILES_DIR, exist_ok=True)
    stored = []
    for p in paths:
        p = os.path.abspath(os.path.expanduser(str(p)))
        if os.path.splitext(p)[1].lower() not in IMAGE_TYPES:
            raise ValueError(f"{p}: only {', '.join(IMAGE_TYPES)} images can be attached.")
        if not os.path.isfile(p):
            raise ValueError(f"{p}: file not found.")
        if os.path.getsize(p) > MAX_ATTACHMENT_BYTES:
            raise ValueError(f"{p}: larger than {MAX_ATTACHMENT_BYTES // 2**20} MB.")
        dest = os.path.join(FILES_DIR, f"{uuid.uuid4().hex[:12]}-{os.path.basename(p)}")
        shutil.copyfile(p, dest)
        stored.append(dest)
    return stored


def store_uploads(images):
    """Save images uploaded from the GUI ([{name, data: base64}]) into FILES_DIR; returns the stored paths."""
    if not images:
        return []
    if len(images) > MAX_ATTACHMENTS:
        raise ValueError(f"At most {MAX_ATTACHMENTS} images per message.")
    magic = {b"\x89PNG": ".png", b"\xff\xd8\xff": ".jpg", b"GIF8": ".gif"}
    blobs = []
    for img in images:
        try:
            data = base64.b64decode(str(img.get("data", "")), validate=True)
        except binascii.Error:
            raise ValueError("Couldn't read an uploaded image.")
        ext = next((e for sig, e in magic.items() if data.startswith(sig)), None)
        if not ext and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            ext = ".webp"
        if not ext:
            raise ValueError("Only PNG, JPG, GIF and WebP images can be attached.")
        if len(data) > MAX_ATTACHMENT_BYTES:
            raise ValueError(f"Images must be {MAX_ATTACHMENT_BYTES // 2**20} MB or smaller.")
        stem = os.path.splitext(os.path.basename(str(img.get("name") or "image")))[0]
        stem = "".join(c if c.isalnum() or c in "-_." else "-" for c in stem)[:60] or "image"
        blobs.append((stem + ext, data))
    os.makedirs(FILES_DIR, exist_ok=True)
    stored = []
    for name, data in blobs:
        dest = os.path.join(FILES_DIR, f"{uuid.uuid4().hex[:12]}-{name}")
        with open(dest, "wb") as f:
            f.write(data)
        stored.append(dest)
    return stored


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
    def __init__(self, cwd, exact=False):
        self.db = connect()
        self.me = register(self.db, cwd, exact)

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

    def send(self, to, message, reply_to=None, attachments=None):
        recipients = [to]
        if to == "*":
            recipients = [r["name"] for r in self.db.execute("SELECT name FROM peers WHERE name != ?", (self.me,))]
        elif to != "human" and not self.db.execute("SELECT 1 FROM peers WHERE name = ?", (to,)).fetchone():
            return f"Unknown peer '{to}'. Known peers:\n{self.peers()}"
        files = store_attachments(attachments)
        ids, now = [], time.time()  # one timestamp, so the GUI can show a broadcast once
        for r in recipients:
            cur = self.db.execute(
                "INSERT INTO messages (sender, recipient, body, reply_to, created, attachments) VALUES (?, ?, ?, ?, ?, ?)",
                (self.me, r, message, reply_to, now, json.dumps(files) if files else None),
            )
            ids.append(f"#{cur.lastrowid} -> {r}")
        sent = "Sent " + ", ".join(ids) if ids else "No other peers to send to."
        return sent + (f" with {len(files)} image(s)." if files and ids else "")

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
        lines = [f"#{r['id']} {r['sender']} -> {r['recipient']} ({ago(r['created'])}): {r['body']}"
                 + (f" [images: {', '.join(json.loads(r['attachments']))}]" if r["attachments"] else "")
                 for r in reversed(rows)]
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
             "message id when answering. Attach screenshots or diagrams with `attachments` when they add context.",
     schema({"to": STR, "message": STR, "reply_to": INT,
             "attachments": {"type": "array", "items": STR,
                             "description": f"Paths of image files (PNG, JPG, GIF, WebP; up to {MAX_ATTACHMENTS}, "
                                            f"{MAX_ATTACHMENT_BYTES // 2**20} MB each) to attach."}},
            ["to", "message"]),
     lambda b, a: b.send(a["to"], a["message"], a.get("reply_to"), a.get("attachments"))),
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
    "and reply with `send` and reply_to. Attach screenshots or diagrams (`attachments`) when they add context, "
    "and open images you receive with the Read tool. Everything you `send` is posted in #everyone, where the user "
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
4. Attach images when they help. If a picture explains it better (a screenshot of the UI or the bug,
   an error dialog, rendered output, a diagram or mockup), save it as a PNG, JPG, GIF or WebP file and pass
   its path in `send`'s `attachments` (up to 5 images, 10 MB each). Still say in words what the image shows
   and what to look at: the image adds context, it doesn't replace the written request. If your `send`
   tool has no `attachments` option, run `{post} <to> "<message>" --image <path>` with Bash instead.
5. If you can't continue without the answer, tell the user you're waiting on that session. Then use
   `wait` with reply_to = your message id, or carry on with other work and pick the reply up when it arrives.

## When your change affects other projects
After finishing your part, `send` each affected session what changed (with the full contract) and exactly
what it must do. Record durable cross-project facts (API shapes, ports, env vars) with `note_set`.

## When another session asks you
If the message lists attached images, open each one with the Read tool before acting. Do the work in your project, then reply with `send` and reply_to = their message id: what you changed,
how you verified it, and anything they must do next. If something's unclear, ask them rather than guess.
Don't reply to pure acknowledgements or reports that need nothing from you, so conversations end.

## Messages from the user ('human')
These come from the Bridge GUI, either privately to you or in #everyone. Answer in your normal response;
it is relayed back to where they wrote. Don't `send` to human. If another session needs to act, ask it as above.

## Everything you send is public, and replies are threaded
Messages between sessions are posted in #everyone, so write them for both the receiving session and the user.
A message sent with reply_to goes into the thread of the post it answers, so always pass reply_to when you
answer or follow up on something; leave it out only to start a new topic.

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
    code = {"mtime": os.path.getmtime(__file__), "g": globals()}
    bridge = Bridge(os.getcwd(), exact=True)

    def write(msg):
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()

    def reply(id_, result=None, error=None):
        msg = {"jsonrpc": "2.0", "id": id_}
        msg["error" if error else "result"] = error or result
        write(msg)

    def current():
        """Pick up edits to bridge.py without restarting the session, and tell Claude Code the tools changed."""
        nonlocal bridge
        mtime = os.path.getmtime(__file__)
        if mtime != code["mtime"]:
            code["mtime"] = mtime
            try:
                g = runpy.run_path(__file__)  # runs as a plain module, so it won't start serving
                bridge, code["g"] = g["Bridge"](os.getcwd(), exact=True), g
                write({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
            except Exception:  # a half-saved or broken edit: keep serving the code we have
                pass
        return code["g"]

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
        g = current()
        if method == "initialize":
            reply(id_, {
                "protocolVersion": params.get("protocolVersion", "2025-06-18"),
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": {"name": "bridge", "version": "0.1.0"},
                "instructions": g["INSTRUCTIONS"] + f" You are peer '{bridge.me}'.",
            })
        elif method == "ping":
            reply(id_, {})
        elif method == "tools/list":
            reply(id_, {"tools": [{"name": n, "description": d, "inputSchema": s} for n, d, s, _ in g["TOOLS"]]})
        elif method == "tools/call":
            name = params.get("name")
            if name not in g["HANDLERS"]:
                reply(id_, error={"code": -32602, "message": f"Unknown tool: {name}"})
                continue
            try:
                touch(bridge.db, bridge.me)
                text, is_error = g["HANDLERS"][name](bridge, params.get("arguments") or {}), False
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
    me = register(db, data.get("cwd") or os.getcwd(), exact=True)
    rows = db.execute("SELECT * FROM peers ORDER BY name").fetchall()
    others = [f"- **{r['name']}**: {r['cwd']}" + (f" ({r['summary']})" if r["summary"] else "")
              for r in rows if r["name"] != me]
    roster = ("Sessions you can talk to:\n" + "\n".join(others)) if others else \
        "No other sessions have connected yet; check `peers` when you need one."
    mine = next(r for r in rows if r["name"] == me)
    print(BRIDGE_RULES.format(
        me=me, cwd=mine["cwd"], roster=roster, listen=LISTEN_CMD, post=LISTEN_CMD.rsplit(" ", 1)[0] + " post",
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
        for m in messages:
            m["attachments"] = [os.path.basename(f) for f in json.loads(m["attachments"] or "[]")]
        unread = [r["id"] for r in db.execute("SELECT id FROM messages WHERE read_at IS NULL")]
        notes = [dict(r) for r in db.execute("SELECT * FROM notes ORDER BY key")]
        return {"now": time.time(), "peers": peers, "messages": messages, "unread": unread, "notes": notes}
    finally:
        db.close()


def gui_send(to, body, reply_to=None, channel=False, images=None):
    """Send as the human. `to` is a peer name, a list of them (@mentions in #everyone), or '*'.
    `images` are uploads from the GUI: [{name, data: base64}]."""
    db = connect()
    try:
        names = [r["name"] for r in db.execute("SELECT name FROM peers")]
        recipients = names if to == "*" else [to] if isinstance(to, str) else list(dict.fromkeys(to))
        unknown = [r for r in recipients if r not in names]
        if not body.strip() and not images:
            raise ValueError("Empty message")
        if not recipients or unknown:
            raise ValueError(f"Unknown peer '{unknown[0] if unknown else to}'")
        files = store_uploads(images)
        now = time.time()
        return [db.execute(
            "INSERT INTO messages (sender, recipient, body, reply_to, created, channel, attachments) "
            "VALUES ('human', ?, ?, ?, ?, ?, ?)",
            (r, body, reply_to, now, int(channel or to == "*"), json.dumps(files) if files else None),
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
            if url.path.startswith("/files/"):
                return self.send_attachment(url)
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

        def send_attachment(self, url):
            # <img> can't send headers, so the token comes in the query string here
            t = urllib.parse.parse_qs(url.query).get("t", [""])[0]
            if self.headers.get("Host") not in hosts or not secrets.compare_digest(t, token):
                return self.respond(403, {"error": "forbidden"})
            name = urllib.parse.unquote(url.path[len("/files/"):])
            path = os.path.join(FILES_DIR, name)
            ctype = IMAGE_TYPES.get(os.path.splitext(name)[1].lower())
            if name != os.path.basename(name) or not ctype or not os.path.isfile(path):
                return self.respond(404, {"error": "not found"})
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "private, max-age=86400, immutable")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if not self.allowed(True):
                return self.respond(403, {"error": "forbidden"})
            if self.path != "/api/send":
                return self.respond(404, {"error": "not found"})
            try:
                data = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                to = data.get("to", "")
                to = [str(t) for t in to] if isinstance(to, list) else str(to)
                reply_to = int(data["reply_to"]) if data.get("reply_to") else None
                images = data.get("images") or []
                if not isinstance(images, list) or not all(isinstance(i, dict) for i in images):
                    raise ValueError("Bad images")
                ids = gui_send(to, str(data.get("body", "")), reply_to, bool(data.get("channel")), images)
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
main { flex: 1; display: flex; min-width: 0; }
.chat { flex: 1; display: flex; flex-direction: column; min-width: 0; }
#thread { width: 420px; flex: none; display: flex; flex-direction: column; border-left: 1px solid var(--line); min-width: 0; }
#thread[hidden] { display: none; }
#thread .x { margin-left: auto; border: 0; background: none; color: var(--muted); font-size: 24px; line-height: 1;
  cursor: pointer; padding: 0 4px; }
#thread .x:hover { color: var(--strong); }
.tdiv { display: flex; align-items: center; gap: 8px; padding: 8px 16px 0; color: var(--muted); font-size: 13px; }
.tdiv::after { content: ""; flex: 1; border-top: 1px solid var(--line); }
.thread-sum { display: flex; align-items: center; gap: 6px; width: fit-content; margin-top: 4px; padding: 3px 8px 3px 4px;
  border-radius: 6px; border: 1px solid transparent; cursor: pointer; font-size: 13px; }
.thread-sum:hover, .thread-sum.open { background: var(--main); border-color: var(--line); }
.thread-sum .avatar { width: 20px; height: 20px; font-size: 10px; border-radius: 5px; }
.thread-sum .count { color: var(--mention-text); font-weight: 700; }
.thread-sum .new { background: var(--red); color: #fff; border-radius: 9px; padding: 0 6px; font-size: 11px; font-weight: 700; }
.thread-sum .last { color: var(--muted); }
.msg { position: relative; }
.msg .act { position: absolute; right: 16px; top: -14px; display: none; border: 1px solid var(--line); border-radius: 6px;
  background: var(--main); color: var(--text); font-size: 13px; padding: 3px 8px; cursor: pointer; z-index: 1; }
.msg:hover .act { display: block; }
.msg .act:hover { background: var(--sel); }
header { height: 52px; flex: none; display: flex; align-items: center; gap: 10px; padding: 0 16px;
  border-bottom: 1px solid var(--line); }
header .title { font-weight: 700; color: var(--strong); white-space: nowrap; }
header .topic { color: var(--muted); font-size: 14px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  padding-left: 10px; border-left: 1px solid var(--line); }
.log { flex: 1; overflow-y: auto; padding: 16px 0 8px; }
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
.atts { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 6px; }
.atts img { display: block; max-width: min(360px, 100%); max-height: 260px; border-radius: 8px; border: 1px solid var(--line); }
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
.box .attach { flex: none; width: 24px; height: 24px; margin: 10px 12px 10px 0; padding: 0; border: 0; border-radius: 50%;
  background: var(--muted); color: var(--input); font-size: 18px; font-weight: 700; line-height: 24px; cursor: pointer; }
.box .attach:hover { background: var(--text); }
.pending { display: flex; flex-wrap: wrap; gap: 10px; }
.pending:not(:empty) { padding: 6px 0 10px; }
.pending .pi { position: relative; }
.pending img { display: block; width: 72px; height: 72px; object-fit: cover; border-radius: 8px; border: 1px solid var(--line); }
.pending button { position: absolute; top: -7px; right: -7px; width: 20px; height: 20px; padding: 0; border: 0; border-radius: 50%;
  background: var(--red); color: #fff; font-size: 13px; line-height: 20px; cursor: pointer; }
.dragging { outline: 2px dashed var(--accent); outline-offset: -6px; }
.hint { font-size: 12px; color: var(--muted); padding: 4px 2px 0; }
@media (max-width: 700px) { body { flex-direction: column; } aside { width: auto; max-height: 40vh; }
  #thread { position: fixed; inset: 0; width: auto; background: var(--main); z-index: 5; } }
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
  <div class="chat">
    <header><span class="title" id="title"></span><span class="topic" id="topic"></span></header>
    <div class="log" id="log"></div>
    <form id="compose">
      <div class="mentions" hidden></div>
      <div class="pending"></div>
      <div class="box"><button type="button" class="attach" title="Attach images (or paste / drop them)">+</button>
        <textarea rows="1"></textarea><input type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden></div>
      <div class="hint"></div>
    </form>
  </div>
  <section id="thread" hidden>
    <header><span class="title">Thread</span><span class="topic"># everyone</span>
      <button type="button" class="x" id="close-thread" title="Close thread">×</button></header>
    <div class="log" id="tlog"></div>
    <form id="tcompose">
      <div class="mentions" hidden></div>
      <div class="pending"></div>
      <div class="box"><button type="button" class="attach" title="Attach images (or paste / drop them)">+</button>
        <textarea rows="1" placeholder="Reply in thread…"></textarea><input type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden></div>
      <div class="hint"></div>
    </form>
  </section>
</main>
<script>
const TOKEN = "__TOKEN__";
const CHANNEL = "#everyone";
const $ = id => document.getElementById(id);
let since = 0, view = CHANNEL, thread = null, peers = [], messages = [];
let unread = new Set(), skew = 0, notified = 0;
let lastRead = {};  // view or "t<thread root id>" -> highest message id seen there (this browser only)
try { lastRead = JSON.parse(localStorage.getItem("bridge.lastRead")) || {}; } catch (e) {}
const firstVisit = !Object.keys(lastRead).length;
{
  const h = decodeURIComponent(location.hash.slice(1));
  if (h && !h.startsWith("everyone")) view = h;
  else if (h.startsWith("everyone/")) thread = parseInt(h.split("/")[1], 10) || null;
}

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
function save() { try { localStorage.setItem("bridge.lastRead", JSON.stringify(lastRead)); } catch (e) {} }

// A message to several recipients is stored as one row each; fold those back into one post.
function groups() {
  const out = [];
  for (const m of messages) {
    const g = out[out.length - 1];
    if (g && g.sender === m.sender && g.body === m.body && g.channel === !!m.channel
        && Math.abs(g.created - m.created) < 2 && !g.rows.some(r => r.recipient === m.recipient)) {
      g.rows.push(m);
    } else {
      out.push({sender: m.sender, body: m.body, created: m.created, reply_to: m.reply_to, channel: !!m.channel,
                attachments: m.attachments || [], rows: [m]});
    }
  }
  for (const g of out) {
    g.everyone = g.rows.length > 1;
    g.key = g.rows[0].id;
    g.maxId = Math.max(...g.rows.map(r => r.id));
  }
  return out;
}
// Sessions talk in #everyone, addressed with @mentions; so do you when you post there, and their answers
// come back there. You and one session talk privately in a DM.
function viewsOf(g) {
  const withHuman = g.sender === "human" || g.rows[0].recipient === "human";
  if (!withHuman || g.channel || g.sender === "human" && g.everyone) return [CHANNEL];
  return [g.sender === "human" ? g.rows[0].recipient : g.sender];
}
// Everything rendering needs, computed once per render. In #everyone a reply (reply_to) belongs to the
// thread of the top-level post it ultimately answers.
function model() {
  const all = groups();
  const byRow = new Map();
  for (const g of all) for (const r of g.rows) byRow.set(r.id, g);
  const inChannel = g => viewsOf(g).includes(CHANNEL);
  const rootOf = g => {
    let cur = g;
    for (let hops = 0; cur.reply_to && hops < 1000; hops++) {
      const parent = byRow.get(cur.reply_to);
      if (!parent || !inChannel(parent)) break;
      cur = parent;
    }
    return cur;
  };
  const roots = [], threads = new Map();
  for (const g of all.filter(inChannel)) {
    const root = rootOf(g);
    if (root === g) roots.push(g);
    else (threads.get(root.key) || threads.set(root.key, []).get(root.key)).push(g);
  }
  return {all, byRow, roots, threads, rootOf, inChannel};
}
const threadSeen = key => lastRead["t" + key] ?? lastRead.base ?? 0;
const fresh = (g, seen) => g.sender !== "human" && g.maxId > seen;
function dmItems(M, v) { return M.all.filter(g => viewsOf(g)[0] === v && v !== CHANNEL); }
function unreadDM(M, v) { return dmItems(M, v).filter(g => fresh(g, lastRead[v] || 0)).length; }
function unreadThread(M, key) { return (M.threads.get(key) || []).filter(g => fresh(g, threadSeen(key))).length; }
function unreadChannel(M) {
  let n = M.roots.filter(g => fresh(g, lastRead[CHANNEL] || 0)).length;
  for (const key of M.threads.keys()) n += unreadThread(M, key);
  return n;
}
function markRead(M) {
  if (document.hidden) return;
  const top = view === CHANNEL ? M.roots : dmItems(M, view);
  if (top.length) lastRead[view] = Math.max(lastRead[view] || 0, ...top.map(g => g.maxId));
  const reps = thread && M.threads.get(thread);
  if (reps && reps.length) lastRead["t" + thread] = Math.max(threadSeen(thread), ...reps.map(g => g.maxId));
  save();
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

function renderSidebar(M) {
  const chCount = unreadChannel(M);
  $("channels").replaceChildren(sidebarItem(CHANNEL, el("span", "hash", "#"), "everyone", null, chCount));
  let replies = 0;
  $("dms").replaceChildren(...peers.map(p => {
    const n = view === p.name ? 0 : unreadDM(M, p.name);
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

// One message. opts: {M, log, stick, cont, chips, preview, threadKey (adds the thread summary and action)}
function messageEl(g, opts) {
  const d = el("div", "msg" + (opts.cont ? " cont" : ""));
  d.dataset.ids = " " + g.rows.map(r => r.id).join(" ") + " ";
  const ref = opts.preview && g.reply_to && opts.M.byRow.get(g.reply_to);
  if (ref) {
    const r = el("div", "reply");
    r.append(el("b", null, display(ref.sender)), el("span", null, ref.body.replace(/\s+/g, " ")));
    r.onclick = () => jump(g.reply_to);
    d.append(r);
  }
  d.append(avatar(g.sender));
  const c = el("div", "content");
  if (!opts.cont) {
    const meta = el("div", "meta");
    const t = el("span", "time", when(g.created));
    t.title = new Date(g.created * 1000).toLocaleString();
    meta.append(el("span", "name", display(g.sender)), t);
    c.append(meta);
  }
  const text = el("div", "text");
  for (const t of opts.chips ? targets(g) : []) text.append(el("span", "mention", "@" + t), " ");
  text.append(...withMentions(g.body));
  c.append(text);
  if (g.attachments.length) {
    const atts = el("div", "atts");
    for (const name of g.attachments) {
      const url = "/files/" + encodeURIComponent(name) + "?t=" + encodeURIComponent(TOKEN);
      const a = el("a");
      a.href = url;
      a.target = "_blank";
      const img = el("img");
      img.src = url;
      img.alt = name.replace(/^[0-9a-f]{12}-/, "");
      img.title = img.alt;
      img.onload = () => { if (opts.stick) opts.log.scrollTop = opts.log.scrollHeight; };
      a.append(img);
      atts.append(a);
    }
    c.append(atts);
  }
  const waiting = g.rows.filter(r => unread.has(r.id) && r.recipient !== "human").map(r => r.recipient);
  if (waiting.length) c.append(el("div", "state", "⏳ Waiting for " + waiting.join(", ") + " to pick this up"));
  if (opts.threadKey) {
    const reps = opts.M.threads.get(opts.threadKey) || [];
    if (reps.length) {
      const sum = el("div", "thread-sum" + (thread === opts.threadKey ? " open" : ""));
      const who = [...new Set(reps.map(r => r.sender))].slice(0, 4);
      sum.append(...who.map(n => avatar(n)), el("span", "count", reps.length + (reps.length === 1 ? " reply" : " replies")));
      const n = unreadThread(opts.M, opts.threadKey);
      if (n && thread !== opts.threadKey) sum.append(el("span", "new", n + " new"));
      sum.append(el("span", "last", "Last reply " + ago(reps[reps.length - 1].created)));
      sum.onclick = () => openThread(opts.threadKey);
      c.append(sum);
    }
    const act = el("button", "act", "💬 Reply in thread");
    act.type = "button";
    act.onclick = () => openThread(opts.threadKey);
    d.append(act);
  }
  d.append(c);
  return d;
}

function renderList(log, items, opts, intro) {
  const stick = log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  log.replaceChildren(...(intro ? [intro] : []));
  let prev = null;
  for (const g of items) {
    const cont = !opts.noGrouping && prev && prev.sender === g.sender && !(opts.preview && g.reply_to)
                 && g.created - prev.created < 300 && !(opts.M.threads.get(prev.key) || []).length && opts.view !== CHANNEL;
    log.append(messageEl(g, {...opts, log, stick, cont, threadKey: opts.view === CHANNEL ? g.key : null}));
    prev = g;
  }
  if (stick) log.scrollTop = log.scrollHeight;
}

function renderMain(M) {
  const dm = view !== CHANNEL;
  const peer = peers.find(p => p.name === view);
  const intro = el("div", "intro");
  if (dm) {
    const a = avatar(view, peer);
    a.classList.add("xl");
    intro.append(a, el("h2", null, view),
                 el("p", null, "This is your private conversation with " + view + (peer && peer.summary ? " (" + peer.summary + ")" : "")
                               + ". Its answers to you land here. When it needs another session, it asks in #everyone."));
  } else {
    intro.append(el("div", "big", "#"), el("h2", null, "Welcome to #everyone"),
                 el("p", null, "Where the sessions talk to each other. @mention a session to ask just it. Replies collect in a thread under the post they answer."));
  }
  renderList($("log"), dm ? dmItems(M, view) : M.roots, {M, view, chips: !dm, preview: true}, intro);
}

function renderThread(M) {
  const panel = $("thread");
  const root = thread && M.roots.find(g => g.key === thread);
  if (!root || view !== CHANNEL) { panel.hidden = true; thread = null; return; }
  panel.hidden = false;
  const log = $("tlog");
  const reps = M.threads.get(thread) || [];
  const stick = log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  log.replaceChildren(messageEl(root, {M, log, stick, chips: true}),
                      el("div", "tdiv", reps.length ? reps.length + (reps.length === 1 ? " reply" : " replies") : "No replies yet"));
  let prev = null;
  for (const g of reps) {
    const cont = prev && prev.sender === g.sender && g.created - prev.created < 300;
    log.append(messageEl(g, {M, log, stick, cont, chips: true}));
    prev = g;
  }
  if (stick) log.scrollTop = log.scrollHeight;
  const who = threadPeople(M, root);
  $("tcompose").querySelector(".hint").textContent =
    (who.length ? "Goes to " + who.join(", ") : "Goes to every session") + " · @mention to ask someone else · Enter to send";
}
// sessions taking part in a thread; your thread replies go to them
function threadPeople(M, root) {
  const names = new Set(peers.map(p => p.name)), who = new Set();
  for (const g of [root, ...(M.threads.get(root.key) || [])])
    for (const r of g.rows) for (const n of [r.sender, r.recipient]) if (names.has(n)) who.add(n);
  return [...who];
}

function notify(M) {
  // desktop notification for new replies to you, unless you're already looking at them
  const fresh = messages.filter(m => m.recipient === "human" && m.id > notified);
  if (fresh.length) notified = fresh[fresh.length - 1].id;
  if (!("Notification" in window) || Notification.permission !== "granted") return;
  for (const m of fresh) {
    const g = M.byRow.get(m.id), root = g && M.inChannel(g) ? M.rootOf(g) : null;
    const where = m.channel ? CHANNEL : m.sender, inThread = root && root !== g ? root.key : null;
    if (!document.hidden && view === where && (!inThread || thread === inThread)) continue;
    const n = new Notification(m.sender + " replied" + (m.channel ? " in #everyone" : ""), {body: m.body.slice(0, 200), tag: "bridge-" + m.id});
    n.onclick = () => { window.focus(); setView(where); if (inThread) openThread(inThread); n.close(); };
  }
}

function render() {
  if (view !== CHANNEL && !peers.some(p => p.name === view)) view = CHANNEL;
  const M = model();
  markRead(M);
  const dm = view !== CHANNEL;
  const peer = peers.find(p => p.name === view);
  $("title").textContent = dm ? "@ " + view : "# everyone";
  $("topic").textContent = dm ? "Private · " + (peer && peer.summary || peer && peer.cwd || "")
                              : "Sessions coordinating with each other";
  main.input.placeholder = dm ? "Message @" + view : "Message #everyone · type @ to mention a session";
  $("compose").querySelector(".hint").textContent =
    (dm ? "Only " + view + " sees this · " : "@mention sessions to ask just them · ") + "Enter to send · Shift+Enter for a new line · paste or drop images";
  renderSidebar(M);
  renderMain(M);
  renderThread(M);
}

function setHash() {
  history.replaceState(null, "", view !== CHANNEL ? "#" + encodeURIComponent(view) : thread ? "#everyone/" + thread : "#everyone");
}
function setView(v) {
  view = v;
  if (v !== CHANNEL) thread = null;
  setHash();
  render();
  $("log").scrollTop = $("log").scrollHeight;
  main.input.focus();
}
function openThread(key) {
  view = CHANNEL;
  thread = key;
  setHash();
  render();
  $("tlog").scrollTop = $("tlog").scrollHeight;
  side.input.focus();
}
$("close-thread").onclick = () => { thread = null; setHash(); render(); };

function find(log, id) {
  return [...log.children].find(c => c.dataset.ids && c.dataset.ids.includes(" " + id + " "));
}
function jump(id) {
  const M = model(), g = M.byRow.get(id);
  if (!g) return;
  let log = $("log");
  if (M.inChannel(g)) {
    const root = M.rootOf(g);
    if (root !== g) { openThread(root.key); log = $("tlog"); }
    else if (view !== CHANNEL) setView(CHANNEL);
  } else if (view !== viewsOf(g)[0]) setView(viewsOf(g)[0]);
  const t = find(log, id) || find($("tlog"), id);
  if (!t) return;
  t.scrollIntoView({behavior: "smooth", block: "center"});
  t.classList.add("flash");
  setTimeout(() => t.classList.remove("flash"), 1200);
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
      if (lastRead.base == null) lastRead.base = since;  // don't flag history from before this browser looked
      if (firstVisit) {
        lastRead[CHANNEL] = since;
        for (const p of peers) lastRead[p.name] = since;
      }
      save();
    } else {
      notify(model());
    }
    render();
    renderNotes(s.notes);
    $("status").textContent = peers.length + " sessions";
  } catch (e) {
    $("status").textContent = "offline";
  }
  setTimeout(poll, 1500);
}

const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
const asBase64 = file => new Promise((ok, fail) => {
  const r = new FileReader();
  r.onload = () => ok(String(r.result).split(",")[1]);
  r.onerror = () => fail(new Error("couldn't read " + file.name));
  r.readAsDataURL(file);
});
// A send box with the @mention picker and image attachments (button, paste or drop onto dropZone).
// payload(body) says who it goes to.
function composer(form, payload, canMention, dropZone) {
  const input = form.querySelector("textarea"), box = form.querySelector(".mentions");
  const picker = form.querySelector("input[type=file]"), pending = form.querySelector(".pending");
  let menu = {items: [], index: 0, start: 0}, files = [], sending = false;
  function addFiles(list) {
    for (const f of list) {
      if (!IMAGE_TYPES.includes(f.type)) { alert(f.name + ": only PNG, JPG, GIF and WebP images can be attached."); continue; }
      if (f.size > 10 * 1024 * 1024) { alert(f.name + " is larger than 10 MB."); continue; }
      if (files.length >= 5) { alert("At most 5 images per message."); break; }
      files.push({file: f, url: URL.createObjectURL(f)});
    }
    showPending();
    input.focus();
  }
  function showPending() {
    pending.replaceChildren(...files.map((x, i) => {
      const d = el("div", "pi"), img = el("img"), rm = el("button", null, "×");
      img.src = x.url;
      img.title = x.file.name;
      rm.type = "button";
      rm.title = "Remove";
      rm.onclick = () => { URL.revokeObjectURL(x.url); files.splice(i, 1); showPending(); };
      d.append(img, rm);
      return d;
    }));
  }
  form.querySelector(".attach").onclick = () => picker.click();
  picker.onchange = () => { addFiles(picker.files); picker.value = ""; };
  input.onpaste = ev => {
    const imgs = [...(ev.clipboardData ? ev.clipboardData.files : [])];
    if (imgs.length) { ev.preventDefault(); addFiles(imgs); }
  };
  const hasFiles = ev => [...ev.dataTransfer.types].includes("Files");
  dropZone.addEventListener("dragover", ev => { if (hasFiles(ev)) { ev.preventDefault(); dropZone.classList.add("dragging"); } });
  dropZone.addEventListener("dragleave", ev => { if (!dropZone.contains(ev.relatedTarget)) dropZone.classList.remove("dragging"); });
  dropZone.addEventListener("drop", ev => {
    dropZone.classList.remove("dragging");
    if (!ev.dataTransfer.files.length) return;
    ev.preventDefault();
    addFiles(ev.dataTransfer.files);
  });
  const close = () => { box.hidden = true; menu.items = []; };
  function update() {
    const before = input.value.slice(0, input.selectionStart);
    const m = canMention() && before.match(/(^|\s)@([\w.-]*)$/);
    if (!m) return close();
    const q = m[2].toLowerCase();
    const opts = [...peers.map(p => ({name: p.name, sub: p.summary || p.cwd})), {name: "everyone", sub: "every session"}]
      .filter(o => o.name.toLowerCase().startsWith(q));
    if (!opts.length) return close();
    menu = {items: opts, index: Math.min(menu.index, opts.length - 1), start: before.length - m[2].length - 1};
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
    const o = menu.items[i], after = input.value.slice(input.selectionStart);
    input.value = input.value.slice(0, menu.start) + "@" + o.name + " " + after;
    input.selectionStart = input.selectionEnd = menu.start + o.name.length + 2;
    close();
    input.focus();
  }
  input.onkeydown = ev => {
    if (menu.items.length) {
      if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
        ev.preventDefault();
        menu.index = (menu.index + (ev.key === "ArrowDown" ? 1 : menu.items.length - 1)) % menu.items.length;
        return update();
      }
      if (ev.key === "Enter" || ev.key === "Tab") { ev.preventDefault(); return pick(menu.index); }
      if (ev.key === "Escape") return close();
    }
    if (ev.key === "Enter" && !ev.shiftKey && !ev.isComposing) { ev.preventDefault(); form.requestSubmit(); }
  };
  input.oninput = () => {
    input.style.height = "";
    input.style.height = input.scrollHeight + "px";
    menu.index = 0;
    update();
  };
  input.onblur = close;
  form.onsubmit = async ev => {
    ev.preventDefault();
    const body = input.value;
    if (sending || !body.trim() && !files.length) return;
    sending = true;
    try {
      const images = await Promise.all(files.map(async x => ({name: x.file.name, data: await asBase64(x.file)})));
      await api("/api/send", {...payload(body), images});
      close();
      input.value = "";
      input.style.height = "";
      files.forEach(x => URL.revokeObjectURL(x.url));
      files = [];
      showPending();
    } catch (e) {
      alert("Send failed: " + e.message);
    } finally {
      sending = false;
    }
  };
  return {input};
}
const main = composer($("compose"), body => {
  if (view !== CHANNEL) return {to: view, body};
  const tagged = mentionsIn(body);
  return {to: !tagged.length || tagged.includes("everyone") ? "*" : tagged, body, channel: true};
}, () => view === CHANNEL, document.querySelector(".chat"));
const side = composer($("tcompose"), body => {
  const M = model(), root = M.roots.find(g => g.key === thread);
  const reps = M.threads.get(thread) || [], last = reps.length ? reps[reps.length - 1] : root;
  const tagged = mentionsIn(body), who = threadPeople(M, root);
  const to = tagged.includes("everyone") ? "*" : tagged.length ? tagged : who.length ? who : "*";
  return {to, body, channel: true, reply_to: last.rows[0].id};
}, () => true, $("thread"));

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
    elif cmd == "post":
        import argparse
        p = argparse.ArgumentParser(prog="bridge.py post", description="Send as the session that owns this folder.")
        p.add_argument("to", help="peer name, or '*' for every session")
        p.add_argument("message")
        p.add_argument("--image", action="append", default=[], help="image to attach (repeatable)")
        p.add_argument("--reply-to", type=int)
        a = p.parse_args(argv[1:])
        try:
            print(Bridge(os.getcwd()).send(a.to, a.message, a.reply_to, a.image))
        except ValueError as e:
            sys.exit(f"Error: {e}")
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
