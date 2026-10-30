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
    bridge.py quiet NAME [on|off]  # NAME skips messages to everyone (only @mentions reach it)
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
CREATE TABLE IF NOT EXISTS reactions (
    message_id INTEGER NOT NULL,
    who TEXT NOT NULL,
    emoji TEXT NOT NULL,
    created REAL NOT NULL,
    PRIMARY KEY (message_id, who, emoji)
);
"""
BUSY_TTL = 30 * 60  # a turn the Stop hook never closed (you interrupted it, say) stops counting as working after this


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
        # 1 when the session skips messages to everyone (broadcasts, posts without @mentions)
        "ALTER TABLE peers ADD COLUMN quiet INTEGER NOT NULL DEFAULT 0",
        # when the session's current turn started (NULL when it's idle), and what the turn is about
        "ALTER TABLE peers ADD COLUMN busy_since REAL",
        "ALTER TABLE peers ADD COLUMN activity TEXT",
        # when the human pinned it in the GUI
        "ALTER TABLE messages ADD COLUMN pinned REAL",
        # when the human unsent it (only possible before any session read it)
        "ALTER TABLE messages ADD COLUMN deleted REAL",
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


def set_busy(db, name, activity):
    """Mark a session as mid-turn, with a line on what it's doing, or as idle again when activity is None."""
    if activity is None:
        db.execute("UPDATE peers SET busy_since = NULL, activity = NULL WHERE name = ?", (name,))
        return
    now = time.time()
    db.execute(  # a turn that's still going keeps its start time
        "UPDATE peers SET busy_since = CASE WHEN busy_since > ? THEN busy_since ELSE ? END, activity = ? WHERE name = ?",
        (now - BUSY_TTL, now, " ".join(activity.split())[:200], name),
    )


def add_reaction(db, message_id, who, emoji):
    emoji = str(emoji).strip()
    if not emoji or len(emoji) > 16 or any(c.isspace() for c in emoji):
        raise ValueError("A reaction is one emoji, like 👍 or ✅.")
    if not db.execute("SELECT 1 FROM messages WHERE id = ? AND deleted IS NULL", (message_id,)).fetchone():
        raise ValueError(f"No message #{message_id}.")
    db.execute("INSERT OR IGNORE INTO reactions (message_id, who, emoji, created) VALUES (?, ?, ?, ?)",
               (message_id, who, emoji, time.time()))


def save_note(db, key, value, who):
    if value == "":
        db.execute("DELETE FROM notes WHERE key = ?", (key,))
        return f"Deleted note '{key}'."
    db.execute(
        "INSERT INTO notes (key, value, updated_by, updated) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_by = excluded.updated_by, updated = excluded.updated",
        (key, value, who, time.time()),
    )
    return f"Saved note '{key}'."


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
    q = "SELECT * FROM messages WHERE recipient = ? AND read_at IS NULL ORDER BY id"
    if not db.execute(q, (name,)).fetchone():  # the cheap check the listener makes every second
        return []
    db.execute("BEGIN IMMEDIATE")  # so the human can't unsend a message while it's being picked up
    try:
        rows = db.execute(q, (name,)).fetchall()
        if rows:
            db.execute(
                f"UPDATE messages SET read_at = ? WHERE id IN ({','.join('?' * len(rows))})",
                (time.time(), *[r["id"] for r in rows]),
            )
            human = [r["id"] for r in rows if r["sender"] == "human"]
            if human:
                db.execute("UPDATE peers SET pending_human = ? WHERE name = ?", (max(human), name))
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
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
            recipients = [r["name"] for r in self.db.execute("SELECT name FROM peers WHERE name != ? AND NOT quiet", (self.me,))]
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
        q, args = "SELECT * FROM messages WHERE (sender = ? OR recipient = ?) AND deleted IS NULL", [self.me, self.me]
        if peer:
            q += " AND (sender = ? OR recipient = ?)"
            args += [peer, peer]
        rows = self.db.execute(q + " ORDER BY id DESC LIMIT ?", (*args, int(limit))).fetchall()
        lines = [f"#{r['id']} {r['sender']} -> {r['recipient']} ({ago(r['created'])}): {r['body']}"
                 + (f" [images: {', '.join(json.loads(r['attachments']))}]" if r["attachments"] else "")
                 for r in reversed(rows)]
        return "\n".join(lines) or "No messages yet."

    def react(self, message_id, emoji):
        add_reaction(self.db, int(message_id), self.me, emoji)
        return f"Reacted {emoji.strip()} to #{message_id}."

    def note_set(self, key, value):
        return save_note(self.db, key, value, self.me)

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
    ("react", "React to a message with one emoji instead of replying: 👍 got it, 👀 on it, ✅ done, ❌ can't. It doesn't "
              "wake anyone, so use it to acknowledge a report or answer that needs nothing more.",
     schema({"message_id": INT, "emoji": STR}, ["message_id", "emoji"]), lambda b, a: b.react(a["message_id"], a["emoji"])),
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
    "and reply with `send` and reply_to; acknowledge what needs no answer with `react` (👍) instead. Attach "
    "screenshots or diagrams (`attachments`) when they add context, and open images you receive with the Read tool. Everything you `send` is posted in #everyone, where the user "
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
Don't reply to pure acknowledgements or reports that need nothing from you, so conversations end. To show
you've seen one, react to it instead (`react` with 👍, or ✅ when it reports something done): that doesn't wake anyone.

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
    "If it is just a report or acknowledgement needing nothing from you, don't reply (`react` with 👍 if you like); "
    "summarise it for the user. "
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
            set_busy(db, me, reading(rows))  # printing this wakes the session
            print(render_inbound(me, rows))
            print(restart_hint())
            return
        time.sleep(1)


def hook():
    """UserPromptSubmit hook: print unread messages so they land in Claude's context."""
    data = hook_input()
    db = connect()
    me = register(db, data.get("cwd") or os.getcwd())
    prompt = next((line for line in str(data.get("prompt") or "").splitlines() if line.strip()), "")
    set_busy(db, me, prompt or "Working on a prompt")
    rows = take_unread(db, me)
    if rows:
        print(render_inbound(me, rows))


def reading(rows):
    senders = list(dict.fromkeys(display_name(r["sender"]) for r in rows))
    return "Reading " + ("a message" if len(rows) == 1 else "messages") + " from " + ", ".join(senders)


def display_name(name):
    return "you" if name == "human" else name


def session_start():
    data = hook_input()
    db = connect()
    me = register(db, data.get("cwd") or os.getcwd(), exact=True)
    set_busy(db, me, None)
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
        set_busy(db, me, reading(rows))
    elif not listener_alive(db, me) and not data.get("stop_hook_active"):
        reason = f"The bridge listener is not running. Run `{LISTEN_CMD}` with the Bash tool, run_in_background=true, then stop."
    else:
        set_busy(db, me, None)  # the turn is over
        return
    print(json.dumps({"decision": "block", "reason": reason}))


# --- GUI -----------------------------------------------------------------------

def wire(r):
    """A message row as the GUI gets it: attachments by file name only."""
    m = dict(r)
    m["attachments"] = [os.path.basename(f) for f in json.loads(m["attachments"] or "[]")]
    return m


def gui_state(since, first=0):
    """Messages after `since`, plus everything that can change on messages the page already has (id >= `first`):
    reactions, pins and unsent ones."""
    db = connect()
    try:
        now = time.time()
        sent = dict(db.execute(  # a message to several sessions is one row each, all with the same timestamp
            "SELECT sender, COUNT(DISTINCT created) FROM messages WHERE deleted IS NULL GROUP BY sender").fetchall())
        peers = [
            {"name": r["name"], "cwd": r["cwd"], "summary": r["summary"], "last_seen": r["last_seen"],
             "listening": listener_alive(db, r["name"]), "quiet": bool(r["quiet"]), "sent": sent.get(r["name"], 0),
             "busy_since": r["busy_since"] if r["busy_since"] and now - r["busy_since"] < BUSY_TTL else None,
             "activity": r["activity"] or ""}
            for r in db.execute("SELECT * FROM peers ORDER BY name").fetchall()
        ]
        messages = [wire(r) for r in db.execute(
            "SELECT * FROM (SELECT * FROM messages WHERE id > ? AND deleted IS NULL ORDER BY id DESC LIMIT 500) ORDER BY id",
            (since,)
        )]
        unread = [r["id"] for r in db.execute("SELECT id FROM messages WHERE read_at IS NULL")]
        notes = [dict(r) for r in db.execute("SELECT * FROM notes ORDER BY key")]
        reactions = [dict(r) for r in db.execute(
            "SELECT message_id, who, emoji FROM reactions WHERE message_id >= ? ORDER BY created", (first,))]
        pins = [[r["id"], r["pinned"]] for r in db.execute(  # all of them, so the page can load older pinned ones
            "SELECT id, pinned FROM messages WHERE pinned IS NOT NULL AND deleted IS NULL")]
        deleted = [r["id"] for r in db.execute("SELECT id FROM messages WHERE deleted IS NOT NULL AND id >= ?", (first,))]
        oldest = db.execute("SELECT MIN(id) FROM messages WHERE deleted IS NULL").fetchone()[0] or 0
        return {"now": now, "peers": peers, "messages": messages, "unread": unread, "notes": notes,
                "reactions": reactions, "pins": pins, "deleted": deleted, "oldest": oldest}
    finally:
        db.close()


def gui_older(before, until=None):
    """Earlier history: the 300 messages before `before`, or all of them back to `until` (for jumping to an old one)."""
    db = connect()
    try:
        if until:
            rows = db.execute("SELECT * FROM messages WHERE id < ? AND id >= ? AND deleted IS NULL ORDER BY id LIMIT 5000",
                              (before, until))
        else:
            rows = db.execute("SELECT * FROM (SELECT * FROM messages WHERE id < ? AND deleted IS NULL "
                              "ORDER BY id DESC LIMIT 300) ORDER BY id", (before,))
        return {"messages": [wire(r) for r in rows]}
    finally:
        db.close()


def gui_search(query, limit=50):
    """Messages containing every word of `query` (from:NAME limits it to one sender), newest first,
    with a message to several sessions counted once."""
    words, sender = [], None
    for w in query.split():
        if w.lower().startswith("from:") and len(w) > 5:
            sender = w[5:].lstrip("@")
            sender = "human" if sender.lower() in ("me", "you", "human") else sender
        else:
            words.append(w)
    if not words and not sender:
        return {"results": [], "total": 0}
    sql, args = "SELECT * FROM messages WHERE deleted IS NULL", []
    for w in words:
        sql += " AND body LIKE ? ESCAPE '\\'"
        args.append("%" + w.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    if sender:
        sql += " AND sender = ?"
        args.append(sender)
    db = connect()
    try:
        posts = {}
        for r in db.execute(sql + " ORDER BY id DESC LIMIT 2000", args):
            key = (r["sender"], r["created"], r["body"])
            if key in posts:
                posts[key]["to"].append(r["recipient"])
            else:
                posts[key] = {**wire(r), "to": [r["recipient"]]}
        results = list(posts.values())
        return {"results": results[:limit], "total": len(results)}
    finally:
        db.close()


def gui_send(to, body, reply_to=None, channel=False, images=None, everyone=False):
    """Send as the human. `to` is a peer name, a list of them (@mentions in #everyone), or '*'.
    '*' skips quiet sessions unless `everyone` (an explicit @everyone). `images` are GUI uploads: [{name, data: base64}]."""
    db = connect()
    try:
        peers = db.execute("SELECT name, quiet FROM peers").fetchall()
        names = [r["name"] for r in peers]
        if to == "*":
            recipients = [r["name"] for r in peers if everyone or not r["quiet"]]
            if not recipients:
                raise ValueError("Every session skips posts to everyone; @mention the one you want.")
        else:
            recipients = [to] if isinstance(to, str) else list(dict.fromkeys(to))
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


def set_quiet(name, quiet):
    """Make a session skip (or receive again) messages to everyone; @mentions always reach it."""
    db = connect()
    try:
        if not db.execute("UPDATE peers SET quiet = ? WHERE name = ?", (int(quiet), name)).rowcount:
            raise ValueError(f"Unknown peer '{name}'")
    finally:
        db.close()


def ids_of(data):
    ids = data.get("ids")
    if not isinstance(ids, list) or not 0 < len(ids) <= 100:
        raise ValueError("Bad ids")
    return [int(i) for i in ids]


def gui_react(ids, emoji, on):
    """Add your reaction to a post (stored on its first row) or take it back (from any of its rows)."""
    db = connect()
    try:
        if on:
            add_reaction(db, ids[0], "human", emoji)
        else:
            db.execute(f"DELETE FROM reactions WHERE who = 'human' AND emoji = ? AND message_id IN ({','.join('?' * len(ids))})",
                       (emoji, *ids))
    finally:
        db.close()


def gui_pin(ids, pinned):
    db = connect()
    try:
        db.execute(f"UPDATE messages SET pinned = ? WHERE id IN ({','.join('?' * len(ids))})",
                   (time.time() if pinned else None, *ids))
    finally:
        db.close()


def gui_unsend(ids):
    """Take back your message, as long as no session has picked it up yet."""
    db = connect()
    try:
        db.execute("BEGIN IMMEDIATE")  # take_unread can't pick it up halfway through this
        try:
            q = ",".join("?" * len(ids))
            rows = db.execute(f"SELECT sender, read_at FROM messages WHERE id IN ({q}) AND deleted IS NULL", ids).fetchall()
            if len(rows) != len(ids) or any(r["sender"] != "human" for r in rows):
                raise ValueError("You can only unsend your own messages.")
            if any(r["read_at"] for r in rows):
                raise ValueError("Too late: a session has already read it.")
            now = time.time()
            db.execute(f"UPDATE messages SET deleted = ?, read_at = ? WHERE id IN ({q})", (now, now, *ids))
            db.execute(f"DELETE FROM reactions WHERE message_id IN ({q})", ids)
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    finally:
        db.close()


def gui_note(key, value):
    key = key.strip()
    if not key or len(key) > 100:
        raise ValueError("A note needs a short name.")
    db = connect()
    try:
        save_note(db, key, value, "human")
    finally:
        db.close()


def post_send(data):
    to = data.get("to", "")
    to = [str(t) for t in to] if isinstance(to, list) else str(to)
    images = data.get("images") or []
    if not isinstance(images, list) or not all(isinstance(i, dict) for i in images):
        raise ValueError("Bad images")
    reply_to = int(data["reply_to"]) if data.get("reply_to") else None
    return {"ids": gui_send(to, str(data.get("body", "")), reply_to, bool(data.get("channel")), images,
                            bool(data.get("everyone")))}


# What the GUI page can do, by path; each takes the posted JSON object.
POST_ROUTES = {
    "/api/send": post_send,
    "/api/quiet": lambda d: set_quiet(str(d.get("name", "")), bool(d.get("quiet"))),
    "/api/react": lambda d: gui_react(ids_of(d), str(d.get("emoji", "")), bool(d.get("on"))),
    "/api/pin": lambda d: gui_pin(ids_of(d), bool(d.get("pinned"))),
    "/api/unsend": lambda d: gui_unsend(ids_of(d)),
    "/api/note": lambda d: gui_note(str(d.get("key", "")), str(d.get("value", ""))),
}


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
            q = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
            try:
                if url.path == "/api/state":
                    return self.respond(200, gui_state(int(q.get("since", 0)), int(q.get("first", 0))))
                if url.path == "/api/older":
                    return self.respond(200, gui_older(int(q["before"]), int(q["until"]) if q.get("until") else None))
                if url.path == "/api/search":
                    return self.respond(200, gui_search(q.get("q", "")))
            except (KeyError, ValueError):
                return self.respond(400, {"error": "bad query"})
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
            route = POST_ROUTES.get(self.path)
            if not route:
                return self.respond(404, {"error": "not found"})
            try:
                data = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                if not isinstance(data, dict):
                    raise ValueError("Bad request")
                result = route(data)
            except (ValueError, TypeError, KeyError) as e:
                return self.respond(400, {"error": str(e)})
            self.respond(200, result or {"ok": True})

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
  --rail: #e3e5e8; --side: #f2f3f5; --main: #ffffff; --panel: #ebedef; --card: #ffffff; --box: #f2f3f5;
  --hover: rgba(116, 124, 138, .14); --sel: rgba(116, 124, 138, .26); --msg-hover: rgba(6, 6, 7, .03);
  --text: #313338; --strong: #060607; --muted: #5c5e66; --faint: #80848e; --line: rgba(6, 6, 7, .1);
  --input: #ebedef; --btn: #e3e5e8; --code: #f2f3f5; --code-line: #e3e5e8; --thumb: rgba(6, 6, 7, .2);
  --accent: #7c3aed; --red: #da373c; --on: #23a55a; --idle: #f0b232; --off: #80848e; --link: #006ce7;
  --mention: rgba(124, 58, 237, .12); --mention-text: #6d28d9; --flash: rgba(250, 204, 21, .25); --name-l: 38%;
  --elev: 0 1px 0 rgba(6, 6, 7, .08), 0 2px 0 rgba(6, 6, 7, .03); --shadow: 0 8px 24px rgba(0, 0, 0, .16);
  color-scheme: light;
}
:root[data-theme="dark"] {
  --rail: #1e1f22; --side: #2b2d31; --main: #313338; --panel: #232428; --card: #232428; --box: #111214;
  --hover: rgba(78, 80, 88, .3); --sel: rgba(78, 80, 88, .6); --msg-hover: rgba(2, 2, 2, .06);
  --text: #dbdee1; --strong: #f2f3f5; --muted: #949ba4; --faint: #6d6f78; --line: rgba(255, 255, 255, .07);
  --input: #383a40; --btn: #4e5058; --code: #2b2d31; --code-line: #1e1f22; --thumb: rgba(0, 0, 0, .35);
  --accent: #8b5cf6; --red: #f23f43; --link: #00a8fc;
  --mention: rgba(139, 92, 246, .2); --mention-text: #c4b5fd; --flash: rgba(250, 204, 21, .12); --name-l: 72%;
  --elev: 0 1px 0 rgba(4, 4, 5, .2), 0 2px 0 rgba(6, 6, 7, .05); --shadow: 0 8px 24px rgba(0, 0, 0, .4);
  color-scheme: dark;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --rail: #1e1f22; --side: #2b2d31; --main: #313338; --panel: #232428; --card: #232428; --box: #111214;
    --hover: rgba(78, 80, 88, .3); --sel: rgba(78, 80, 88, .6); --msg-hover: rgba(2, 2, 2, .06);
    --text: #dbdee1; --strong: #f2f3f5; --muted: #949ba4; --faint: #6d6f78; --line: rgba(255, 255, 255, .07);
    --input: #383a40; --btn: #4e5058; --code: #2b2d31; --code-line: #1e1f22; --thumb: rgba(0, 0, 0, .35);
    --accent: #8b5cf6; --red: #f23f43; --link: #00a8fc;
    --mention: rgba(139, 92, 246, .2); --mention-text: #c4b5fd; --flash: rgba(250, 204, 21, .12); --name-l: 72%;
    --elev: 0 1px 0 rgba(4, 4, 5, .2), 0 2px 0 rgba(6, 6, 7, .05); --shadow: 0 8px 24px rgba(0, 0, 0, .4);
    color-scheme: dark;
  }
}
* { box-sizing: border-box; scrollbar-width: thin; scrollbar-color: var(--thumb) transparent; }
[hidden] { display: none !important; }
html, body { height: 100%; }
body { margin: 0; overflow: hidden; background: var(--main); color: var(--text); -webkit-font-smoothing: antialiased;
  font: 15px/1.375 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif; }
button { font: inherit; color: inherit; }
.ic { fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; flex: none; display: block; }
.app { display: flex; height: 100vh; }

/* server rail */
.rail { width: 72px; flex: none; background: var(--rail); display: flex; flex-direction: column; align-items: center;
  gap: 8px; padding-top: 12px; --ring: var(--rail); }
.rail-item { position: relative; width: 48px; height: 48px; }
.rail-item::before { content: ""; position: absolute; left: -16px; top: 4px; width: 8px; height: 40px;
  border-radius: 0 4px 4px 0; background: var(--strong); }
.rail-item svg { width: 48px; height: 48px; border-radius: 16px; display: block; }
.rail-item .pill { position: absolute; right: -6px; bottom: -6px; height: 24px; min-width: 24px; border-radius: 12px;
  line-height: 16px; border: 4px solid var(--rail); }
.rail-sep { width: 32px; height: 2px; border-radius: 1px; background: var(--line); }

/* channel sidebar */
.side { width: 240px; flex: none; background: var(--side); display: flex; flex-direction: column; min-height: 0; --ring: var(--side); }
.side-head { height: 48px; flex: none; display: flex; align-items: center; gap: 8px; padding: 0 16px; font-size: 16px;
  font-weight: 700; color: var(--strong); box-shadow: var(--elev); position: relative; z-index: 1; }
.side-scroll { flex: 1; overflow-y: auto; padding: 8px 8px 16px; }
.section { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .02em; color: var(--muted); padding: 18px 8px 4px; }
.item { position: relative; display: flex; align-items: center; gap: 12px; padding: 6px 8px; margin: 1px 0;
  border-radius: 4px; cursor: pointer; color: var(--muted); }
.item.ch { gap: 6px; }
.item:hover { background: var(--hover); color: var(--text); }
.item.active { background: var(--sel); color: var(--strong); }
.item.unread { color: var(--strong); font-weight: 600; }
.item.unread:not(.active)::before { content: ""; position: absolute; left: -8px; top: 50%; margin-top: -4px; width: 4px;
  height: 8px; border-radius: 0 4px 4px 0; background: var(--strong); }
.item .label { flex: 1; min-width: 0; }
.item .label div, .member .label div { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.item .sub, .member .sub { font-size: 12px; font-weight: 400; color: var(--muted); }
.item .quiet-ic, .member .quiet-ic { color: var(--faint); }
.pill { min-width: 16px; height: 16px; padding: 0 4px; border-radius: 8px; background: var(--red); color: #fff;
  font-size: 12px; font-weight: 700; line-height: 16px; text-align: center; flex: none; }

/* you panel */
.me { height: 52px; flex: none; background: var(--panel); display: flex; align-items: center; gap: 4px; padding: 0 8px;
  --ring: var(--panel); }
.me .who { flex: 1; min-width: 0; display: flex; align-items: center; gap: 8px; padding: 2px 4px; border-radius: 4px;
  cursor: pointer; border: 0; background: none; text-align: left; }
.me .who:hover { background: var(--hover); }
.who-txt { display: block; min-width: 0; }
.who-txt b { display: block; font-size: 14px; line-height: 18px; color: var(--strong); }
#status { display: block; font-size: 12px; line-height: 14px; color: var(--muted); overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
.ib { position: relative; width: 32px; height: 32px; flex: none; display: grid; place-items: center; padding: 0;
  border: 0; border-radius: 4px; background: none; color: var(--muted); cursor: pointer; }
.ib:hover { background: var(--hover); color: var(--text); }
.ib.on { color: var(--strong); }
.ib.alert { color: var(--red); }
.ib .badge { position: absolute; right: 1px; bottom: 1px; min-width: 14px; height: 14px; padding: 0 3px; border-radius: 7px;
  background: var(--red); color: #fff; font-size: 10px; font-weight: 700; line-height: 14px; }

/* avatars */
.avatar { --size: 32px; position: relative; width: var(--size); height: var(--size); border-radius: 50%; flex: none;
  display: grid; place-items: center; color: #fff; font-weight: 600; font-size: calc(var(--size) * .42);
  text-transform: uppercase; background: hsl(var(--h, 260) 48% 46%); user-select: none; }
.avatar.me { background: linear-gradient(135deg, #8b5cf6, #5b21b6); }
.avatar.click { cursor: pointer; }
.avatar .st { position: absolute; right: -3px; bottom: -3px; width: calc(var(--size) * .3); height: calc(var(--size) * .3);
  min-width: 10px; min-height: 10px; border-radius: 50%; border: 3px solid var(--ring); box-sizing: content-box; background: var(--off); }
.st.on, .dot.on { background: var(--on); }
.st.idle, .dot.idle { background: var(--idle); }
.st.off, .dot.off { background: var(--off); }
.dot { width: 8px; height: 8px; border-radius: 50%; flex: none; }

/* main area */
main { flex: 1; min-width: 0; display: flex; flex-direction: column; --ring: var(--main); }
.banner { flex: none; display: flex; align-items: center; justify-content: center; gap: 12px; min-height: 36px;
  padding: 4px 48px; background: var(--accent); color: #fff; font-size: 14px; font-weight: 500; position: relative; }
.banner .cta { border: 1px solid rgba(255, 255, 255, .85); background: none; color: #fff; border-radius: 3px;
  padding: 2px 12px; font-size: 13px; font-weight: 600; cursor: pointer; }
.banner .cta:hover { background: #fff; color: var(--accent); }
.banner .ib { position: absolute; right: 8px; color: #fff; }
.banner .ib:hover { background: rgba(255, 255, 255, .15); color: #fff; }
.body { flex: 1; display: flex; min-height: 0; }
.center { flex: 1; display: flex; flex-direction: column; min-width: 0; }
.center-row { flex: 1; display: flex; min-height: 0; }
.chat { flex: 1; display: flex; flex-direction: column; min-width: 0; }
.bar { height: 48px; flex: none; display: flex; align-items: center; gap: 8px; padding: 0 8px 0 16px; box-shadow: var(--elev);
  position: relative; z-index: 1; }
.bar-icon { color: var(--faint); display: flex; }
.bar-icon .avatar { --size: 24px; }
.bar .title { font-weight: 600; color: var(--strong); white-space: nowrap; font-size: 16px; }
.bar .topic { flex: 1; min-width: 0; color: var(--muted); font-size: 14px; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; padding-left: 12px; margin-left: 4px; border-left: 1px solid var(--line); }
.bar .tools { margin-left: auto; display: flex; gap: 4px; flex: none; }

/* messages */
.log { flex: 1; overflow-y: auto; overflow-x: hidden; padding-bottom: 20px; }
.intro { padding: 16px 16px 4px; margin-top: 16px; }
.intro .big { width: 68px; height: 68px; border-radius: 50%; display: grid; place-items: center; background: var(--sel);
  color: var(--strong); margin-bottom: 8px; }
.intro .avatar.xl { --size: 80px; margin-bottom: 12px; }
.intro h2 { margin: 0 0 6px; font-size: 30px; line-height: 1.25; color: var(--strong); font-weight: 800; }
.intro p { margin: 0; color: var(--muted); max-width: 680px; }
.intro p b { color: var(--text); }
.chips { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
.chip { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 4px; background: var(--hover);
  color: var(--text); font-size: 13px; border: 0; }
.chip .ic { width: 14px; height: 14px; }
.daydiv { display: flex; align-items: center; margin: 24px 16px 8px; color: var(--muted); font-size: 12px; font-weight: 600; }
.daydiv::before, .daydiv::after { content: ""; flex: 1; border-top: 1px solid var(--line); }
.daydiv span { padding: 0 8px; }
.newdiv { position: relative; height: 0; margin: 12px 16px 0; border-top: 1px solid var(--red); }
.newdiv span { position: absolute; right: 0; top: -8px; height: 16px; padding: 0 6px; border-radius: 8px 0 0 8px;
  background: var(--red); color: #fff; font-size: 10px; font-weight: 700; line-height: 16px; letter-spacing: .03em; }
.tdiv { display: flex; align-items: center; gap: 8px; padding: 10px 16px 2px; color: var(--muted); font-size: 13px; }
.tdiv::after { content: ""; flex: 1; border-top: 1px solid var(--line); }
.msg { position: relative; display: grid; grid-template-columns: 40px minmax(0, 1fr); column-gap: 16px;
  padding: 2px 48px 2px 16px; margin-top: 17px; transition: background-color .4s; }
.msg.cont { margin-top: 0; }
.msg:hover { background: var(--msg-hover); }
.msg.flash { background: var(--flash); }
.msg > .avatar { --size: 40px; grid-column: 1; margin-top: 2px; }
.msg.cont > .avatar { visibility: hidden; height: 0; }
.msg .content { grid-column: 2; min-width: 0; }
.msg .htime { position: absolute; left: 12px; width: 48px; top: 5px; text-align: center; font-size: 11px; color: var(--muted);
  display: none; }
.msg.cont:hover .htime { display: block; }
.meta { display: flex; align-items: baseline; gap: 6px; min-width: 0; }
.name { font-weight: 600; color: var(--strong); }
.name.peer { color: hsl(var(--h) 62% var(--name-l)); cursor: pointer; }
.name.peer:hover { text-decoration: underline; }
.tag { display: inline-block; height: 15px; padding: 0 4px; border-radius: 3px; background: var(--accent); color: #fff;
  font-size: 10px; font-weight: 700; line-height: 15px; letter-spacing: .02em; align-self: center; vertical-align: 1px; }
.time { font-size: 12px; color: var(--muted); white-space: nowrap; }
.text { white-space: pre-wrap; overflow-wrap: anywhere; }
.text code { font: 85%/1.2 ui-monospace, SFMono-Regular, Menlo, monospace; background: var(--code); padding: .15em .3em;
  border-radius: 4px; }
.text pre { margin: 6px 0; padding: 8px 10px; background: var(--code); border: 1px solid var(--code-line); border-radius: 4px;
  font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; overflow-x: auto; }
.text .h1, .text .h2, .text .h3 { font-weight: 700; color: var(--strong); line-height: 1.3; margin: 6px 0 2px; }
.text .h1 { font-size: 1.45em; } .text .h2 { font-size: 1.2em; } .text .h3 { font-size: 1.05em; }
.text .quote { border-left: 4px solid var(--sel); padding-left: 12px; }
.text a { color: var(--link); text-decoration: none; }
.text a:hover { text-decoration: underline; }
.text strong { color: var(--strong); }
.mention { background: var(--mention); color: var(--mention-text); border-radius: 3px; padding: 0 2px; font-weight: 500; }
.mention.peer { cursor: pointer; }
.mention.peer:hover { background: var(--accent); color: #fff; }
.msg .reply { grid-column: 2; position: relative; display: flex; align-items: center; gap: 4px; margin-bottom: 4px;
  font-size: 13px; color: var(--muted); cursor: pointer; white-space: nowrap; overflow: hidden; }
.msg .reply::before { content: ""; position: absolute; left: -36px; top: 9px; width: 32px; height: 10px;
  border-left: 2px solid var(--sel); border-top: 2px solid var(--sel); border-top-left-radius: 6px; }
.msg .reply .avatar { --size: 16px; font-size: 9px; }
.msg .reply .name { font-size: 13px; margin-right: 2px; }
.msg .reply .rtext { overflow: hidden; text-overflow: ellipsis; }
.msg .reply:hover .rtext { color: var(--text); }
.atts { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 6px; }
.atts a { display: block; border-radius: 8px; overflow: hidden; cursor: zoom-in; }
.atts img { display: block; max-width: min(400px, 100%); max-height: 300px; border-radius: 8px; }
.state { display: flex; align-items: center; gap: 5px; font-size: 12px; color: var(--muted); margin-top: 3px; }
.state .ic { width: 13px; height: 13px; }
.thread-sum { display: flex; align-items: center; gap: 8px; max-width: 480px; margin-top: 6px; padding: 6px 10px;
  border-radius: 6px; background: var(--side); cursor: pointer; font-size: 13px; border: 1px solid transparent; }
.thread-sum:hover, .thread-sum.open { border-color: var(--line); background: var(--hover); }
.thread-sum .avatar { --size: 18px; font-size: 9px; margin-right: -4px; box-shadow: 0 0 0 2px var(--side); }
.thread-sum .count { color: var(--link); font-weight: 600; margin-left: 6px; }
.thread-sum .new { background: var(--red); color: #fff; border-radius: 8px; padding: 0 6px; font-size: 11px; font-weight: 700; }
.thread-sum .last { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.actions { position: absolute; right: 16px; top: -16px; display: none; padding: 2px; z-index: 2; background: var(--main);
  border: 1px solid var(--line); border-radius: 6px; box-shadow: 0 2px 6px rgba(0, 0, 0, .12); }
.msg:hover .actions { display: flex; }
.actions .ib { width: 32px; height: 28px; }
.actions .ic { width: 18px; height: 18px; }

/* compact message display */
.compact .msg { display: block; margin-top: 0; padding-top: 1px; padding-bottom: 1px; }
.compact .msg > .avatar, .compact .msg .reply::before { display: none; }
.compact .msg .reply { margin-bottom: 0; }
.compact .meta { display: inline-flex; margin-right: 6px; }
.compact .meta .time { font-size: 11px; min-width: 52px; }
.compact .text { display: inline; }
.compact .daydiv { margin-top: 14px; }

/* composer */
form { flex: none; position: relative; padding: 0 16px 6px; }
.box { display: flex; align-items: flex-start; background: var(--input); border-radius: 8px; padding-right: 16px; }
.box .attach { flex: none; width: 24px; height: 24px; margin: 10px 16px; padding: 0; display: grid; place-items: center;
  border: 0; border-radius: 50%; background: var(--muted); color: var(--input); cursor: pointer; }
.box .attach:hover { background: var(--text); }
.box .attach .ic { width: 16px; height: 16px; stroke-width: 3; }
textarea { flex: 1; border: 0; outline: 0; resize: none; background: transparent; color: var(--text); font: inherit;
  line-height: 22px; padding: 11px 0; max-height: 50vh; height: 44px; }
textarea::placeholder { color: var(--faint); }
.hint { font-size: 12px; color: var(--muted); padding: 5px 4px 0; min-height: 22px; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; }
.hint b { color: var(--text); font-weight: 600; }
.mentions { position: absolute; left: 16px; right: 16px; bottom: calc(100% - 4px); background: var(--card);
  border-radius: 8px; padding: 8px; box-shadow: var(--shadow); --ring: var(--card); z-index: 3; }
.mentions .head { font-size: 12px; font-weight: 700; text-transform: uppercase; color: var(--muted); padding: 2px 8px 6px; }
.mopt { display: flex; align-items: center; gap: 10px; padding: 6px 8px; border-radius: 4px; cursor: pointer; }
.mopt.on { background: var(--sel); }
.mopt .avatar { --size: 24px; }
.mopt .hash { width: 24px; display: grid; place-items: center; color: var(--muted); }
.mopt b { color: var(--strong); font-weight: 600; }
.mopt .sub { margin-left: auto; color: var(--muted); font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.pending { display: flex; flex-wrap: wrap; gap: 12px; }
.pending:not(:empty) { padding: 10px 0 12px; }
.pending .pi { position: relative; padding: 8px; border-radius: 8px; background: var(--input); }
.pending img { display: block; width: 96px; height: 96px; object-fit: cover; border-radius: 4px; }
.pending button { position: absolute; top: -8px; right: -8px; width: 24px; height: 24px; padding: 0; border: 0; border-radius: 50%;
  display: grid; place-items: center; background: var(--red); color: #fff; cursor: pointer; box-shadow: var(--shadow); }
.pending button .ic { width: 14px; height: 14px; }
.dragging { outline: 2px dashed var(--accent); outline-offset: -8px; }

/* side panes */
.pane { flex: none; display: flex; flex-direction: column; min-height: 0; background: var(--side); --ring: var(--side); }
#members { width: 240px; }
#profile { width: 340px; overflow-y: auto; }
#thread { width: 420px; background: var(--main); border-left: 1px solid var(--line); --ring: var(--main); }
#thread .bar .topic { color: var(--muted); }
.members { flex: 1; overflow-y: auto; padding: 0 8px 20px; }
.members .section { padding-top: 24px; }
.member { display: flex; align-items: center; gap: 12px; padding: 5px 8px; margin: 1px 0; border-radius: 4px; cursor: pointer; }
.member:hover { background: var(--hover); }
.member .label { flex: 1; min-width: 0; }
.member .name { font-weight: 500; }
.member.off { opacity: .45; }
.member.off:hover { opacity: 1; }

/* profile card */
.card { width: 300px; background: var(--card); border-radius: 8px; overflow: hidden; box-shadow: var(--shadow); --ring: var(--card); }
.card.docked { width: auto; border-radius: 0; box-shadow: none; background: transparent; --ring: var(--side); }
.card-banner { height: 60px; background: linear-gradient(135deg, hsl(var(--h) 55% 52%), hsl(calc(var(--h) + 40) 50% 34%)); }
.docked .card-banner { height: 120px; }
.card > .avatar.xl { --size: 80px; margin: -44px 0 0 16px; box-shadow: 0 0 0 6px var(--ring); }
.card > .avatar.xl .st { border-width: 5px; right: -2px; bottom: -2px; }
.card-body { padding: 10px 16px 16px; }
.card-name { display: flex; align-items: center; gap: 8px; font-size: 20px; font-weight: 700; color: var(--strong);
  overflow-wrap: anywhere; }
.card-status { display: flex; align-items: center; gap: 6px; margin-top: 2px; color: var(--muted); font-size: 13px; }
.card-box { margin-top: 12px; padding: 12px; border-radius: 8px; background: var(--box); }
.card.docked .card-box { background: var(--card); }
.card-box h4 { margin: 0 0 4px; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .02em;
  color: var(--strong); }
.card-box h4 + * { margin: 0; font-size: 14px; }
.card-box > * + h4 { margin-top: 12px; }
.mono { font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; color: var(--muted); overflow-wrap: anywhere; }
.setrow { display: flex; align-items: center; gap: 12px; }
.setrow .txt { flex: 1; min-width: 0; }
.setrow .txt b { display: block; font-size: 14px; font-weight: 600; color: var(--strong); }
.setrow .txt > span { display: block; font-size: 12px; color: var(--muted); }
.card-actions { display: flex; gap: 8px; margin-top: 12px; }
.btn { flex: 1; height: 34px; padding: 0 16px; display: inline-flex; align-items: center; justify-content: center; gap: 6px;
  border: 0; border-radius: 4px; background: var(--btn); color: var(--strong); font-size: 14px; font-weight: 500; cursor: pointer; }
.btn:hover { filter: brightness(1.08); }
.btn.primary { background: var(--accent); color: #fff; }
.btn .ic { width: 16px; height: 16px; }
.switch { flex: none; position: relative; width: 40px; height: 24px; padding: 0; border: 0; border-radius: 12px;
  background: var(--off); cursor: pointer; transition: background-color .15s; }
.switch::after { content: ""; position: absolute; top: 3px; left: 3px; width: 18px; height: 18px; border-radius: 50%;
  background: #fff; transition: transform .15s; }
.switch.on { background: var(--on); }
.switch.on::after { transform: translateX(16px); }
.switch:disabled { opacity: .5; cursor: not-allowed; }

/* popovers */
#popout, #pins { position: fixed; z-index: 20; }
.pins { width: 420px; max-height: 70vh; display: flex; flex-direction: column; overflow: hidden; background: var(--card);
  border-radius: 8px; box-shadow: var(--shadow); }
.pins-head { display: flex; align-items: center; gap: 8px; padding: 12px 16px; background: var(--panel); color: var(--strong);
  font-weight: 600; }
.pins-head span { color: var(--muted); font-weight: 400; font-size: 13px; }
.pins-list { overflow-y: auto; padding: 4px 12px 12px; }
.note { margin-top: 8px; padding: 10px 12px; border-radius: 6px; background: var(--main); border: 1px solid var(--line); }
.note b { color: var(--strong); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
.note pre { margin: 6px 0; white-space: pre-wrap; overflow-wrap: anywhere; font: 12.5px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; }
.note .by { font-size: 12px; color: var(--muted); }
.empty { padding: 28px 16px; text-align: center; color: var(--muted); font-size: 14px; }
.empty .ic { width: 40px; height: 40px; margin: 0 auto 10px; color: var(--faint); }

/* settings */
.modal { position: fixed; inset: 0; z-index: 30; display: grid; place-items: center; padding: 24px; background: rgba(0, 0, 0, .6); }
.sheet { width: min(920px, 100%); height: min(660px, 100%); display: flex; overflow: hidden; background: var(--main);
  border-radius: 10px; box-shadow: var(--shadow); --ring: var(--main); }
.set-nav { width: 220px; flex: none; padding: 24px 10px; background: var(--side); overflow-y: auto; }
.set-nav .section { padding: 0 10px 6px; }
.tab { display: flex; align-items: center; gap: 10px; width: 100%; padding: 7px 10px; margin-bottom: 2px; border: 0;
  border-radius: 4px; background: none; color: var(--muted); text-align: left; cursor: pointer; }
.tab .ic { width: 18px; height: 18px; }
.tab:hover { background: var(--hover); color: var(--text); }
.tab.on { background: var(--sel); color: var(--strong); }
.set-nav hr { border: 0; border-top: 1px solid var(--line); margin: 10px; }
.set-nav .ver { padding: 0 10px; font-size: 12px; color: var(--faint); }
.set-main { flex: 1; overflow-y: auto; padding: 32px 88px 40px 40px; position: relative; }
.set-main h2 { margin: 0 0 20px; font-size: 20px; color: var(--strong); }
.set-main h3 { margin: 28px 0 10px; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .02em; color: var(--muted); }
.set-main .lead { margin: -8px 0 12px; color: var(--muted); font-size: 14px; }
.set-close { position: absolute; top: 24px; right: 24px; display: flex; flex-direction: column; align-items: center; gap: 6px;
  color: var(--muted); font-size: 12px; font-weight: 600; }
.set-close .ib { width: 36px; height: 36px; border: 2px solid currentColor; border-radius: 50%; }
.choices { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; }
.choice { padding: 10px; border: 2px solid transparent; border-radius: 8px; background: var(--side); text-align: left; cursor: pointer; }
.choice:hover { border-color: var(--sel); }
.choice.on { border-color: var(--accent); }
.choice b { display: block; font-size: 14px; color: var(--strong); }
.choice span { display: block; font-size: 12px; color: var(--muted); }
.prev { height: 72px; margin-bottom: 10px; border-radius: 6px; overflow: hidden; display: flex; border: 1px solid var(--line); }
.prev i { display: block; }
.prev.dens { flex-direction: column; justify-content: center; gap: 8px; padding: 0 12px; background: var(--main); }
.pv-row { display: flex; align-items: center; gap: 8px; }
.pv-av { width: 20px; height: 20px; border-radius: 50%; background: var(--accent); flex: none; }
.pv-line { flex: 1; height: 7px; border-radius: 4px; background: var(--sel); }
.prev.compact-p { gap: 6px; }
.compact-p .pv-av { width: 18px; height: 5px; border-radius: 3px; background: var(--muted); }
.compact-p .pv-line { height: 5px; }
.setting { display: flex; align-items: center; gap: 16px; padding: 16px 0; border-bottom: 1px solid var(--line); }
.setting .txt { flex: 1; min-width: 0; }
.setting .txt b { display: block; font-size: 15px; font-weight: 500; color: var(--strong); }
.setting .txt > span { display: block; font-size: 13px; color: var(--muted); }
.setting .txt .mono { font-size: 12px; }
.radio { display: flex; align-items: center; gap: 12px; width: 100%; padding: 10px 12px; margin-bottom: 8px; border: 0;
  border-radius: 6px; background: var(--side); text-align: left; cursor: pointer; }
.radio:hover { background: var(--hover); }
.radio::before { content: ""; flex: none; width: 16px; height: 16px; border-radius: 50%; border: 2px solid var(--muted); }
.radio.on::before { border-color: var(--accent); background: var(--accent); box-shadow: inset 0 0 0 3px var(--side); }
.radio b { display: block; color: var(--strong); font-weight: 500; }
.radio span { display: block; font-size: 13px; color: var(--muted); }
.legend { display: grid; grid-template-columns: auto 1fr; gap: 8px 12px; align-items: center; font-size: 14px; }
.legend .dot { width: 10px; height: 10px; }
.legend span { color: var(--muted); }
.legend b { color: var(--strong); font-weight: 500; margin-right: 6px; }
.keys { display: grid; grid-template-columns: auto 1fr; gap: 10px 16px; align-items: center; font-size: 14px; }
kbd { display: inline-block; min-width: 24px; padding: 2px 6px; border-radius: 4px; background: var(--side);
  border-bottom: 2px solid var(--line); font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; text-align: center; color: var(--strong); }
.about-head { display: flex; align-items: center; gap: 16px; }
.about-head svg { width: 64px; height: 64px; border-radius: 18px; }
.about-head h2 { margin: 0; }
.about-head p { margin: 2px 0 0; color: var(--muted); }
.set-main a { color: var(--link); text-decoration: none; }
.set-main a:hover { text-decoration: underline; }

/* lightbox, toasts */
.lightbox { position: fixed; inset: 0; z-index: 40; display: flex; flex-direction: column; align-items: center;
  justify-content: center; gap: 12px; padding: 40px; background: rgba(0, 0, 0, .85); }
.lightbox img { max-width: 90vw; max-height: 80vh; border-radius: 4px; box-shadow: var(--shadow); }
.lightbox a { color: #fff; opacity: .75; font-size: 14px; text-decoration: none; }
.lightbox a:hover { opacity: 1; text-decoration: underline; }
#toasts { position: fixed; bottom: 96px; left: 50%; transform: translateX(-50%); z-index: 50; display: flex;
  flex-direction: column; align-items: center; gap: 8px; pointer-events: none; }
.toast { padding: 10px 16px; border-radius: 8px; background: var(--card); color: var(--strong); font-size: 14px;
  box-shadow: var(--shadow); transition: opacity .3s, transform .3s; }
.toast.bad { background: var(--red); color: #fff; }
.toast.out { opacity: 0; transform: translateY(6px); }

/* working: a session in the middle of a turn */
.st.busy, .dot.busy { background: var(--accent); animation: busy 1.6s ease-in-out infinite; }
@keyframes busy { 50% { opacity: .45; } }
.working { color: var(--accent); font-weight: 500; }
.typing { display: inline-flex; align-items: center; gap: 6px; color: var(--text); }
.typing b { font-weight: 700; color: var(--strong); }
.typing .act { color: var(--muted); overflow: hidden; text-overflow: ellipsis; }
.dots { display: inline-flex; gap: 3px; flex: none; }
.dots i { width: 6px; height: 6px; border-radius: 50%; background: var(--text); animation: dot 1.4s infinite ease-in-out; }
.dots i:nth-child(2) { animation-delay: .2s; }
.dots i:nth-child(3) { animation-delay: .4s; }
@keyframes dot { 0%, 70%, 100% { opacity: .25; transform: scale(.8); } 35% { opacity: 1; transform: none; } }
@media (prefers-reduced-motion: reduce) { .st.busy, .dot.busy, .dots i { animation: none; } }
.card-box .activity { font-style: italic; }

/* reactions, pins, unsend */
.reacts { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; margin-top: 4px; }
.react { display: inline-flex; align-items: center; gap: 6px; height: 24px; padding: 0 7px; border-radius: 8px;
  border: 1px solid transparent; background: var(--side); cursor: pointer; font-size: 14px; }
.react:hover { border-color: var(--sel); }
.react.mine { background: var(--mention); border-color: var(--accent); }
.react .n { font-size: 13px; font-weight: 600; color: var(--muted); }
.react.mine .n { color: var(--mention-text); }
.reacts .add-react { width: 28px; height: 24px; opacity: 0; }
.msg:hover .reacts .add-react { opacity: 1; }
.reacts .add-react .ic { width: 16px; height: 16px; }
.meta .pin-ic { color: var(--muted); width: 14px; height: 14px; align-self: center; }
.actions .ib.danger:hover, .actions .ib.armed { color: var(--red); }
.actions .ib.armed { background: var(--hover); }
#picker { position: fixed; z-index: 25; }
.picker { display: grid; grid-template-columns: repeat(6, 36px); gap: 2px; padding: 8px; background: var(--card);
  border-radius: 8px; box-shadow: var(--shadow); }
.picker button { width: 36px; height: 36px; border: 0; border-radius: 6px; background: none; font-size: 20px; cursor: pointer; }
.picker button:hover, .picker button.mine { background: var(--hover); }
.picker .head { grid-column: 1 / -1; font-size: 12px; font-weight: 700; text-transform: uppercase; color: var(--muted); padding: 0 4px 4px; }

/* richer Markdown */
.codewrap { position: relative; }
.codewrap .lang { position: absolute; right: 44px; top: 7px; font: 11px ui-monospace, SFMono-Regular, Menlo, monospace;
  color: var(--faint); pointer-events: none; }
.codewrap .codecopy { position: absolute; top: 4px; right: 4px; width: 28px; height: 28px; opacity: 0; background: var(--card); }
.codewrap:hover .codecopy { opacity: 1; }
.codewrap .codecopy .ic { width: 15px; height: 15px; }
.text .li { position: relative; padding-left: calc(18px + var(--depth, 0) * 18px); }
.text .li .mk { position: absolute; left: calc(var(--depth, 0) * 18px); width: 16px; color: var(--muted); }
.text .li .mk.num { font-variant-numeric: tabular-nums; font-size: 13px; line-height: 21px; }
.tablewrap { overflow-x: auto; margin: 6px 0; white-space: normal; }
.text table { border-collapse: collapse; font-size: 14px; }
.text th, .text td { border: 1px solid var(--line); padding: 4px 10px; text-align: left; vertical-align: top; white-space: pre-wrap; }
.text th { background: var(--side); color: var(--strong); font-weight: 600; }
.text a.msglink { color: var(--link); font-weight: 500; cursor: pointer; }
.text.clamp { max-height: 340px; overflow: hidden; -webkit-mask-image: linear-gradient(#000 75%, transparent);
  mask-image: linear-gradient(#000 75%, transparent); }
.more { display: inline-flex; align-items: center; gap: 4px; margin-top: 2px; padding: 0; border: 0; background: none;
  color: var(--link); font-size: 13px; font-weight: 600; cursor: pointer; }
.more:hover { text-decoration: underline; }
.more .ic { width: 14px; height: 14px; }
.more.open .ic { transform: rotate(180deg); }
mark { background: rgba(250, 166, 26, .35); color: inherit; border-radius: 2px; padding: 0 1px; }

/* header search, popover panels */
.search { position: relative; flex: none; margin-left: 4px; }
.search input { width: 150px; height: 26px; padding: 0 28px 0 8px; border: 0; border-radius: 4px; background: var(--box);
  color: var(--text); font-size: 14px; font-family: inherit; outline: 0; transition: width .15s; }
.search input:focus, .search.has input { width: 240px; }
.search .ic { position: absolute; right: 7px; top: 5px; width: 16px; height: 16px; color: var(--muted); pointer-events: none; }
.pins-head .grow { flex: 1; }
.pins-head .btn { flex: none; height: 28px; padding: 0 10px; font-size: 13px; }
.hit { display: grid; grid-template-columns: 32px minmax(0, 1fr); gap: 4px 10px; margin-top: 8px; padding: 10px 12px; border-radius: 6px;
  background: var(--main); border: 1px solid var(--line); cursor: pointer; --ring: var(--main); }
.hit:hover { border-color: var(--sel); }
.hit .avatar { --size: 32px; }
.hit .hm { display: flex; align-items: baseline; gap: 6px; min-width: 0; font-size: 13px; }
.hit .hm .name { font-size: 15px; }
.hit .where { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.hit .hb { grid-column: 2; font-size: 14px; white-space: pre-wrap; overflow-wrap: anywhere; display: -webkit-box;
  -webkit-line-clamp: 5; -webkit-box-orient: vertical; overflow: hidden; }
.hit .hx { margin-left: auto; }
.hit .hx .ib { width: 24px; height: 24px; }
.hit .hx .ic { width: 14px; height: 14px; }
.panel-note { padding: 8px 4px 0; font-size: 12px; color: var(--muted); }
.note .row { display: flex; align-items: center; gap: 4px; }
.note .row b { flex: 1; min-width: 0; overflow-wrap: anywhere; }
.note .row .ib { width: 26px; height: 26px; }
.note .row .ic { width: 15px; height: 15px; }
.note-form { margin-top: 8px; padding: 10px 12px; border-radius: 6px; background: var(--main); border: 1px solid var(--accent);
  display: flex; flex-direction: column; gap: 8px; }
.note-form input, .note-form textarea { width: 100%; padding: 6px 8px; border: 0; border-radius: 4px; background: var(--input);
  color: var(--text); font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; outline: 0; resize: vertical; }
.note-form textarea { min-height: 90px; height: auto; max-height: 40vh; }
.note-form .bts { display: flex; justify-content: flex-end; gap: 8px; }
.note-form .btn { flex: none; height: 30px; }

/* history, jump to present */
.older { display: flex; justify-content: center; padding: 16px 0 0; }
.older .btn { flex: none; }
.present { position: absolute; left: 16px; right: 16px; bottom: calc(100% - 8px); display: flex; align-items: center;
  gap: 8px; padding: 6px 12px 14px; border-radius: 8px 8px 0 0; background: var(--accent); color: #fff; font-size: 13px;
  font-weight: 500; border: 0; cursor: pointer; z-index: 2; }
.present b { margin-left: auto; display: inline-flex; align-items: center; gap: 4px; font-weight: 700; }
.present .ic { width: 16px; height: 16px; }

/* quick switcher */
.qs { width: min(560px, 100%); align-self: start; margin-top: 12vh; padding: 16px; border-radius: 8px; background: var(--side);
  box-shadow: var(--shadow); --ring: var(--side); }
.qs h3 { margin: 0 0 12px; font-size: 13px; color: var(--muted); font-weight: 500; }
.qs input { width: 100%; height: 56px; padding: 0 14px; border: 0; border-radius: 6px; background: var(--box); color: var(--strong);
  font-size: 20px; font-family: inherit; outline: 0; }
.qs-list { margin-top: 10px; max-height: 50vh; overflow-y: auto; }
.qs-item { display: flex; align-items: center; gap: 10px; padding: 7px 10px; border-radius: 4px; cursor: pointer; color: var(--muted); }
.qs-item .avatar { --size: 24px; }
.qs-item .hash { width: 24px; display: grid; place-items: center; }
.qs-item b { color: var(--strong); font-weight: 500; }
.qs-item .sub { margin-left: 4px; font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
.qs-item.on { background: var(--sel); }
.qs-tip { margin-top: 12px; font-size: 12px; color: var(--muted); }
.qs-tip b { color: var(--accent); }

@media (max-width: 1180px) { #members, #profile { display: none !important; } }
@media (max-width: 820px) {
  .rail { display: none; }
  #thread { position: fixed; inset: 0; width: auto; z-index: 10; }
  .sheet { flex-direction: column; }
  .set-nav { width: auto; display: flex; padding: 8px; gap: 4px; overflow-x: auto; }
  .set-nav .section, .set-nav hr, .set-nav .ver { display: none; }
  .tab { width: auto; flex: none; }
  .set-main { padding: 24px 16px; }
  .set-close { top: 12px; right: 12px; }
  .set-close span { display: none; }
}
@media (max-width: 700px) { .search { display: none; } }
@media (max-width: 600px) {
  .app { flex-direction: column; }
  .side { width: auto; max-height: 38vh; }
  .side-head { display: none; }
  .pins { width: calc(100vw - 16px); }
}
</style>
</head>
<body>
<div class="app">
  <nav class="rail">
    <div class="rail-item" title="Bridge" data-logo><span class="pill" id="rail-count" hidden></span></div>
    <div class="rail-sep"></div>
  </nav>
  <aside class="side">
    <div class="side-head">Bridge</div>
    <div class="side-scroll">
      <div id="channels"></div>
      <div class="section">Direct messages</div>
      <div id="dms"></div>
    </div>
    <div class="me">
      <button type="button" class="who" id="who" title="Settings"><span id="me-avatar"></span>
        <span class="who-txt"><b>You</b><span id="status">Connecting…</span></span></button>
      <button type="button" class="ib" id="mute"></button>
      <button type="button" class="ib" id="gear" title="Settings" aria-label="Settings"><i data-icon="gear"></i></button>
    </div>
  </aside>
  <main>
    <div class="banner" id="banner" hidden>
      <span>Turn on desktop notifications to hear when a session replies.</span>
      <button type="button" class="cta" id="banner-on">Turn on</button>
      <button type="button" class="ib" id="banner-x" title="Dismiss" aria-label="Dismiss"><i data-icon="x"></i></button>
    </div>
    <div class="body">
      <div class="center">
        <header class="bar"><span class="bar-icon" id="bar-icon"></span><span class="title" id="title"></span>
          <span class="topic" id="topic"></span><div class="tools" id="tools"></div>
          <label class="search pins-btn" id="search-box"><input id="search" placeholder="Search" aria-label="Search messages"
            autocomplete="off"><i data-icon="search"></i></label></header>
        <div class="center-row">
          <div class="chat">
            <div class="log" id="log"></div>
            <form id="compose">
              <button type="button" class="present" id="present" hidden>You're viewing older messages
                <b>Jump to present <i data-icon="down"></i></b></button>
              <div class="mentions" hidden></div>
              <div class="pending"></div>
              <div class="box"><button type="button" class="attach" title="Attach images (or paste or drop them)"
                aria-label="Attach images"><i data-icon="plus"></i></button>
                <textarea rows="1"></textarea><input type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden></div>
              <div class="hint"></div>
            </form>
          </div>
          <section class="pane" id="members" hidden></section>
          <section class="pane" id="profile" hidden></section>
        </div>
      </div>
      <section class="pane" id="thread" hidden>
        <header class="bar"><span class="bar-icon"><i data-icon="thread"></i></span><span class="title">Thread</span>
          <span class="topic"># everyone</span><div class="tools">
          <button type="button" class="ib" id="close-thread" title="Close thread" aria-label="Close thread"><i data-icon="x"></i></button></div></header>
        <div class="log" id="tlog"></div>
        <form id="tcompose">
          <div class="mentions" hidden></div>
          <div class="pending"></div>
          <div class="box"><button type="button" class="attach" title="Attach images (or paste or drop them)"
            aria-label="Attach images"><i data-icon="plus"></i></button>
            <textarea rows="1" placeholder="Reply in thread"></textarea><input type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden></div>
          <div class="hint"></div>
        </form>
      </section>
    </div>
  </main>
</div>
<div id="popout" hidden></div>
<div id="pins" hidden></div>
<div id="picker" hidden></div>
<div class="modal" id="switcher" hidden>
  <div class="qs" role="dialog" aria-modal="true" aria-label="Quick switcher">
    <h3>Jump to a conversation</h3>
    <input placeholder="Where would you like to go?" autocomplete="off">
    <div class="qs-list"></div>
    <div class="qs-tip"><b>Tip:</b> ↑ ↓ to pick, Enter to go, Esc to close. Alt + ↑ ↓ moves between conversations anywhere.</div>
  </div>
</div>
<div class="modal" id="settings-modal" hidden>
  <div class="sheet" role="dialog" aria-modal="true" aria-label="Settings">
    <nav class="set-nav">
      <div class="section">Settings</div>
      <button type="button" class="tab" data-tab="appearance"><i data-icon="eye"></i>Appearance</button>
      <button type="button" class="tab" data-tab="notifications"><i data-icon="bell"></i>Notifications</button>
      <button type="button" class="tab" data-tab="sessions"><i data-icon="users"></i>Sessions</button>
      <hr>
      <button type="button" class="tab" data-tab="about"><i data-icon="info"></i>About Bridge</button>
    </nav>
    <div class="set-main">
      <div id="set-body"></div>
      <div class="set-close"><button type="button" class="ib" id="set-close" aria-label="Close settings"><i data-icon="x"></i></button><span>ESC</span></div>
    </div>
  </div>
</div>
<div class="lightbox" id="lightbox" hidden><img alt=""><a target="_blank" rel="noopener">Open original</a></div>
<div id="toasts" aria-live="polite"></div>
<script>
const TOKEN = "__TOKEN__";
const CHANNEL = "#everyone";
const $ = id => document.getElementById(id);
let since = 0, view = CHANNEL, thread = null, peers = [], messages = [], notes = [];
let unread = new Set(), skew = 0, notified = 0, drawn = "", connected = false, newMark = null;
// reactions by message row id, pinned row ids (-> when), unsent row ids, the oldest message there is
let reacts = new Map(), pins = new Map(), gone = new Set(), oldest = 0, extraSig = "";
const expanded = new Set();  // long posts you opened up
let drafts = {};  // unsent text per conversation ("t<root id>" for a thread)
try { drafts = JSON.parse(localStorage.getItem("bridge.drafts")) || {}; } catch (e) {}
const imgCache = new Map();  // reuse <img> elements across redraws so images don't flash
let lastRead = {};  // view or "t<thread root id>" -> highest message id seen there (this browser only)
try { lastRead = JSON.parse(localStorage.getItem("bridge.lastRead")) || {}; } catch (e) {}
const firstVisit = !Object.keys(lastRead).length;
// Per-browser preferences. Which sessions get posts to everyone is shared, so it lives on the server.
const settings = {theme: "system", density: "cozy", desktop: true, sound: true, notifyAll: false, muted: false,
                  members: true, profile: true, banner: true, collapse: true};
try { Object.assign(settings, JSON.parse(localStorage.getItem("bridge.settings"))); } catch (e) {}
{
  const h = decodeURIComponent(location.hash.slice(1));
  if (h && !h.startsWith("everyone")) view = h;
  else if (h.startsWith("everyone/")) thread = parseInt(h.split("/")[1], 10) || null;
}

const ICONS = {
  hash: "M4 9h16M4 15h16M10 3 8 21M16 3l-2 18",
  bell: "M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.94 1.94 0 0 0 3.4 0",
  "bell-off": "M8.7 3A6 6 0 0 1 18 8a21.3 21.3 0 0 0 .6 5M17 17H3s3-2 3-9a4.67 4.67 0 0 1 .3-1.7M10.3 21a1.94 1.94 0 0 0 3.4 0M2 2l20 20",
  notes: "M15.5 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V8.5L15.5 3zM15 3v5a1 1 0 0 0 1 1h5M8 13h8M8 17h5",
  users: "M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75",
  user: "M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2M12 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z",
  gear: "M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2zM12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z",
  x: "M18 6 6 18M6 6l12 12",
  plus: "M12 5v14M5 12h14",
  thread: "M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z",
  copy: "M10 8h10a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H10a2 2 0 0 1-2-2V10a2 2 0 0 1 2-2zM4 16a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2",
  clock: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 6v6l4 2",
  at: "M12 16a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM16 8v5a3 3 0 0 0 6 0v-1a10 10 0 1 0-4 8",
  folder: "M4 20h16a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2z",
  eye: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z",
  info: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 16v-4M12 8h.01",
  pin: "M12 17v5M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z",
  smile: "M22 11v1a10 10 0 1 1-9-10M8 14s1.5 2 4 2 4-2 4-2M9 9h.01M15 9h.01M16 5h6M19 2v6",
  search: "M11 19a8 8 0 1 0 0-16 8 8 0 0 0 0 16zM21 21l-4.3-4.3",
  trash: "M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2",
  pencil: "M21.17 6.81a1 1 0 0 0-3.99-3.99L3.84 16.17a2 2 0 0 0-.5.83l-1.32 4.35a.5.5 0 0 0 .62.62l4.35-1.32a2 2 0 0 0 .83-.5zM15 5l4 4",
  check: "M20 6 9 17l-5-5",
  down: "M12 5v14M19 12l-7 7-7-7",
  chevron: "m6 9 6 6 6-6",
};
const LOGO = '<svg viewBox="0 0 128 128" aria-hidden="true"><defs><linearGradient id="bridge-g" x1="0" y1="0" x2="1" y2="1">'
  + '<stop offset="0" stop-color="#8b5cf6"/><stop offset="1" stop-color="#5b21b6"/></linearGradient></defs>'
  + '<rect width="128" height="128" fill="url(#bridge-g)"/><path d="M22 84 Q64 26 106 84" fill="none" stroke="#fff" stroke-width="8" stroke-linecap="round"/>'
  + '<path d="M14 84 H114" stroke="#fff" stroke-width="8" stroke-linecap="round"/><path d="M43 84 V60 M64 84 V52 M85 84 V60" stroke="#fff" stroke-width="5" stroke-linecap="round" opacity=".85"/>'
  + '<rect x="15" y="76" width="14" height="30" rx="3" fill="#fff"/><rect x="99" y="76" width="14" height="30" rx="3" fill="#fff"/></svg>';
function logo() {
  const t = document.createElement("template");
  t.innerHTML = LOGO;
  return t.content.firstChild;
}
function icon(name, size = 20) {
  const ns = "http://www.w3.org/2000/svg", s = document.createElementNS(ns, "svg"), p = document.createElementNS(ns, "path");
  s.setAttribute("viewBox", "0 0 24 24");
  s.setAttribute("width", size);
  s.setAttribute("height", size);
  s.setAttribute("class", "ic");
  s.setAttribute("aria-hidden", "true");
  p.setAttribute("d", ICONS[name]);
  s.append(p);
  return s;
}
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
function iconBtn(name, title, onclick, cls) {
  const b = el("button", "ib" + (cls ? " " + cls : ""));
  b.type = "button";
  b.title = title;
  b.setAttribute("aria-label", title);
  b.append(icon(name));
  b.onclick = onclick;
  return b;
}
function toggle(checked, onchange, label, disabled) {
  const b = el("button", "switch" + (checked ? " on" : ""));
  b.type = "button";
  b.setAttribute("role", "switch");
  b.setAttribute("aria-checked", checked);
  b.setAttribute("aria-label", label);
  b.disabled = !!disabled;
  b.onclick = () => onchange(!checked);
  return b;
}
function toast(text, bad) {
  const t = el("div", "toast" + (bad ? " bad" : ""), text);
  $("toasts").append(t);
  setTimeout(() => t.classList.add("out"), 2600);
  setTimeout(() => t.remove(), 3000);
}
const serverNow = () => Date.now() / 1000 - skew;
const clock = ts => new Date(ts * 1000).toLocaleTimeString([], {hour: "numeric", minute: "2-digit"});
const dayOf = ts => new Date(ts * 1000).toDateString();
function when(ts) {
  const d = new Date(ts * 1000), now = new Date(serverNow() * 1000);
  if (d.toDateString() === now.toDateString()) return "Today at " + clock(ts);
  const y = new Date(now);
  y.setDate(now.getDate() - 1);
  if (d.toDateString() === y.toDateString()) return "Yesterday at " + clock(ts);
  return d.toLocaleDateString() + " " + clock(ts);
}
function ago(ts) {
  const s = Math.max(0, Math.floor(serverNow() - ts));
  if (s < 60) return "just now";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}
function dur(ts) {
  const s = Math.max(0, Math.floor(serverNow() - ts));
  if (s < 60) return "under a minute";
  if (s < 3600) return Math.floor(s / 60) + "m";
  return Math.floor(s / 3600) + "h " + Math.floor(s % 3600 / 60) + "m";
}
const display = name => name === "human" ? "You" : name;
const hue = name => { let h = 0; for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360; return h; };
const peerOf = name => peers.find(p => p.name === name);
// Online: its listener is running, so a message wakes it. Idle: seen lately, but messages wait for its next prompt.
// Working: in the middle of a turn, so a new message waits until the turn ends.
function presence(p) {
  if (!p) return "off";
  if (p.busy_since) return "busy";
  if (p.listening) return "on";
  return serverNow() - p.last_seen < 900 ? "idle" : "off";
}
const PRESENCE = {busy: ["Working", "In the middle of a turn: it picks up new messages when the turn ends"],
                  on: ["Online", "Listening: a message wakes it right away"],
                  idle: ["Idle", "Not listening: messages wait for its next prompt"],
                  off: ["Offline", "Not seen in the last 15 minutes"]};
function avatar(name, withStatus) {
  const a = el("div", "avatar" + (name === "human" ? " me" : ""), name === "human" ? "Y" : name[0]);
  a.style.setProperty("--h", hue(name));
  if (withStatus) {
    const s = name === "human" ? (connected ? "on" : "off") : presence(peerOf(name));
    const st = el("span", "st " + s);
    st.title = name === "human" ? (connected ? "Connected" : "Not connected") : PRESENCE[s].join(": ");
    a.append(st);
  }
  return a;
}
function clickable(node, name) {
  if (name === "human") return node;
  node.classList.add(node.classList.contains("avatar") ? "click" : "peer");
  node.onclick = ev => { ev.stopPropagation(); showPopout(name, node); };
  return node;
}
function nameEl(name) {
  const s = el("span", "name", display(name));
  s.style.setProperty("--h", hue(name));
  return clickable(s, name);
}
const aiTag = () => el("span", "tag", "AI");
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
function saveDrafts() { try { localStorage.setItem("bridge.drafts", JSON.stringify(drafts)); } catch (e) {} }
function saveSettings() { try { localStorage.setItem("bridge.settings", JSON.stringify(settings)); } catch (e) {} }
function applyAppearance() {
  if (settings.theme === "system") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = settings.theme;
  document.body.classList.toggle("compact", settings.density === "compact");
}

let audio = null;
document.addEventListener("pointerdown", () => {
  // browsers only allow sound after the page has been interacted with
  if (!audio) try { audio = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) {}
}, {once: true});
function ping() {
  if (!audio) return;
  const t = audio.currentTime, o = audio.createOscillator(), g = audio.createGain();
  o.type = "sine";
  o.frequency.setValueAtTime(880, t);
  o.frequency.setValueAtTime(1320, t + .09);
  g.gain.setValueAtTime(.0001, t);
  g.gain.exponentialRampToValueAtTime(.12, t + .01);
  g.gain.exponentialRampToValueAtTime(.0001, t + .4);
  o.connect(g).connect(audio.destination);
  o.start(t);
  o.stop(t + .45);
}

// A message to several recipients is stored as one row each; fold those back into one post.
function groups() {
  const out = [];
  for (const m of messages) {
    if (gone.has(m.id)) continue;
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
function mentionEl(name) {
  const s = el("span", "mention", "@" + name);
  return name === "everyone" || name === "You" ? s : clickable(s, name);
}
// The Markdown sessions write most: ```code``` (with a copy button), `code`, **bold**, # headings, > quotes,
// - lists, | tables |, links, @mentions and #123 links to other messages.
const INLINE = /(`[^`\n]+`)|(\*\*[^*\n]+?\*\*)|(https?:\/\/[^\s<>"']*[^\s<>"'.,;:!?)\]])|@([\w-]+(?:\.[\w-]+)*)|(?<![\w&#\/])#(\d{1,9})\b/g;
function inline(text, names) {
  const out = [];
  let last = 0;
  for (const m of text.matchAll(INLINE)) {
    let node;
    if (m[1]) node = el("code", null, m[1].slice(1, -1));
    else if (m[2]) node = el("strong", null, m[2].slice(2, -2));
    else if (m[3]) {
      node = el("a", null, m[3]);
      node.href = m[3];
      node.target = "_blank";
      node.rel = "noopener noreferrer";
    } else if (m[4]) {
      if (!names.has(m[4])) continue;
      node = mentionEl(m[4]);
    } else {
      const id = +m[5];
      if (id < oldest || id > since) continue;  // not a message here
      node = el("a", "msglink", "#" + id);
      node.title = "Jump to message #" + id;
      node.onclick = () => jump(id);
    }
    out.push(text.slice(last, m.index), node);
    last = m.index + m[0].length;
  }
  out.push(text.slice(last));
  return out;
}
function codeBlock(lang, code) {
  const w = el("div", "codewrap");
  w.append(el("pre", null, code));
  if (lang) w.append(el("span", "lang", lang));
  w.append(iconBtn("copy", "Copy code", () => copy(code), "codecopy"));
  return w;
}
// a table row's cells; \| is a literal pipe inside a cell
const cells = line => line.trim().replace(/^\||(?<!\\)\|$/g, "").split(/(?<!\\)\|/).map(c => c.trim().replace(/\\\|/g, "|"));
function table(rows, names) {
  const t = el("table"), w = el("div", "tablewrap");
  rows.forEach((r, i) => {
    const tr = el("tr");
    for (const c of cells(r)) {
      const cell = el(i ? "td" : "th");
      cell.append(...inline(c, names));
      tr.append(cell);
    }
    t.append(tr);
  });
  w.append(t);
  return w;
}
function rich(body) {
  const names = new Set([...peers.map(p => p.name), "everyone"]);
  const parts = body.split(/```([\w+-]*)[^\S\n]*\n?([\s\S]*?)```/), out = [];
  for (let i = 0; i < parts.length; i += 3) {
    let part = parts[i];
    if (i > 0) part = part.replace(/^\n/, "");
    if (i < parts.length - 1) part = part.replace(/\n$/, "");
    let buf = [];
    const flush = () => { if (buf.length) out.push(...inline(buf.join("\n"), names)); buf = []; };
    const lines = part.split("\n");
    for (let j = 0; j < lines.length; j++) {
      const line = lines[j];
      if (/^\s*\|.*\|\s*$/.test(line) && /^\s*\|?\s*:?-{3,}/.test(lines[j + 1] || "")) {
        flush();
        const rows = [line];
        for (j += 2; j < lines.length && /^\s*\|/.test(lines[j]); j++) rows.push(lines[j]);
        j--;
        out.push(table(rows, names));
        continue;
      }
      const h = line.match(/^(#{1,3})\s+(.*)$/), q = line.match(/^>\s?(.*)$/);
      const li = line.match(/^(\s*)([-*•]|\d{1,3}[.)])\s+(.*)$/);
      if (!h && !q && !li) { buf.push(line); continue; }
      flush();
      if (li) {
        const d = el("div", "li"), num = /\d/.test(li[2]);
        d.style.setProperty("--depth", Math.min(4, Math.floor(li[1].replace(/\t/g, "  ").length / 2)));
        d.append(el("span", "mk" + (num ? " num" : ""), num ? li[2] : "•"), ...inline(li[3], names));
        out.push(d);
        continue;
      }
      const d = el("div", h ? "h" + h[1].length : "quote");
      d.append(...inline(h ? h[2] : q[1], names));
      out.push(d);
    }
    flush();
    if (i + 2 < parts.length) out.push(codeBlock(parts[i + 1], parts[i + 2].replace(/\n$/, "")));
  }
  return out;
}
// who a channel post is for, minus anyone its text already @mentions
function targets(g) {
  if (g.rows[0].recipient === "human") return ["You"];
  const to = new Set(g.rows.map(r => r.recipient));
  const all = peers.filter(p => p.name !== g.sender && !p.quiet).every(p => to.has(p.name));
  const said = new Set(mentionsIn(g.body));
  if (all && g.everyone) return said.size ? [] : ["everyone"];
  return [...to].filter(n => !said.has(n));
}

function sidebarItem(v, iconNode, label, sub, count, extra) {
  const d = el("div", "item" + (v === CHANNEL ? " ch" : "") + (view === v ? " active" : "") + (count ? " unread" : ""));
  const l = el("div", "label");
  l.append(el("div", null, label));
  if (sub) {
    const d = el("div", "sub");
    d.append(sub);
    l.append(d);
  }
  d.append(iconNode, l, ...(extra || []));
  if (count) d.append(el("span", "pill", count > 99 ? "99+" : String(count)));
  d.onclick = () => setView(v);
  return d;
}
function quietIcon(p) {
  const i = icon("bell-off", 14);
  i.classList.add("quiet-ic");
  i.setAttribute("aria-hidden", "false");
  const t = document.createElementNS("http://www.w3.org/2000/svg", "title");
  t.textContent = p.name + " skips posts to everyone; @mention it to reach it";
  i.prepend(t);
  return i;
}
function renderSidebar(M) {
  const chCount = unreadChannel(M);
  $("channels").replaceChildren(sidebarItem(CHANNEL, icon("hash"), "everyone", null, chCount));
  let replies = 0;
  $("dms").replaceChildren(...peers.map(p => {
    const n = view === p.name ? 0 : unreadDM(M, p.name);
    replies += n;
    const sub = p.busy_since ? el("span", "working", "Working…") : p.summary || PRESENCE[presence(p)][0];
    const item = sidebarItem(p.name, avatar(p.name, true), p.name, sub, n, p.quiet ? [quietIcon(p)] : []);
    item.title = p.cwd + (p.summary ? "\n" + p.summary : "");
    return item;
  }));
  if (!peers.length) $("dms").append(el("div", "item", "No sessions yet"));
  const total = replies + chCount;
  document.title = total ? `(${total}) Bridge` : "Bridge";
  $("rail-count").hidden = !total;
  $("rail-count").textContent = total > 99 ? "99+" : total;
}
function renderMe() {
  $("me-avatar").replaceChildren(avatar("human", true));
  $("status").textContent = connected ? "Connected" : "Reconnecting…";
  $("who").title = (connected ? peers.length + (peers.length === 1 ? " session" : " sessions") + " · " : "") + "Settings";
  const m = $("mute");
  m.replaceChildren(icon(settings.muted ? "bell-off" : "bell"));
  m.classList.toggle("alert", settings.muted);
  m.title = settings.muted ? "Notifications muted: click to unmute" : "Mute notifications";
  m.setAttribute("aria-label", m.title);
}
// "web is working…" under a send box, for the sessions in that conversation that are mid-turn
const working = names => names.filter(n => peerOf(n) && peerOf(n).busy_since);
const andList = names => names.length < 3 ? names.join(" and ") : names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
function setHint(form, text, busy) {
  const hint = form.querySelector(".hint");
  if (!busy.length) { hint.textContent = text; hint.title = ""; return; }
  const t = el("span", "typing"), dots = el("span", "dots");
  dots.append(el("i"), el("i"), el("i"));
  const who = busy.length > 3 ? [el("b", null, busy.length + " sessions"), " are working…"]
    : [...busy.flatMap((n, i) => [i ? (i === busy.length - 1 ? " and " : ", ") : "", el("b", null, n)]),
       busy.length === 1 ? " is working…" : " are working…"];
  t.append(dots, ...who);
  const p = busy.length === 1 && peerOf(busy[0]);
  if (p && p.activity) t.append(el("span", "act", " " + p.activity));
  hint.replaceChildren(t);
  hint.title = busy.map(n => n + ": " + (peerOf(n).activity || "working") + " (for " + dur(peerOf(n).busy_since) + ")").join("\n");
}
function renderBanner() {
  $("banner").hidden = !(settings.banner && settings.desktop && "Notification" in window && Notification.permission === "default");
}

// One message. opts: {M, log, stick, cont, chips, preview, receipt (say who's seen it),
// threadKey (adds the thread summary and action)}
function messageEl(g, opts) {
  const d = el("div", "msg" + (opts.cont ? " cont" : ""));
  d.dataset.ids = " " + g.rows.map(r => r.id).join(" ") + " ";
  const ref = opts.preview && g.reply_to && opts.M.byRow.get(g.reply_to);
  if (ref) {
    const r = el("div", "reply");
    r.append(avatar(ref.sender), nameEl(ref.sender), el("span", "rtext", ref.body.replace(/\s+/g, " ") || "Image"));
    r.onclick = () => jump(g.reply_to);
    d.append(r);
  }
  d.append(clickable(avatar(g.sender), g.sender));
  if (opts.cont) d.append(el("span", "htime", clock(g.created)));
  const c = el("div", "content"), pinned = isPinned(g);
  if (!opts.cont) {
    const meta = el("div", "meta"), t = el("span", "time", settings.density === "compact" ? clock(g.created) : when(g.created));
    t.title = new Date(g.created * 1000).toLocaleString() + " · #" + g.key;
    meta.append(nameEl(g.sender));
    if (g.sender !== "human") meta.append(aiTag());
    if (settings.density === "compact") meta.prepend(t); else meta.append(t);
    if (pinned) meta.append(pinIcon());
    c.append(meta);
  } else if (pinned) d.querySelector(".htime").after(pinIcon());
  const text = el("div", "text");
  for (const t of opts.chips ? targets(g) : []) text.append(mentionEl(t), " ");
  text.append(...rich(g.body));
  c.append(text);
  const lines = g.body.split("\n").length;
  if (settings.collapse && (g.body.length > 1800 || lines > 28)) {
    const open = expanded.has(g.key), more = el("button", "more" + (open ? " open" : ""));
    text.classList.toggle("clamp", !open);
    more.type = "button";
    more.append(open ? "Show less" : "Show more (" + lines + " lines)", icon("chevron"));
    more.onclick = () => { open ? expanded.delete(g.key) : expanded.add(g.key); render(); };
    c.append(more);
  }
  if (g.attachments.length) {
    const atts = el("div", "atts");
    for (const file of g.attachments) {
      const url = "/files/" + encodeURIComponent(file) + "?t=" + encodeURIComponent(TOKEN);
      const label = file.replace(/^[0-9a-f]{12}-/, "");
      const a = el("a");
      a.href = url;
      a.target = "_blank";
      a.onclick = ev => { if (!ev.metaKey && !ev.ctrlKey) { ev.preventDefault(); openLightbox(url, label); } };
      const key = opts.log.id + "|" + url;
      let img = imgCache.get(key);
      if (!img) {
        img = el("img");
        img.alt = img.title = label;
        img.onload = () => { if (opts.stick) opts.log.scrollTop = opts.log.scrollHeight; };
        img.src = url;
        imgCache.set(key, img);
      }
      a.append(img);
      atts.append(a);
    }
    c.append(atts);
  }
  const rs = reactionsOf(g);
  if (rs.size) {
    const box = el("div", "reacts");
    for (const [emoji, who] of rs) {
      const mine = who.has("human"), b = el("button", "react" + (mine ? " mine" : ""));
      b.type = "button";
      b.title = andList([...who].map(display)) + " reacted with " + emoji;
      b.append(el("span", null, emoji), el("span", "n", who.size));
      b.onclick = () => react(g, emoji, !mine);
      box.append(b);
    }
    box.append(iconBtn("smile", "Add reaction", ev => openPicker(g, ev.currentTarget), "add-react"));
    c.append(box);
  }
  const st = delivery(g, opts.receipt);
  if (st) c.append(st);
  const acts = el("div", "actions");
  acts.append(iconBtn("smile", "Add reaction", ev => openPicker(g, ev.currentTarget)));
  if (opts.threadKey) {
    const reps = opts.M.threads.get(opts.threadKey) || [];
    if (reps.length) {
      const sum = el("div", "thread-sum" + (thread === opts.threadKey ? " open" : ""));
      const who = [...new Set(reps.map(r => r.sender))].slice(0, 4);
      sum.append(...who.map(n => avatar(n)), el("span", "count", reps.length + (reps.length === 1 ? " reply" : " replies")));
      const n = unreadThread(opts.M, opts.threadKey);
      if (n && thread !== opts.threadKey) sum.append(el("span", "new", n + " new"));
      sum.append(el("span", "last", ago(reps[reps.length - 1].created)));
      sum.onclick = () => openThread(opts.threadKey);
      c.append(sum);
    }
    acts.append(iconBtn("thread", "Reply in thread", () => openThread(opts.threadKey)));
  }
  acts.append(iconBtn("pin", pinned ? "Unpin" : "Pin message", () => pin(g, !pinned)));
  if (g.body) acts.append(iconBtn("copy", "Copy text", () => copy(g.body)));
  if (g.sender === "human" && g.rows.every(r => unread.has(r.id))) {
    acts.append(iconBtn("trash", "Unsend (no session has read it yet)", ev => unsend(g, ev.currentTarget), "danger"));
  }
  d.append(acts, c);
  return d;
}
function pinIcon() {
  const i = icon("pin", 14);
  i.classList.add("pin-ic");
  i.setAttribute("aria-label", "Pinned");
  return i;
}
// Whether the sessions have picked it up: "waiting" while any hasn't, and on your latest post, who's seen it.
function delivery(g, receipt) {
  const to = g.rows.filter(r => r.recipient !== "human").map(r => r.recipient);
  if (!to.length) return null;
  const waiting = g.rows.filter(r => r.recipient !== "human" && unread.has(r.id)).map(r => r.recipient);
  const s = el("div", "state");
  if (waiting.length) {
    const p = waiting.length === 1 && peerOf(waiting[0]);
    const why = !p ? "" : p.busy_since ? " · it's mid-turn and will see it when that turn ends"
      : p.listening ? "" : " · it isn't listening, so it'll see it on its next prompt";
    s.append(icon("clock"), "Waiting for " + andList(waiting) + " to pick this up" + why);
    return s;
  }
  if (!receipt || g.sender !== "human") return null;
  const busy = working(to);
  s.append(icon("check"), (to.length > 3 ? "Seen by " + to.length + " sessions" : "Seen by " + andList(to))
           + (busy.length ? " · " + andList(busy) + (busy.length > 1 ? " are" : " is") + " working" : ""));
  return s;
}
function reactionsOf(g) {
  const by = new Map();
  for (const r of g.rows) for (const x of reacts.get(r.id) || []) {
    if (!by.has(x.emoji)) by.set(x.emoji, new Set());
    by.get(x.emoji).add(x.who);
  }
  return by;
}
const isPinned = g => g.rows.some(r => pins.has(r.id));
const idsOf = g => g.rows.map(r => r.id);
async function react(g, emoji, on) {
  try { await api("/api/react", {ids: idsOf(g), emoji, on}); poll(); }
  catch (e) { toast("Couldn't react: " + e.message, true); }
}
async function pin(g, pinned) {
  try { await api("/api/pin", {ids: idsOf(g), pinned}); toast(pinned ? "Pinned" : "Unpinned"); poll(); }
  catch (e) { toast("Couldn't pin it: " + e.message, true); }
}
// The first click arms the button, a second one within a few seconds unsends.
async function unsend(g, btn) {
  if (!btn.classList.contains("armed")) {
    btn.classList.add("armed");
    btn.title = "Click again to unsend";
    toast("Click the bin again to unsend it");
    setTimeout(() => btn.classList.remove("armed"), 4000);
    return;
  }
  try { await api("/api/unsend", {ids: idsOf(g)}); toast("Message unsent"); poll(); }
  catch (e) { toast(e.message, true); }
}
const QUICK = ["👍", "✅", "👀", "❤️", "🎉", "🚀", "😂", "🤔", "🙏", "🔥", "⚠️", "❌"];
let pickFor = null;
function openPicker(g, anchor) {
  const box = el("div", "picker"), mine = reactionsOf(g);
  box.append(el("div", "head", "React"));
  for (const e of QUICK) {
    const on = mine.has(e) && mine.get(e).has("human"), b = el("button", on ? "mine" : null, e);
    b.type = "button";
    b.title = on ? "Remove your " + e : "React with " + e;
    b.onclick = () => { closePicker(); react(g, e, !on); };
    box.append(b);
  }
  const p = $("picker");
  pickFor = g.key;
  p.replaceChildren(box);
  p.hidden = false;
  let r = anchor.getBoundingClientRect();
  if (!r.width) r = anchor.closest(".msg").getBoundingClientRect();  // the hover bar had already hidden
  p.style.left = Math.max(8, Math.min(r.right - p.offsetWidth, innerWidth - p.offsetWidth - 8)) + "px";
  p.style.top = (r.top - p.offsetHeight - 6 > 8 ? r.top - p.offsetHeight - 6 : r.bottom + 6) + "px";
}
function closePicker() { $("picker").hidden = true; pickFor = null; }
async function copy(text) {
  try { await navigator.clipboard.writeText(text); toast("Copied to clipboard"); }
  catch (e) { toast("Couldn't copy: " + e.message, true); }
}
function dayDivider(ts) {
  const d = el("div", "daydiv");
  d.append(el("span", null, new Date(ts * 1000).toLocaleDateString([], {month: "long", day: "numeric", year: "numeric"})));
  return d;
}

// opts: {M, view, chips, preview, mark (draw the NEW line above the first message after this id), after (message shown above)}
function renderList(log, items, opts, head) {
  const stick = opts.stick ?? log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  const kids = [...head], compact = settings.density === "compact";
  let prev = null, lastDay = opts.after ? dayOf(opts.after.created) : null, marked = false;
  const mine = [...items].reverse().find(g => g.sender === "human");  // your latest post says who's seen it
  for (const g of items) {
    const newDay = dayOf(g.created) !== lastDay;
    if (newDay) kids.push(dayDivider(g.created));
    lastDay = dayOf(g.created);
    const isNew = opts.mark != null && !marked && fresh(g, opts.mark);
    if (isNew) {
      const n = el("div", "newdiv");
      n.append(el("span", null, "NEW"));
      kids.push(n);
      marked = true;
    }
    const cont = !compact && !newDay && !isNew && prev && prev.sender === g.sender && !(opts.preview && g.reply_to)
                 && g.created - prev.created < 300 && opts.view !== CHANNEL;
    kids.push(messageEl(g, {...opts, log, stick, cont, receipt: g === mine, threadKey: opts.view === CHANNEL ? g.key : null}));
    prev = g;
  }
  log.replaceChildren(...kids);
  if (stick) log.scrollTop = log.scrollHeight;
}

function renderMain(M) {
  const dm = view !== CHANNEL, peer = peerOf(view);
  const intro = el("div", "intro");
  if (dm) {
    const a = avatar(view, true);
    a.classList.add("xl");
    const p = el("p");
    p.append("This is the beginning of your private conversation with ", el("b", null, view), ". Its answers to you land here; when it needs another session, it asks in #everyone.");
    const chips = el("div", "chips");
    if (peer) {
      const f = el("span", "chip");
      f.append(icon("folder"), peer.cwd);
      chips.append(f);
    }
    intro.append(a, el("h2", null, view), p, chips);
  } else {
    const big = el("div", "big");
    big.append(icon("hash", 40));
    intro.append(big, el("h2", null, "Welcome to #everyone"),
                 el("p", null, "Where the sessions talk to each other, and where you can talk to all of them. @mention a session to ask just that one. Replies collect in a thread under the post they answer."));
  }
  // Older history loads on request; the intro only shows once you've reached the very first message.
  const head = [intro];
  if (messages.length && messages[0].id > oldest) {
    const b = el("button", "btn"), wrap = el("div", "older");
    b.type = "button";
    b.append(icon("clock"), "Load earlier messages");
    b.onclick = async () => {
      const log = $("log"), h = log.scrollHeight, top = log.scrollTop;
      b.disabled = true;
      await loadOlder();
      render();
      log.scrollTop = top + log.scrollHeight - h;
    };
    wrap.append(b);
    head[0] = wrap;
  }
  renderList($("log"), dm ? dmItems(M, view) : M.roots, {M, view, chips: !dm, preview: true, mark: newMark.id}, head);
}

function renderThread(M) {
  const panel = $("thread");
  const root = thread && M.roots.find(g => g.key === thread);
  if (!root || view !== CHANNEL) { panel.hidden = true; thread = null; return false; }
  panel.hidden = false;
  const log = $("tlog"), reps = M.threads.get(thread) || [];
  const stick = log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  const head = [messageEl(root, {M, log, stick, chips: true}),
                el("div", "tdiv", reps.length ? reps.length + (reps.length === 1 ? " reply" : " replies") : "No replies yet")];
  renderList(log, reps, {M, chips: true, stick, after: root}, head);
  const who = threadPeople(M, root);
  setHint($("tcompose"), (who.length ? "Goes to " + who.join(", ") : "Goes to every session") + " · @mention to bring in someone else",
          working(who));
  return true;
}
// sessions taking part in a thread; your thread replies go to them
function threadPeople(M, root) {
  const names = new Set(peers.map(p => p.name)), who = new Set();
  for (const g of [root, ...(M.threads.get(root.key) || [])])
    for (const r of g.rows) for (const n of [r.sender, r.recipient]) if (names.has(n)) who.add(n);
  return [...who];
}

function renderMembers() {
  const box = el("div", "members"), by = {busy: [], on: [], idle: [], off: []};
  for (const p of peers) by[presence(p)].push(p);
  for (const k of ["busy", "on", "idle", "off"]) {
    if (!by[k].length) continue;
    box.append(el("div", "section", PRESENCE[k][0] + " — " + by[k].length));
    for (const p of by[k]) {
      const m = el("div", "member " + k), l = el("div", "label"), top = el("div");
      const n = el("span", "name", p.name);
      n.style.setProperty("--h", hue(p.name));
      n.style.color = `hsl(${hue(p.name)} 62% var(--name-l))`;
      top.append(n, " ", aiTag());
      l.append(top, k === "busy" ? el("div", "sub working", p.activity || "Working…") : el("div", "sub", p.summary || PRESENCE[k][1]));
      m.append(avatar(p.name, true), l);
      if (p.quiet) m.append(quietIcon(p));
      m.title = p.cwd;
      m.onclick = () => showPopout(p.name, m);
      box.append(m);
    }
  }
  if (!peers.length) box.append(el("div", "empty", "No sessions have connected yet."));
  $("members").replaceChildren(box);
}

// A session's profile: who it is, whether it's listening, and its one setting (posts to everyone).
function profileCard(name, docked) {
  const p = peerOf(name), card = el("div", "card" + (docked ? " docked" : ""));
  const banner = el("div", "card-banner"), a = avatar(name, true);
  banner.style.setProperty("--h", hue(name));
  a.classList.add("xl");
  const body = el("div", "card-body"), head = el("div", "card-name");
  head.append(el("span", null, name), aiTag());
  body.append(head);
  card.append(banner, a, body);
  if (!p) {
    body.append(el("p", "mono", "This session isn't connected to Bridge any more."));
    return card;
  }
  const s = presence(p), st = el("div", "card-status");
  st.append(el("span", "dot " + s), PRESENCE[s][0] + " · " + (p.busy_since ? "for " + dur(p.busy_since)
            : p.listening ? "listening" : "last seen " + ago(p.last_seen)));
  st.title = PRESENCE[s][1];
  const info = el("div", "card-box");
  if (p.busy_since) info.append(el("h4", null, "Working on"), el("p", "activity", p.activity || "Something"));
  info.append(el("h4", null, "About"), el("p", null, p.summary || "No description yet."),
              el("h4", null, "Project"), el("p", "mono", p.cwd),
              el("h4", null, "Activity"), el("p", null, p.sent + (p.sent === 1 ? " message sent" : " messages sent")));
  const set = el("div", "card-box"), row = el("div", "setrow"), txt = el("div", "txt");
  txt.append(el("b", null, "Posts to everyone"),
             el("span", null, p.quiet ? "Skipped. Only an @mention reaches it." : "Gets posts in #everyone that don't @mention anyone."));
  row.append(txt, toggle(!p.quiet, on => setQuiet(p, !on), "Posts to everyone reach " + p.name));
  set.append(row);
  const acts = el("div", "card-actions");
  if (view !== name) {
    const b = el("button", "btn primary");
    b.type = "button";
    b.append(icon("thread"), "Message");
    b.onclick = () => { closePopout(); setView(name); };
    acts.append(b);
  }
  const m = el("button", "btn");
  m.type = "button";
  m.append(icon("at"), view === CHANNEL ? "Mention" : "Mention in #everyone");
  m.onclick = () => { closePopout(); if (view !== CHANNEL) setView(CHANNEL); mainBox.insert("@" + name + " "); };
  acts.append(m);
  body.append(st, info, set, acts);
  return card;
}
let popFor = null;
function showPopout(name, anchor) {
  const pop = $("popout");
  popFor = name;
  pop.replaceChildren(profileCard(name));
  pop.hidden = false;
  const r = anchor.getBoundingClientRect(), w = pop.offsetWidth, h = pop.offsetHeight;
  let x = r.right + 8;
  if (x + w > innerWidth - 8) x = Math.max(8, r.left - w - 8);
  pop.style.left = x + "px";
  pop.style.top = Math.max(8, Math.min(r.top, innerHeight - h - 8)) + "px";
}
function closePopout() { $("popout").hidden = true; popFor = null; }
async function setQuiet(p, quiet) {
  try {
    await api("/api/quiet", {name: p.name, quiet});
    p.quiet = quiet;
    toast(quiet ? p.name + " now skips posts to everyone" : p.name + " gets posts to everyone again");
    refresh();
  } catch (e) {
    toast("Couldn't change it: " + e.message, true);
  }
}

// The popover under the header: pinned messages here, shared notes, or search results.
let panel = null, noteEdit = null, found = {q: "", results: [], total: 0, busy: false};
function pinnedHere(M) {
  const here = view === CHANNEL ? M.all.filter(M.inChannel) : dmItems(M, view);
  return here.filter(isPinned).sort((a, b) => Math.max(...idsOf(b).map(i => pins.get(i) || 0)) - Math.max(...idsOf(a).map(i => pins.get(i) || 0)));
}
// a message as a card in a list (pins, search results); click to jump to it
function hit(m, body, where, extra) {
  const d = el("div", "hit"), meta = el("div", "hm");
  meta.append(nameEl(m.sender));
  if (m.sender !== "human") meta.append(aiTag());
  meta.append(el("span", "time", when(m.created)));
  if (where) meta.append(el("span", "where", "· " + where));
  if (extra) {
    const x = el("span", "hx");
    x.append(extra);
    meta.append(x);
  }
  const b = el("div", "hb");
  b.append(...body);
  if (m.attachments && m.attachments.length) b.append(m.body ? "\n" : "", "🖼 " + m.attachments.length + (m.attachments.length === 1 ? " image" : " images"));
  d.append(avatar(m.sender), meta, b);
  d.onclick = ev => { if (!ev.target.closest(".peer, .ib")) { closePanel(); jump(m.id); } };
  return d;
}
const whereOf = m => m.sender !== "human" && m.recipient !== "human" || m.channel ? "#everyone"
  : "DM with " + (m.sender === "human" ? m.recipient : m.sender);
// the text with each search word highlighted, trimmed to the part around the first match
function highlight(body, words) {
  body = body.replace(/\*\*|```[\w+-]*/g, "");
  const low = body.toLowerCase(), at = Math.min(...words.map(w => low.indexOf(w.toLowerCase())).filter(i => i >= 0), body.length);
  let text = body, cut = 0;
  if (at > 160) { cut = body.lastIndexOf(" ", at - 80) + 1 || at - 80; text = "…" + body.slice(cut); }
  if (!words.length) return [text];
  const re = new RegExp("(" + words.map(w => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|") + ")", "gi");
  return text.split(re).map((part, i) => i % 2 ? el("mark", null, part) : part);
}
const PANELS = {
  pinned() {
    const M = model(), list = pinnedHere(M), box = el("div", "pins"), head = el("div", "pins-head"), body = el("div", "pins-list");
    head.append(icon("pin"), "Pinned messages", el("span", null, view === CHANNEL ? "in #everyone" : "with " + view));
    for (const g of list) {
      const m = {...g.rows[0], attachments: g.attachments};
      body.append(hit(m, [g.body], null, iconBtn("x", "Unpin", () => pin(g, false))));
    }
    if (!list.length) {
      const e = el("div", "empty");
      e.append(icon("pin"), "Nothing pinned here yet. Hover a message and click the pin to keep it handy.");
      body.append(e);
    }
    box.append(head, body);
    return box;
  },
  notes() {
    const box = el("div", "pins"), head = el("div", "pins-head"), list = el("div", "pins-list");
    const add = el("button", "btn primary");
    add.type = "button";
    add.append(icon("plus"), "New note");
    add.onclick = () => { noteEdit = {key: "", value: "", isNew: true}; renderPanel(true); };
    head.append(icon("notes"), "Shared notes", el("span", "grow"), add);
    if (noteEdit && noteEdit.isNew) list.append(noteForm());
    for (const n of notes) {
      if (noteEdit && !noteEdit.isNew && noteEdit.key === n.key) { list.append(noteForm()); continue; }
      const d = el("div", "note"), row = el("div", "row");
      row.append(el("b", null, n.key),
                 iconBtn("pencil", "Edit note", () => { noteEdit = {key: n.key, value: n.value}; renderPanel(true); }),
                 iconBtn("trash", "Delete note", ev => deleteNote(n.key, ev.currentTarget)));
      d.append(row, el("pre", null, n.value), el("div", "by", "Saved by " + display(n.updated_by) + " · " + ago(n.updated)));
      list.append(d);
    }
    if (!notes.length && !noteEdit) {
      const e = el("div", "empty");
      e.append(icon("notes"), "No shared notes yet. Sessions save facts other projects need (ports, API shapes) here, and so can you.");
      list.append(e);
    }
    box.append(head, list);
    return box;
  },
  search() {
    const box = el("div", "pins"), head = el("div", "pins-head"), list = el("div", "pins-list");
    const words = found.q.split(/\s+/).filter(w => w && !/^from:/i.test(w));
    head.append(icon("search"), found.busy ? "Searching…" : found.total + (found.total === 1 ? " result" : " results"),
                el("span", null, found.total > found.results.length ? "showing the newest " + found.results.length : ""));
    for (const m of found.results) {
      const to = m.to.filter(n => n !== "human");
      const where = whereOf(m) + (m.sender !== "human" && to.length && whereOf(m) === "#everyone" ? " → " + andList(to) : "");
      list.append(hit(m, highlight(m.body, words), where));
    }
    if (!found.busy && !found.results.length) {
      const e = el("div", "empty");
      e.append(icon("search"), "Nothing matches “" + found.q + "”.");
      list.append(e);
    }
    list.append(el("div", "panel-note", "Tip: from:web finds what one session said; from:me finds yours."));
    box.append(head, list);
    return box;
  },
};
function noteForm() {
  const f = el("form", "note-form"), key = el("input"), value = el("textarea"), bts = el("div", "bts");
  key.placeholder = "Name, e.g. api/orders-contract";
  key.value = noteEdit.key;
  key.disabled = !noteEdit.isNew;
  key.oninput = () => { noteEdit.key = key.value; };
  value.placeholder = "What every session should know";
  value.value = noteEdit.value;
  value.oninput = () => { noteEdit.value = value.value; };
  const cancel = el("button", "btn", "Cancel"), ok = el("button", "btn primary", "Save");
  cancel.type = "button";
  cancel.onclick = () => { noteEdit = null; renderPanel(true); };
  bts.append(cancel, ok);
  f.append(key, value, bts);
  f.onsubmit = async ev => {
    ev.preventDefault();
    if (!noteEdit.key.trim() || !noteEdit.value.trim()) return toast("A note needs a name and some text.", true);
    try {
      await api("/api/note", {key: noteEdit.key, value: noteEdit.value});
      toast("Saved note " + noteEdit.key.trim());
      noteEdit = null;
      await poll();
      renderPanel(true);
    } catch (e) { toast("Couldn't save it: " + e.message, true); }
  };
  setTimeout(() => (noteEdit && noteEdit.isNew ? key : value).focus());
  return f;
}
async function deleteNote(key, btn) {
  if (!btn.classList.contains("armed")) {
    btn.classList.add("armed", "alert");
    btn.title = "Click again to delete";
    toast("Click the bin again to delete " + key);
    setTimeout(() => btn.classList.remove("armed", "alert"), 4000);
    return;
  }
  try { await api("/api/note", {key, value: ""}); toast("Deleted note " + key); await poll(); renderPanel(true); }
  catch (e) { toast("Couldn't delete it: " + e.message, true); }
}
// Redraws leave the panel alone while you're typing in it, unless forced.
function renderPanel(force) {
  const box = $("pins");
  if (box.hidden || !panel || !force && box.contains(document.activeElement) && document.activeElement.matches("input, textarea")) return;
  box.replaceChildren(PANELS[panel]());
}
function openPanel(kind, anchor, toggle = true) {
  const box = $("pins");
  if (toggle && !box.hidden && panel === kind) return closePanel();
  closePicker();
  panel = kind;
  box.hidden = false;
  renderPanel(true);
  const r = anchor.getBoundingClientRect();
  box.style.top = r.bottom + 8 + "px";
  box.style.right = Math.max(8, innerWidth - r.right) + "px";
}
function closePanel() { $("pins").hidden = true; panel = null; noteEdit = null; }
async function search(q) {
  q = q.trim();
  if (!q) return closePanel();
  $("search").value = q;
  $("search-box").classList.add("has");
  found = {q, results: [], total: 0, busy: true};
  openPanel("search", $("search-box"), false);
  try {
    const r = await api("/api/search?q=" + encodeURIComponent(q));
    if (found.q !== q) return;
    found = {q, results: r.results, total: r.total, busy: false};
  } catch (e) {
    found.busy = false;
    toast("Search failed: " + e.message, true);
  }
  if (panel === "search") renderPanel(true);
}

function renderHeader() {
  const dm = view !== CHANNEL, peer = peerOf(view);
  if (dm) $("bar-icon").replaceChildren(avatar(view, true));
  else $("bar-icon").replaceChildren(icon("hash", 24));
  $("title").textContent = dm ? view : "everyone";
  $("topic").textContent = dm ? (peer && (peer.summary || peer.cwd) || "") : "Sessions coordinating with each other";
  const tools = [];
  if (dm && peer) tools.push(iconBtn(peer.quiet ? "bell-off" : "bell",
    peer.quiet ? view + " skips posts to everyone. Click to include it." : view + " gets posts to everyone. Click to skip them (@mentions still reach it).",
    () => setQuiet(peer, !peer.quiet), peer.quiet ? "alert" : ""));
  const pinned = pinnedHere(model()).length;
  const pb = iconBtn("pin", "Pinned messages", ev => openPanel("pinned", ev.currentTarget), "pins-btn" + (panel === "pinned" ? " on" : ""));
  if (pinned) pb.append(el("span", "badge", pinned));
  const nb = iconBtn("notes", "Shared notes", ev => openPanel("notes", ev.currentTarget), "pins-btn" + (panel === "notes" ? " on" : ""));
  if (notes.length) nb.append(el("span", "badge", notes.length));
  tools.push(pb, nb);
  if (dm) tools.push(iconBtn("user", settings.profile ? "Hide profile" : "Show profile",
                             () => { settings.profile = !settings.profile; saveSettings(); refresh(); }, settings.profile ? "on" : ""));
  else tools.push(iconBtn("users", settings.members ? "Hide member list" : "Show member list",
                          () => { settings.members = !settings.members; saveSettings(); refresh(); }, settings.members ? "on" : ""));
  $("tools").replaceChildren(...tools);
}

function notify(M) {
  // a chime and desktop notification for new posts to you (or anything, if chosen), unless you're looking at them
  const seen = new Set(), news = [];
  for (const m of messages) {
    if (m.id <= notified) continue;
    const g = M.byRow.get(m.id);
    if (!g || seen.has(g.key) || g.sender === "human") continue;
    seen.add(g.key);
    if (g.rows.some(r => r.recipient === "human") || settings.notifyAll && M.inChannel(g)) news.push(g);
  }
  notified = since;
  if (settings.muted) return;
  let chimed = false;
  for (const g of news) {
    const root = M.inChannel(g) ? M.rootOf(g) : null;
    const where = root ? CHANNEL : g.sender, inThread = root && root !== g ? root.key : null;
    if (!document.hidden && view === where && (!inThread || thread === inThread)) continue;
    if (settings.sound && !chimed) { ping(); chimed = true; }
    if (!settings.desktop || !("Notification" in window) || Notification.permission !== "granted") continue;
    const toMe = g.rows.some(r => r.recipient === "human");
    const n = new Notification(g.sender + (toMe ? " replied" : " posted") + (root ? " in #everyone" : ""),
                               {body: g.body.slice(0, 200) || "Sent an image", tag: "bridge-" + g.key});
    n.onclick = () => { window.focus(); setView(where); if (inThread) openThread(inThread); n.close(); };
  }
}

const peerSig = () => peers.map(p => [p.name, presence(p), p.summary, p.cwd, p.quiet, p.activity]);
const sig = () => JSON.stringify([since, extraSig, messages.length, [...unread], peerSig(), notes, view, thread, connected,
                                  Math.floor(serverNow() / 60)]);
function render() {
  if (view !== CHANNEL && !peerOf(view)) view = CHANNEL;
  drawn = sig();
  const M = model(), dm = view !== CHANNEL;
  // The NEW line marks what arrived since you last looked at this view. It stays put while you're here, and
  // isn't drawn for messages that arrive while you watch.
  if (!newMark || newMark.view !== view || document.hidden) {
    const seen = lastRead[view] ?? lastRead.base ?? 0;
    newMark = {view, id: (dm ? dmItems(M, view) : M.roots).some(g => fresh(g, seen)) ? seen : Infinity};
  }
  markRead(M);
  mainBox.input.placeholder = dm ? "Message @" + view : "Message #everyone";
  const quiet = peers.filter(p => p.quiet).map(p => p.name);
  setHint($("compose"), dm ? "Only " + view + " sees this · type / for commands"
    : "@mention sessions to ask just them" + (quiet.length ? " · without one, it skips " + quiet.join(", ") : "") + " · / for commands",
    working(dm ? [view] : peers.map(p => p.name)));
  renderHeader();
  renderSidebar(M);
  renderMe();
  renderBanner();
  renderMain(M);
  const threadOpen = renderThread(M);
  $("members").hidden = dm || threadOpen || !settings.members;
  if (!$("members").hidden) renderMembers();
  $("profile").hidden = !dm || !settings.profile;
  if (!$("profile").hidden) $("profile").replaceChildren(profileCard(view, true));
  if (popFor && !$("popout").hidden) $("popout").replaceChildren(profileCard(popFor));
  renderPanel();
  if (pickFor != null && !M.all.some(g => g.key === pickFor)) closePicker();
  if (!$("settings-modal").hidden) renderSettings();
}
const refresh = () => render();

function setHash() {
  history.replaceState(null, "", view !== CHANNEL ? "#" + encodeURIComponent(view) : thread ? "#everyone/" + thread : "#everyone");
}
function setView(v) {
  if (v !== view && panel === "pinned") closePanel();
  view = v;
  if (v !== CHANNEL) thread = null;
  setHash();
  mainBox.restore();
  render();
  $("log").scrollTop = $("log").scrollHeight;
  mainBox.input.focus();
}
function openThread(key) {
  view = CHANNEL;
  thread = key;
  setHash();
  sideBox.restore();
  render();
  $("tlog").scrollTop = $("tlog").scrollHeight;
  sideBox.input.focus();
}
$("close-thread").onclick = () => { thread = null; setHash(); render(); };

function findIn(log, id) {
  return [...log.children].find(c => c.dataset.ids && c.dataset.ids.includes(" " + id + " "));
}
// Fetch earlier history: 300 more messages, or all of them back to `until`.
async function loadOlder(until) {
  if (!messages.length) return false;
  try {
    const r = await api("/api/older?before=" + messages[0].id + (until ? "&until=" + until : ""));
    if (!r.messages.length) { oldest = messages[0].id; return false; }
    messages.unshift(...r.messages);
    return true;
  } catch (e) {
    toast("Couldn't load earlier messages: " + e.message, true);
    return false;
  }
}
async function jump(id) {
  if (messages.length && id < messages[0].id && id >= oldest) {
    if (!await loadOlder(id)) return;
    render();
  }
  const M = model(), g = M.byRow.get(id);
  if (!g) return toast("That message isn't around any more.");
  let log = $("log");
  if (M.inChannel(g)) {
    const root = M.rootOf(g);
    if (root !== g) { openThread(root.key); log = $("tlog"); }
    else if (view !== CHANNEL) setView(CHANNEL);
  } else if (view !== viewsOf(g)[0]) setView(viewsOf(g)[0]);
  const t = findIn(log, id) || findIn($("tlog"), id);
  if (!t) return;
  t.scrollIntoView({behavior: "smooth", block: "center"});
  t.classList.add("flash");
  setTimeout(() => t.classList.remove("flash"), 1200);
}
function openLightbox(url, label) {
  const lb = $("lightbox"), img = lb.querySelector("img");
  img.src = url;
  img.alt = label;
  lb.querySelector("a").href = url;
  lb.hidden = false;
}
$("lightbox").onclick = ev => { if (ev.target.tagName !== "A") $("lightbox").hidden = true; };

// --- settings ---
let setTab = "appearance";
function openSettings(tab) {
  if (tab) setTab = tab;
  closePopout();
  closePanel();
  closePicker();
  renderSettings();
  $("settings-modal").hidden = false;
}
function closeSettings() { $("settings-modal").hidden = true; }
function change(key, value) {
  settings[key] = value;
  saveSettings();
  applyAppearance();
  render();
}
function settingRow(title, desc, control) {
  const r = el("div", "setting"), t = el("div", "txt");
  t.append(el("b", null, title), el("span", null, desc));
  r.append(t, control);
  return r;
}
function choices(key, opts) {
  const box = el("div", "choices");
  for (const [value, label, desc, preview] of opts) {
    const b = el("button", "choice" + (settings[key] === value ? " on" : ""));
    b.type = "button";
    b.append(preview, el("b", null, label), el("span", null, desc));
    b.onclick = () => change(key, value);
    box.append(b);
  }
  return box;
}
function radio(key, value, label, desc) {
  const b = el("button", "radio" + (settings[key] === value ? " on" : "")), t = el("div");
  b.type = "button";
  t.append(el("b", null, label), el("span", null, desc));
  b.append(t);
  b.onclick = () => change(key, value);
  return b;
}
function themePreview(bg) {
  const p = el("div", "prev");
  p.style.background = bg;
  return p;
}
function densityPreview(compact) {
  const p = el("div", "prev dens" + (compact ? " compact-p" : ""));
  for (let i = 0; i < (compact ? 5 : 2); i++) {
    const r = el("div", "pv-row");
    r.append(el("i", "pv-av"), el("i", "pv-line"));
    r.lastChild.style.maxWidth = [90, 60, 75, 50, 85][i] + "%";
    p.append(r);
  }
  return p;
}
const TABS = {
  appearance: () => [
    el("h2", null, "Appearance"),
    el("h3", null, "Theme"),
    choices("theme", [
      ["system", "Sync with system", "Follows your computer", themePreview("linear-gradient(135deg, #313338 50%, #f2f3f5 50%)")],
      ["dark", "Dark", "Easy on the eyes", themePreview("linear-gradient(90deg, #1e1f22 14%, #2b2d31 14% 40%, #313338 40%)")],
      ["light", "Light", "Bright and crisp", themePreview("linear-gradient(90deg, #e3e5e8 14%, #f2f3f5 14% 40%, #fff 40%)")],
    ]),
    el("h3", null, "Message display"),
    choices("density", [
      ["cozy", "Cozy", "Avatars, names and grouped messages", densityPreview(false)],
      ["compact", "Compact", "More messages on screen", densityPreview(true)],
    ]),
    settingRow("Collapse long messages", "Fold answers longer than about 30 lines behind “Show more”.",
               toggle(settings.collapse, on => change("collapse", on), "Collapse long messages")),
  ],
  notifications: () => {
    const perm = "Notification" in window ? Notification.permission : "unsupported";
    const desc = perm === "denied" ? "Your browser blocks notifications from this page. Allow them in its site settings first."
      : perm === "unsupported" ? "This browser doesn't support desktop notifications."
      : "Pop up a notification when a session replies while you're looking elsewhere.";
    const desktop = toggle(settings.desktop && perm === "granted", async on => {
      if (on && perm === "default") await Notification.requestPermission();
      change("desktop", on);
    }, "Desktop notifications", perm === "denied" || perm === "unsupported");
    return [
      el("h2", null, "Notifications"),
      settingRow("Mute all notifications", "Silence chimes and pop-ups. Unread badges still count up.",
                 toggle(settings.muted, on => change("muted", on), "Mute all notifications")),
      settingRow("Desktop notifications", desc, desktop),
      settingRow("Sound", "Play a short chime when something new arrives.",
                 toggle(settings.sound, on => { change("sound", on); if (on) ping(); }, "Sound")),
      el("h3", null, "Notify me about"),
      radio("notifyAll", false, "Replies to me", "A session answers you, in a DM or in #everyone."),
      radio("notifyAll", true, "All session activity", "Also when sessions post to each other in #everyone."),
    ];
  },
  sessions: () => {
    const out = [el("h2", null, "Sessions"),
                 el("p", "lead", "Choose which sessions get posts in #everyone that don't @mention anyone (and other sessions' broadcasts). An @mention always gets through. This is shared by everyone using Bridge on this computer.")];
    for (const p of peers) {
      const r = el("div", "setting"), t = el("div", "txt"), nm = el("b");
      nm.append(p.name, " ", aiTag());
      t.append(nm, el("span", "mono", p.cwd));
      if (p.summary) t.append(el("span", null, p.summary));
      r.append(clickable(avatar(p.name, true), p.name), t, toggle(!p.quiet, on => setQuiet(p, !on), "Posts to everyone reach " + p.name));
      out.push(r);
    }
    if (!peers.length) out.push(el("p", "lead", "No sessions have connected yet."));
    const legend = el("div", "legend");
    for (const k of ["busy", "on", "idle", "off"]) {
      const t = el("div");
      t.append(el("b", null, PRESENCE[k][0]), el("span", null, PRESENCE[k][1]));
      legend.append(el("span", "dot " + k), t);
    }
    out.push(el("h3", null, "Status"), legend);
    return out;
  },
  about: () => {
    const head = el("div", "about-head"), t = el("div");
    t.append(el("h2", null, "Bridge"), el("p", null, "Lets Claude Code sessions in different projects talk to each other."));
    head.append(logo(), t);
    const keys = el("div", "keys");
    for (const [k, what] of [["Ctrl/⌘ + K", "Jump to a conversation"], ["Alt + ↑ ↓", "Previous or next conversation"],
                             ["Alt + Shift + ↑ ↓", "Previous or next conversation with unread messages"],
                             ["Enter", "Send"], ["Shift + Enter", "New line"],
                             ["@", "Mention a session (↑ ↓ to pick, Tab or Enter to insert)"],
                             ["/", "Commands: " + COMMANDS.map(c => "/" + c[0]).join(", ")],
                             ["Ctrl/⌘ + V", "Paste an image to attach it"], ["Esc", "Close a panel or dialog"]]) {
      keys.append(el("kbd", null, k), el("span", null, what));
    }
    const link = el("p");
    const a = el("a", null, "github.com/Nyanites/bridge");
    a.href = "https://github.com/Nyanites/bridge";
    a.target = "_blank";
    a.rel = "noopener";
    link.append("Free for everyone. Made by Cydraintel · ", a);
    return [head, el("h3", null, "Keyboard"), keys, el("h3", null, "Project"), link];
  },
};
function renderSettings() {
  for (const b of document.querySelectorAll(".tab")) b.classList.toggle("on", b.dataset.tab === setTab);
  $("set-body").replaceChildren(...TABS[setTab]());
}
for (const b of document.querySelectorAll(".tab")) b.onclick = () => { setTab = b.dataset.tab; renderSettings(); };
$("gear").onclick = () => openSettings();
$("who").onclick = () => openSettings();
$("set-close").onclick = closeSettings;
$("settings-modal").onmousedown = ev => { if (ev.target === ev.currentTarget) closeSettings(); };
$("mute").onclick = () => { change("muted", !settings.muted); toast(settings.muted ? "Notifications muted" : "Notifications on"); };
$("banner-on").onclick = async () => { await Notification.requestPermission(); settings.desktop = true; saveSettings(); render(); };
$("banner-x").onclick = () => change("banner", false);

document.addEventListener("mousedown", ev => {
  if (!$("popout").hidden && !$("popout").contains(ev.target)) closePopout();
  if (!$("pins").hidden && !$("pins").contains(ev.target) && !ev.target.closest(".pins-btn")) closePanel();
  if (!$("picker").hidden && !$("picker").contains(ev.target)) closePicker();
});
document.addEventListener("keydown", ev => {
  const mod = ev.metaKey || ev.ctrlKey;
  if (mod && ev.key.toLowerCase() === "k") { ev.preventDefault(); return openSwitcher(); }
  if (ev.altKey && (ev.key === "ArrowUp" || ev.key === "ArrowDown")) {
    ev.preventDefault();
    return step(ev.key === "ArrowDown" ? 1 : -1, ev.shiftKey);
  }
  if (ev.key !== "Escape") return;
  if (!$("lightbox").hidden) $("lightbox").hidden = true;
  else if (!$("switcher").hidden) $("switcher").hidden = true;
  else if (!$("picker").hidden) closePicker();
  else if (!$("popout").hidden) closePopout();
  else if (!$("pins").hidden) closePanel();
  else if (!$("settings-modal").hidden) closeSettings();
});
$("search").onkeydown = ev => {
  if (ev.key === "Enter") { ev.preventDefault(); search($("search").value); }
  if (ev.key === "Escape") { $("search").value = ""; $("search-box").classList.remove("has"); $("search").blur(); }
};
$("search").oninput = () => $("search-box").classList.toggle("has", !!$("search").value);
$("present").onclick = () => $("log").scrollTo({top: $("log").scrollHeight, behavior: "smooth"});
$("log").addEventListener("scroll", () => {
  const log = $("log");
  $("present").hidden = log.scrollHeight - log.scrollTop - log.clientHeight < 700;
}, {passive: true});

// Conversations in sidebar order; Alt+arrows walk them, Ctrl/⌘+K searches them.
const conversations = () => [CHANNEL, ...peers.map(p => p.name)];
function step(dir, unreadOnly) {
  const list = conversations(), M = model(), i = list.indexOf(view);
  for (let k = 1; k < list.length; k++) {
    const v = list[(i + dir * k + list.length * k) % list.length];
    if (!unreadOnly || (v === CHANNEL ? unreadChannel(M) : unreadDM(M, v))) return setView(v);
  }
  if (unreadOnly) toast("Nothing unread anywhere else");
}
let qsIndex = 0;
function openSwitcher() {
  const box = $("switcher"), input = box.querySelector("input");
  closePopout();
  closePanel();
  input.value = "";
  qsIndex = 0;
  box.hidden = false;
  renderSwitcher();
  input.focus();
}
function switcherItems() {
  const q = $("switcher").querySelector("input").value.trim().toLowerCase().replace(/^[#@]/, ""), M = model();
  return conversations().map(v => {
    const p = peerOf(v);
    return {v, name: v === CHANNEL ? "everyone" : v, sub: p ? p.busy_since ? "Working…" : p.summary || p.cwd : "every session",
            n: v === CHANNEL ? unreadChannel(M) : unreadDM(M, v)};
  }).filter(o => !q || o.name.toLowerCase().includes(q) || o.sub.toLowerCase().includes(q))
    .sort((a, b) => !q ? 0 : b.name.toLowerCase().startsWith(q) - a.name.toLowerCase().startsWith(q));
}
function renderSwitcher() {
  const items = switcherItems(), list = $("switcher").querySelector(".qs-list");
  qsIndex = Math.max(0, Math.min(qsIndex, items.length - 1));
  list.replaceChildren(...items.map((o, i) => {
    const d = el("div", "qs-item" + (i === qsIndex ? " on" : ""));
    const ic = o.v === CHANNEL ? el("span", "hash") : avatar(o.v, true);
    if (o.v === CHANNEL) ic.append(icon("hash", 18));
    d.append(ic, el("b", null, o.name), el("span", "sub", o.sub));
    if (o.n) d.append(el("span", "pill", o.n > 99 ? "99+" : String(o.n)));
    d.onmousedown = ev => { ev.preventDefault(); $("switcher").hidden = true; setView(o.v); };
    return d;
  }));
  if (!items.length) list.append(el("div", "empty", "No conversation matches that."));
}
{
  const input = $("switcher").querySelector("input");
  input.oninput = () => { qsIndex = 0; renderSwitcher(); };
  input.onkeydown = ev => {
    const items = switcherItems();
    if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
      ev.preventDefault();
      qsIndex = (qsIndex + (ev.key === "ArrowDown" ? 1 : items.length - 1)) % Math.max(1, items.length);
      renderSwitcher();
    } else if (ev.key === "Enter" && items[qsIndex]) {
      ev.preventDefault();
      $("switcher").hidden = true;
      setView(items[qsIndex].v);
    }
  };
  $("switcher").onmousedown = ev => { if (ev.target === ev.currentTarget) $("switcher").hidden = true; };
}

// One poll at a time: a poll asked for while another runs (say, right after you react) runs straight after it.
let pollTimer = null, polling = null, pollAgain = false;
async function poll() {
  if (polling) { pollAgain = true; return polling; }
  clearTimeout(pollTimer);
  polling = pollOnce();
  await polling;
  polling = null;
  pollTimer = setTimeout(poll, pollAgain ? 0 : 1500);
  pollAgain = false;
}
async function pollOnce() {
  try {
    const s = await api("/api/state?since=" + since + "&first=" + (messages.length ? messages[0].id : 0));
    const first = since === 0;
    skew = Date.now() / 1000 - s.now;
    peers = s.peers;
    notes = s.notes;
    connected = true;
    messages.push(...s.messages);
    if (s.messages.length) since = s.messages[s.messages.length - 1].id;
    unread = new Set(s.unread);
    reacts = new Map();
    for (const r of s.reactions) (reacts.get(r.message_id) || reacts.set(r.message_id, []).get(r.message_id)).push(r);
    pins = new Map(s.pins);
    gone = new Set(s.deleted);
    oldest = s.oldest;
    extraSig = JSON.stringify([s.reactions, s.pins, s.deleted, s.oldest]);
    const pinnedEarlier = s.pins.map(p => p[0]).filter(id => messages.length && id < messages[0].id);
    if (pinnedEarlier.length) await loadOlder(Math.min(...pinnedEarlier));  // keep pinned messages at hand
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
  } catch (e) {
    connected = false;
  }
  // Redraw only when something visible changed (plus once a minute for "2m ago" times). Rebuilding on
  // every poll made the page flash, and peers' last_seen ticks every second, so it isn't part of this.
  if (sig() !== drawn) render();
}

const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
const asBase64 = file => new Promise((ok, fail) => {
  const r = new FileReader();
  r.onload = () => ok(String(r.result).split(",")[1]);
  r.onerror = () => fail(new Error("couldn't read " + file.name));
  r.readAsDataURL(file);
});
// Slash commands: [name, arguments, what it does, run(rest of the line)]. They run here and send nothing.
const COMMANDS = [
  ["note", "name text", "Save a shared note every session can read", async a => {
    const [, key, value] = a.match(/^(\S+)\s+([\s\S]+)$/) || [];
    if (!key) throw new Error("Usage: /note name text");
    await api("/api/note", {key, value});
    toast("Saved note " + key);
  }],
  ["search", "words", "Search every message (from:web for one session's)", a => search(a)],
  ["pins", "", "Show the messages pinned here", () => openPanel("pinned", $("tools"))],
  ["notes", "", "Show the shared notes", () => openPanel("notes", $("tools"))],
  ["quiet", "session", "Switch whether a session gets posts to everyone", a => {
    const p = peerOf(a.replace(/^@/, "").trim());
    if (!p) throw new Error("Usage: /quiet session");
    return setQuiet(p, !p.quiet);
  }],
  ["go", "conversation", "Jump to #everyone or a session's DM", a => {
    const q = a.replace(/^[#@]/, "").trim().toLowerCase();
    const v = q === "everyone" ? CHANNEL : (peers.find(p => p.name.toLowerCase() === q) || peers.find(p => p.name.toLowerCase().startsWith(q)) || {}).name;
    if (!v) throw new Error("No conversation called " + a);
    setView(v);
  }],
  ["theme", "dark | light | system", "Switch the theme", a => {
    if (!["dark", "light", "system"].includes(a.trim())) throw new Error("Usage: /theme dark, light or system");
    change("theme", a.trim());
  }],
  ["compact", "", "Switch between cozy and compact messages", () => change("density", settings.density === "compact" ? "cozy" : "compact")],
  ["shortcuts", "", "Show keyboard shortcuts", () => openSettings("about")],
];
// A send box with the @mention and /command pickers, image attachments (button, paste or drop onto dropZone)
// and a draft kept per conversation. payload(body) says who it goes to; draftKey() names the conversation.
function composer(form, payload, canMention, dropZone, draftKey) {
  const input = form.querySelector("textarea"), box = form.querySelector(".mentions");
  const picker = form.querySelector("input[type=file]"), pending = form.querySelector(".pending");
  let menu = {items: [], index: 0, start: 0}, files = [], sending = false;
  function addFiles(list) {
    for (const f of list) {
      if (!IMAGE_TYPES.includes(f.type)) { toast(f.name + ": only PNG, JPG, GIF and WebP images can be attached.", true); continue; }
      if (f.size > 10 * 1024 * 1024) { toast(f.name + " is larger than 10 MB.", true); continue; }
      if (files.length >= 5) { toast("At most 5 images per message.", true); break; }
      files.push({file: f, url: URL.createObjectURL(f)});
    }
    showPending();
    input.focus();
  }
  function showPending() {
    pending.replaceChildren(...files.map((x, i) => {
      const d = el("div", "pi"), img = el("img"), rm = el("button");
      img.src = x.url;
      img.title = x.file.name;
      rm.type = "button";
      rm.title = "Remove";
      rm.append(icon("x"));
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
    const cmd = before.match(/^\/(\w*)$/), m = !cmd && canMention() && before.match(/(^|\s)@([\w.-]*)$/);
    if (!cmd && !m) return close();
    const q = (cmd ? cmd[1] : m[2]).toLowerCase();
    const opts = cmd ? COMMANDS.filter(c => c[0].startsWith(q)).map(c => ({name: c[0], args: c[1], sub: c[2], cmd: true}))
      : [...peers.map(p => ({name: p.name, sub: p.summary || p.cwd})), {name: "everyone", sub: "every session"}]
        .filter(o => o.name.toLowerCase().startsWith(q));
    if (!opts.length) return close();
    menu = {items: opts, index: Math.min(menu.index, opts.length - 1), start: before.length - q.length - 1};
    box.replaceChildren(el("div", "head", cmd ? "Commands" : "Mention a session"));
    opts.forEach((o, i) => {
      const d = el("div", "mopt" + (i === menu.index ? " on" : ""));
      const ic = o.cmd || o.name === "everyone" ? el("span", "hash", o.cmd ? "/" : null) : avatar(o.name, true);
      if (!o.cmd && o.name === "everyone") ic.append(icon("at"));
      const label = el("b", null, o.cmd ? "/" + o.name : o.name);
      if (o.args) label.append(el("span", "sub", " " + o.args));
      d.append(ic, label, el("span", "sub", o.sub));
      d.onmousedown = ev => { ev.preventDefault(); pick(i); };
      box.append(d);
    });
    box.hidden = false;
  }
  function pick(i) {
    const o = menu.items[i], after = input.value.slice(input.selectionStart);
    const text = (o.cmd ? "/" : "@") + o.name + " ";
    input.value = input.value.slice(0, menu.start) + text + after;
    input.selectionStart = input.selectionEnd = menu.start + text.length;
    close();
    keep();
    input.focus();
  }
  function keep() {
    if (input.value) drafts[draftKey()] = input.value;
    else delete drafts[draftKey()];
    saveDrafts();
  }
  function grow() {
    input.style.height = "";
    input.style.height = input.scrollHeight + "px";
  }
  input.onkeydown = ev => {
    if (menu.items.length) {
      if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
        ev.preventDefault();
        menu.index = (menu.index + (ev.key === "ArrowDown" ? 1 : menu.items.length - 1)) % menu.items.length;
        return update();
      }
      if (ev.key === "Enter" || ev.key === "Tab") { ev.preventDefault(); return pick(menu.index); }
      if (ev.key === "Escape") { ev.stopPropagation(); return close(); }
    }
    if (ev.key === "Enter" && !ev.shiftKey && !ev.isComposing) { ev.preventDefault(); form.requestSubmit(); }
  };
  input.oninput = () => {
    grow();
    menu.index = 0;
    update();
    keep();
  };
  input.onblur = close;
  async function command(body) {
    const [, name, rest] = body.match(/^\/(\w+)\s*([\s\S]*)$/) || [];
    const c = COMMANDS.find(c => c[0] === name);
    if (!c) {
      toast("No command /" + (name || "") + ". Type / to see them, or start with a space to send it as a message.", true);
      return false;
    }
    try { await c[3](rest.trim()); return true; }
    catch (e) { toast(e.message, true); return false; }
  }
  form.onsubmit = async ev => {
    ev.preventDefault();
    const body = input.value;
    if (sending || !body.trim() && !files.length) return;
    if (body.startsWith("/") && !files.length) {
      if (await command(body)) { input.value = ""; input.style.height = ""; close(); keep(); }
      return;
    }
    sending = true;
    try {
      const images = await Promise.all(files.map(async x => ({name: x.file.name, data: await asBase64(x.file)})));
      await api("/api/send", {...payload(body), images});
      close();
      input.value = "";
      input.style.height = "";
      keep();
      files.forEach(x => URL.revokeObjectURL(x.url));
      files = [];
      showPending();
      newMark = {view, id: Infinity};  // you've caught up once you write
    } catch (e) {
      toast("Send failed: " + e.message, true);
    } finally {
      sending = false;
    }
  };
  function insert(text) {
    const at = input.selectionStart ?? input.value.length;
    input.value = input.value.slice(0, at) + text + input.value.slice(input.selectionEnd ?? at);
    input.focus();
    input.selectionStart = input.selectionEnd = at + text.length;
    grow();
    keep();
  }
  // show the draft of the conversation you just switched to
  function restore() {
    input.value = drafts[draftKey()] || "";
    close();
    grow();
  }
  restore();
  return {input, insert, restore};
}
const mainBox = composer($("compose"), body => {
  if (view !== CHANNEL) return {to: view, body};
  const tagged = mentionsIn(body);
  const everyone = tagged.includes("everyone");
  return {to: !tagged.length || everyone ? "*" : tagged, everyone, body, channel: true};
}, () => view === CHANNEL, document.querySelector(".chat"), () => view);
const sideBox = composer($("tcompose"), body => {
  const M = model(), root = M.roots.find(g => g.key === thread);
  const reps = M.threads.get(thread) || [], last = reps.length ? reps[reps.length - 1] : root;
  const tagged = mentionsIn(body), who = threadPeople(M, root);
  const to = tagged.includes("everyone") ? "*" : tagged.length ? tagged : who.length ? who : "*";
  return {to, body, channel: true, reply_to: last.rows[0].id};
}, () => true, $("thread"), () => "t" + thread);

for (const i of document.querySelectorAll("i[data-icon]")) i.replaceWith(icon(i.dataset.icon));
document.querySelector("[data-logo]").prepend(logo());
document.addEventListener("visibilitychange", () => {
  if (document.hidden) newMark = null;  // when you come back, mark what arrived while you were away
  else render();
});
applyAppearance();
renderMe();
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
    elif cmd == "quiet" and len(argv) >= 2:
        on = (argv[2] if len(argv) > 2 else "on").lower() not in ("off", "no", "0", "false")
        try:
            set_quiet(argv[1], on)
        except ValueError as e:
            sys.exit(f"Error: {e}")
        print(f"{argv[1]} {'skips' if on else 'gets'} messages to everyone; @mentions always reach it.")
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
