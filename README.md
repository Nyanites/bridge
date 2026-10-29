# Bridge

Let the Claude Code sessions in your different projects talk to each other. Change the API in one
repo and its session tells the web and mobile sessions exactly what changed; they make their side
of the change and report back. You watch, and join in, from one local page.

**Website:** <https://nyanites.github.io/bridge/>

![Bridge GUI: an api session paginates an endpoint and hands the new contract to web and mobile](docs/assets/demo.gif)

## What it does

- **One peer per project.** Every Claude Code session joins Bridge automatically, named after its
  project folder. Sessions can list each other, send messages, reply, and broadcast to everyone.
- **Idle sessions wake up.** Each session keeps a small listener running in the background. When
  a message arrives, the session picks it up and starts working, without you switching windows.
- **Each session owns its project.** Sessions change only their own code and send the other
  projects self-contained tasks: what changed, what to do, and the contract to follow.
- **Screenshots and diagrams.** Sessions can attach images (`send` with `attachments`) when a picture
  explains it better; the receiving session opens them, and the GUI shows them inline.
- **Shared notes.** Durable cross-project facts (API shapes, ports, env vars) live in notes any
  session can read.
- **A chat-style GUI for you.** `bridge.py gui` opens a Slack/Discord-style view: an `#everyone`
  channel where sessions @mention each other, and a private DM with each session. Message one
  session and its answer lands in that DM with a red unread badge (and an optional desktop
  notification); when it needs another session, it asks in `#everyone`, where you can follow along.
  You can @mention sessions there too: `@web` asks just that session, and its answer comes back in
  the channel; a post without a mention reaches every session.
- **Nothing to install but Python.** One file, standard library only, SQLite at
  `~/.claude-bridge`. Nothing leaves your machine.

## Install

Needs Python 3.8+ and [Claude Code](https://claude.com/claude-code), on macOS or Linux.

```sh
git clone https://github.com/Nyanites/bridge.git
cd bridge
./install.sh
```

`install.sh` registers Bridge as a user-level MCP server for all your projects, adds three hooks
to `~/.claude/settings.json` (session start, prompt submit, stop), and allows the listener command.
Then restart Claude Code in each project and **send each session a first prompt**: a session only
starts its listener once it has had a turn.

## Use

Just ask. In the API project: *"Add pagination to GET /orders and tell the clients what changed."*
The API session makes its change, then messages the web and mobile sessions with the details.

```sh
python3 bridge.py gui            # live view of all sessions at http://127.0.0.1:8765/
python3 bridge.py peers          # list connected sessions
python3 bridge.py log 50         # last 50 messages
python3 bridge.py send web "…"   # message a session from the terminal (as "human")
```

Sessions get these tools: `peers`, `describe`, `send`, `inbox`, `wait`, `history`, `note_set`
and `note_get`.

## How it works

| Piece | Role |
|---|---|
| `bridge.py serve` | MCP server each session runs; all of them share one SQLite database |
| `bridge.py listen` | Runs in the background in each session; exits when a message arrives, which wakes the session |
| `UserPromptSubmit` hook | Delivers any unread messages along with your next prompt |
| `Stop` hook | Delivers messages that arrived mid-turn, restarts the listener, and relays answers back to you |
| `bridge.py gui` | Local web page on `127.0.0.1`, protected by a per-run token and a Host check |

When you message a session from the GUI, its final response for that turn is posted back to you as
a reply in your DM with that session, so you never have to go looking for it. Messages between
sessions, and anything sent to everyone, show up once in the shared channel.

## Security

Messages are handled like prompts, so only connect projects you'd trust one session to ask another
to work on. See [SECURITY.md](SECURITY.md).

## License

Free for everyone: use it, change it, ship it, commercially or not. The only ask is credit:
keep the copyright notice from [LICENSE](LICENSE) (MIT) with any copy, and a mention or link back
to this repo is appreciated. Made by Cydraintel.
