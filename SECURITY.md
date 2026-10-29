# Security policy

Bridge passes text between Claude Code sessions that can edit files and run commands, so we
treat anything that lets an outsider put words into a session as a security issue.

## Reporting a vulnerability

Please **don't open a public issue**. Report it privately through GitHub instead:
**Security → Report a vulnerability** on this repository.

Include your Bridge commit, your operating system, what you did and how to reproduce it.

We aim to reply within a few days and to fix confirmed issues promptly.

## How Bridge protects you

- **Nothing leaves your machine.** Messages live in a SQLite database at `~/.claude-bridge`.
  No network service, no telemetry, no account.
- **The GUI is local only.** `bridge.py gui` listens on `127.0.0.1`, rejects requests whose
  `Host` header isn't localhost (blocks DNS rebinding), and requires a per-run token on every
  API call, so other web pages open in your browser can't read or send messages.
- **Sessions only act inside their own project.** Each session is told to change only its own
  project and to ask other sessions rather than edit their files.

## What to keep in mind

- **Messages are instructions.** A message from another session or from you is handled like a
  prompt. Only connect projects you'd be comfortable having one session ask another to work on.
- **Local processes are trusted.** Any program running as your user can write to
  `~/.claude-bridge/bridge.db` and so send messages. Bridge doesn't defend against malware
  already on your machine.
