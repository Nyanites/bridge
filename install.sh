#!/bin/sh
# Registers the bridge MCP server for every project (user scope) and adds the
# hooks that start each session's listener and deliver incoming messages.
set -e
BRIDGE="$(cd "$(dirname "$0")" && pwd)/bridge.py"
PY="$(command -v python3)"

claude mcp remove --scope user bridge >/dev/null 2>&1 || true
claude mcp add --scope user bridge -- "$PY" "$BRIDGE" serve

"$PY" - "$PY" "$BRIDGE" <<'EOF'
import json, os, shlex, sys
py, bridge = sys.argv[1:]
path = os.path.expanduser("~/.claude/settings.json")
settings = json.load(open(path)) if os.path.exists(path) else {}
hooks = settings.setdefault("hooks", {})
for event, sub in (("SessionStart", "session-start"), ("UserPromptSubmit", "hook"), ("Stop", "stop")):
    groups = hooks.setdefault(event, [])
    groups[:] = [g for g in groups if not any("bridge.py" in h.get("command", "") for h in g.get("hooks", []))]
    groups.append({"hooks": [{"type": "command", "command": f"{shlex.quote(py)} {shlex.quote(bridge)} {sub}"}]})
allow = settings.setdefault("permissions", {}).setdefault("allow", [])
allow[:] = [r for r in allow if "bridge.py" not in r]
for rule in ("mcp__bridge", f"Bash(python3 {shlex.quote(bridge)} listen)"):
    if rule not in allow:
        allow.append(rule)
json.dump(settings, open(path, "w"), indent=2)
print(f"Hooks + permissions added to {path}")
EOF
echo "Done. Restart Claude in each IDE to connect it to the bridge."
