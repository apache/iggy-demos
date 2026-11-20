# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Prints Claude Code's stream-json output as one line per MCP call."""

import json
import sys

role = sys.argv[1] if len(sys.argv) > 1 else "agent"
WIDTH = 140
COLOURS = {"planner": "35", "worker": "36", "auditor": "33"}
tty = sys.stdout.isatty()


def colour(code, text):
    return f"\033[{code}m{text}\033[0m" if tty else text


def short(text):
    text = " ".join(str(text).split())
    return text if len(text) <= WIDTH else text[: WIDTH - 3] + "..."


def call(name, args):
    name = name.removeprefix("mcp__iggy__")
    messages = args.pop("messages", None)
    parts = [f"{k}={v}" for k, v in args.items()]
    if messages:
        payload = messages[0].get("payload", "")
        try:
            body = json.loads(payload)
            payload = body.get("task") or body.get("result") or payload
            label = body.get("plan_id", "")
            parts.append(f"{label}: {payload}" if label else payload)
        except (json.JSONDecodeError, AttributeError):
            parts.append(f"payload={payload}")
    return f"{name} {' '.join(parts)}"


def result_text(block):
    content = block.get("content")
    if isinstance(content, list):
        content = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
    return "ok" if content in (None, "", "null") else content


tag = colour(COLOURS.get(role, "0"), f"[{role}]")
for line in sys.stdin:
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    kind = event.get("type")
    if kind == "system" and event.get("subtype") == "init":
        servers = ", ".join(f"{s['name']} {s['status']}" for s in event.get("mcp_servers", []))
        print(f"{tag} mcp: {servers}", flush=True)
    elif kind == "assistant":
        for block in event["message"].get("content", []):
            if block.get("type") == "text" and block["text"].strip():
                print(f"{tag} {block['text'].strip()}", flush=True)
            elif block.get("type") == "tool_use":
                print(f"{tag} {colour('1', '->')} {short(call(block['name'], dict(block['input'])))}", flush=True)
    elif kind == "user":
        for block in event["message"].get("content", []):
            if isinstance(block, dict) and block.get("type") == "tool_result":
                mark = colour("31", "!!") if block.get("is_error") else "<-"
                print(f"{tag} {mark} {short(result_text(block))}", flush=True)
    elif kind == "result":
        print(f"{tag} done in {event.get('duration_ms', 0) / 1000:.0f}s, {event.get('num_turns', 0)} turns", flush=True)
