#!/usr/bin/env bash
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

# Runs one agent with Claude Code, connected only to that agent's MCP server.
#   ./run-agent.sh planner
#   ./run-agent.sh worker
#   ./run-agent.sh auditor
#   ./run-agent.sh operator "Delete the results topic."
# A second argument replaces the default prompt for any role. The operator
# role has no default: it runs your request on the auditor's MCP server, or
# on another agent's with OPERATOR_ON=planner or OPERATOR_ON=worker.
# Set AGENT_MODEL to pick a model.

set -euo pipefail
cd "$(dirname "$0")"

role=${1:?usage: run-agent.sh planner|worker|auditor|operator [prompt]}
case "$role" in
  planner) port=8082; prompt="Goal: Write a one-page runbook for rotating the TLS certificate on a Kubernetes ingress." ;;
  worker) port=8083; prompt="Check your progress in Iggy and do any tasks that are left." ;;
  auditor) port=8084; prompt="Audit the planner and worker." ;;
  operator)
    case "${OPERATOR_ON:-auditor}" in
      planner) port=8082 ;; worker) port=8083 ;; auditor) port=8084 ;;
      *) echo "OPERATOR_ON must be planner, worker or auditor" >&2; exit 2 ;;
    esac
    prompt=${2:?the operator needs a request} ;;
  *) echo "unknown role: $role" >&2; exit 2 ;;
esac
prompt=${2:-$prompt}

mkdir -p run
config=run/mcp-$role.json
printf '{"mcpServers":{"iggy":{"type":"http","url":"http://127.0.0.1:%s/mcp"}}}\n' "$port" > "$config"

# No built-in tools, no other MCP servers: the agent can only reach Iggy.
claude -p "$prompt" \
  --system-prompt "$(cat "agents/$role.md")" \
  --mcp-config "$config" --strict-mcp-config \
  --tools "" --allowedTools mcp__iggy \
  --no-session-persistence \
  ${AGENT_MODEL:+--model "$AGENT_MODEL"} \
  --output-format stream-json --verbose \
  | python3 -u agents/show.py "$role"
