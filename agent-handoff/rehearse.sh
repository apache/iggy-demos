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

# Runs the whole demo unattended, killing the worker after its second task.
# Resets the demo first. Output goes to the terminal and run/rehearsal.log.

set -uo pipefail
cd "$(dirname "$0")"
mkdir -p run
exec > >(tee run/rehearsal.log) 2>&1

say() { printf '\n== %s\n' "$*"; }
started=$(date +%s)

say "Resetting"
./setup.sh >run/setup.log 2>&1 || { echo "setup failed, see run/setup.log"; exit 1; }

./port-forward.sh >/dev/null 2>&1 &
forward=$!
trap 'kill $forward 2>/dev/null' EXIT
sleep 3

say "1. Planner"
./run-agent.sh planner

say "2. Worker, killed after its second task"
./run-agent.sh worker > >(tee run/worker-killed.log) &
worker=$!
until [ "$(grep -c store_consumer_offset run/worker-killed.log 2>/dev/null)" -ge 2 ]; do sleep 1; done
sleep 3
pkill -f "claude -p Check your progress"
wait $worker 2>/dev/null
echo "== worker killed"
# On stage there is talk time before the restart.
sleep 20

say "3. Worker again, with no memory of the first run"
./run-agent.sh worker

say "4. Auditor"
./run-agent.sh auditor

say "5. Guardrails: the MCP server refuses"
./run-agent.sh operator "Reset the worker's stored offset on agents/plans partition 0 to 0, then delete the results topic in the agents stream."

say "6. Guardrails: the broker refuses"
OPERATOR_ON=worker ./run-agent.sh operator "Create a topic called scratch in the agents stream with 1 partition."

say "Finished in $(( $(date +%s) - started ))s"
