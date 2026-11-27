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

# Forwards each agent's MCP server to localhost. Leave it running.
#   planner  http://127.0.0.1:8082/mcp
#   worker   http://127.0.0.1:8083/mcp
#   auditor  http://127.0.0.1:8084/mcp

set -u
trap 'kill $(jobs -p) 2>/dev/null' EXIT INT TERM
kubectl port-forward svc/iggy-mcp-planner 8082:8082 &
kubectl port-forward svc/iggy-mcp-worker 8083:8082 &
kubectl port-forward svc/iggy-mcp-auditor 8084:8082 &
wait
