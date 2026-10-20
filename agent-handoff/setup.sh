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

# Sets up the demo on a kind cluster, or on the current kubectl context with
# USE_CURRENT_CONTEXT=1. Safe to run again: it resets the stream, the users
# and their tokens each time.

set -euo pipefail
cd "$(dirname "$0")"

CLUSTER=${CLUSTER:-iggy-agents}
IGGY_TAG=${IGGY_TAG:-server-0.9.0}
IGGY_IMAGE=apache/iggy:0.9.0
MCP_IMAGE=apache/iggy-mcp:0.5.0

say() { printf '\n== %s\n' "$*"; }

if [ "${USE_CURRENT_CONTEXT:-0}" != 1 ]; then
  if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    say "Creating kind cluster $CLUSTER"
    kind create cluster --name "$CLUSTER"
  fi
  kubectl config use-context "kind-$CLUSTER" >/dev/null
  say "Loading images into kind"
  for image in "$IGGY_IMAGE" "$MCP_IMAGE"; do
    docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image"
  done
  kind load docker-image "$IGGY_IMAGE" "$MCP_IMAGE" --name "$CLUSTER" >/dev/null
fi

# The chart is not published to a Helm repository, so take it from the tag.
if [ -z "${IGGY_CHART:-}" ]; then
  IGGY_CHART=.iggy/helm/charts/iggy
  if [ ! -d "$IGGY_CHART" ]; then
    say "Fetching the Helm chart from apache/iggy at $IGGY_TAG"
    git clone -q --depth 1 --branch "$IGGY_TAG" --filter=blob:none --sparse \
      https://github.com/apache/iggy.git .iggy
    git -C .iggy sparse-checkout set helm/charts/iggy
  fi
fi

say "Installing the broker with Helm"
helm upgrade --install iggy "$IGGY_CHART" -f k8s/iggy-values.yaml --wait --timeout 3m

# The CLI warns that the pod has no keyring on every call; hide just that.
cli() { kubectl exec deploy/iggy -- iggy -q "$@" 2> >(grep -v keyring >&2); }
root() { cli -u iggy -p iggy "$@"; }

say "Creating the agents stream"
root stream delete agents >/dev/null 2>&1 || true
root stream create agents
root topic create agents plans 1 none 7days
root topic create agents results 1 none 7days

# The stream and its topics are the first ones created, so their IDs are 0
# (agents), 0 (plans) and 1 (results).
say "Creating one Iggy user per agent"
for user in planner worker auditor; do
  root user delete "$user" >/dev/null 2>&1 || true
done
root user create planner planner-pass -s '0:r_str#0:r_top,s_msg'
root user create worker worker-pass -s '0:r_str#0:r_top,p_msg#1:r_top,s_msg'
root user create auditor auditor-pass -s '0:r_str,r_top,p_msg'

say "Creating a personal access token per agent, stored as a Secret"
for user in planner worker auditor; do
  token=$(cli -u "$user" -p "$user-pass" pat create mcp | tail -1)
  kubectl create secret generic "iggy-pat-$user" --from-literal=token="$token" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

say "Deploying the MCP servers"
kubectl apply -f k8s/mcp.yaml
# Restart so each server picks up its new token.
kubectl rollout restart deploy/iggy-mcp-planner deploy/iggy-mcp-worker deploy/iggy-mcp-auditor >/dev/null
kubectl rollout status deploy/iggy-mcp-planner deploy/iggy-mcp-worker deploy/iggy-mcp-auditor --timeout 2m

say "Ready. Next: ./port-forward.sh"
