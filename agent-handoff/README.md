# Give your agents a log: durable handoffs with MCP and Iggy

Two AI agents hand work to each other through Apache Iggy, using only the Iggy MCP server. A planner writes tasks to a topic. A worker does them and stores its offset after each one. Kill the worker partway through and start a new one: it has no memory of the first run, reads its stored offset and carries on. A read-only auditor then replays both topics to show what happened.

Runs on Kubernetes against Iggy server 0.9.0, installed with the Helm chart at the `server-0.9.0` tag, and Iggy MCP server 0.5.0. The agents are Claude Code in headless mode, but any MCP client that speaks streamable HTTP works.

Runtime: about 4 minutes of agent time. Leave 8 minutes on stage.

## What it shows

- An agent's progress can live in the log instead of in its context. A restart loses nothing that was stored.
- One agent hands work to another by writing to a topic. The handoff is on the record, and anyone with read access can replay it.
- Two layers of permissions. Each MCP server has its own create, read, update and delete switches, checked before anything reaches the broker. Behind that, each server logs in as its own Iggy user with stream and topic permissions.

## What's running

| Pod | Iggy user | Consumer | MCP permissions | Broker permissions on `agents` |
| --- | --- | --- | --- | --- |
| `iggy` | | | | |
| `iggy-mcp-planner` | `planner` | `planner` | create, read | send to `plans` |
| `iggy-mcp-worker` | `worker` | `worker` | create, read, update | poll `plans`, send to `results` |
| `iggy-mcp-auditor` | `auditor` | `worker` | read | poll both topics |

Delete is off on every MCP server. Each one logs in with a personal access token mounted from a Secret, through `IGGY_MCP_IGGY_TOKEN=file:/run/secrets/iggy/token`.

The offset tools (`get_consumer_offset`, `store_consumer_offset`) take no consumer argument. They use the consumer name the MCP server is configured with, so each agent that keeps its own progress needs its own MCP server. The auditor's server uses the worker's consumer name, so it can read the worker's stored offset. With update off, it can't move it.

## Setup

You need Docker, kind, kubectl, Helm, Python 3 and Claude Code. On macOS:

```sh
brew install kind helm kubectl
```

1. Create the cluster and everything in it:

```sh
./setup.sh
```

This creates a kind cluster called `iggy-agents`, fetches the Helm chart from apache/iggy at `server-0.9.0`, installs the broker, creates the `agents` stream with `plans` and `results` topics, creates the three users and their tokens, and deploys the three MCP servers. To use your current kubectl context instead of kind, set `USE_CURRENT_CONTEXT=1`. To use a local copy of the chart, set `IGGY_CHART` to its path.

Running it again resets the stream, the users and the tokens.

2. In a second terminal, forward the MCP servers to localhost and leave it running:

```sh
./port-forward.sh
```

Restart it after each `./setup.sh`, because setup replaces the MCP pods.

## The demo

Each agent runs with no built-in tools and only its own MCP server, so everything it does is an Iggy call. Each call prints as one line: `->` is the call, `<-` is the result and `!!` is an error.

### 1. The planner hands off work

```sh
./run-agent.sh planner
```

The planner breaks a goal into 5 tasks and sends each one to `agents/plans` as a message. Its prompt is in [agents/planner.md](agents/planner.md). Pass a different goal as the second argument, starting with `Goal:`.

What to say: the planner has no idea who will do the work. The topic is the handoff.

### 2. The worker starts, and gets killed

```sh
./run-agent.sh worker
```

The worker checks its stored offset, polls the next task by offset with auto-commit off, writes the section, sends the result to `agents/results`, then stores the offset. Its prompt is in [agents/worker.md](agents/worker.md).

After two `store_consumer_offset` lines, press Ctrl-C.

What to say: the agent's context is gone. Nothing was saved in it.

### 3. A new worker picks up

```sh
./run-agent.sh worker
```

The first call is `get_consumer_offset`, which comes back with `stored_offset: 1`. The worker polls offset 2 and carries on from there.

What to say: this is a new process with an empty context. The only thing that survived is the offset in Iggy.

### 4. The auditor replays the handoff

```sh
./run-agent.sh auditor
```

The auditor reads both topics from the start with `strategy: first` and auto-commit off, then reads the worker's stored offset. It reports each plan with its results and arrival times, and points out the gap where the worker was killed. Its prompt is in [agents/auditor.md](agents/auditor.md).

What to say: reading is read-only. The auditor never moves an offset, so it can replay this as often as it likes.

### 5. Guardrails

```sh
./run-agent.sh operator "Reset the worker's stored offset on agents/plans partition 0 to 0, then delete the results topic in the agents stream."
```

The operator role uses a neutral prompt that makes the calls rather than refusing up front, on the auditor's MCP server. Both calls fail with `Insufficient 'update' permissions` and `Insufficient 'delete' permissions`. The MCP server refuses them before anything reaches the broker.

```sh
OPERATOR_ON=worker ./run-agent.sh operator "Create a topic called scratch in the agents stream with 1 partition."
```

The worker's MCP server allows create, so this call goes through to the broker. The broker refuses it with `Unauthorized`, because the `worker` user can't manage topics.

## Rehearse

```sh
./rehearse.sh
```

Runs every step above unattended, killing the worker after its second task. It resets the demo first and writes everything to `run/rehearsal.log`. Run it on the machine you'll present from.

## Tear down

```sh
kind delete cluster --name iggy-agents
```

## Notes

- Agent output varies from run to run. Know roughly what to expect, not the exact words.
- The worker sends its result before it stores the offset. A crash between the two means the next worker redoes that task and sends a second result: at-least-once delivery. The auditor reports any plan with more than one result. Storing the offset first gives at-most-once delivery and can lose a task.
- The MCP server's HTTP endpoint has no authentication. Anyone who can reach port 8082 acts as that server's Iggy user. Keep the Services as ClusterIP, add a NetworkPolicy, and don't expose them through an Ingress.
- `send_messages` returns nothing, not even the offsets. An agent can't tell from the result where its message landed.
- Poll results return payloads base64 encoded. The agents decode them without help.
- `poll_messages` with `strategy: next`, or with `auto_commit: true`, stores the offset and needs update permission. The demo uses explicit offsets so each step is visible.
- At the `server-0.9.0` tag the chart's `appVersion` is `0.9.0-edge.6`, and the image tag defaults to it. `k8s/iggy-values.yaml` pins the image to `0.9.0`.
- The MCP server can export OpenTelemetry traces and logs. In 0.5.0 each HTTP request produces one `serve_inner` span from the MCP library with no tool name, so the demo uses the auditor's replay as the trace instead.
- The broker needs `IGGY_SHARDING_CPU_ALLOCATION` set on kind under Docker Desktop, because the default `numa:auto` fails there.
