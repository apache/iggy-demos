# Talk to your stream: Iggy and an AI agent

An AI coding agent (Claude Code, Cursor, Claude Desktop) drives Apache Iggy through the Iggy MCP server. No client code, no CLI. Plain-English requests turn into streams, topics, messages and replays.

Runs against Iggy server 0.9.0 and the Iggy MCP server 0.5.0, both from their released Docker images.

Runtime: about 8 minutes. Any MCP client works; the commands below use Claude Code.

## What it shows

- Iggy is easy to stand up and easy to drive from the tools developers already use.
- Keyed partitioning, replay from the start or from a timestamp, and consumer groups, without writing client code.
- Guardrails: the MCP server has its own create, read, update and delete permissions, checked before anything reaches the broker.

## Setup

You need Docker and Claude Code.

1. Start the Iggy server:

```sh
docker compose up -d
```

If port 8090 is taken, set `IGGY_TCP_PORT` to another port. The agent doesn't use it; it's only there if you want to look at the server with the Iggy CLI.

2. Register the MCP server with Claude Code. Claude Code starts it in a container over stdio, on the same network as the Iggy server:

```sh
claude mcp add iggy -- docker run -i --rm --network iggy-mcp-demo \
  -e IGGY_MCP_TRANSPORT=stdio \
  -e IGGY_MCP_IGGY_ADDRESS=iggy:8090 \
  -e IGGY_MCP_IGGY_USERNAME=iggy \
  -e IGGY_MCP_IGGY_PASSWORD=iggy \
  apache/iggy-mcp:0.5.0
```

Run `/mcp` in Claude Code and check `iggy` is connected with its tools listed.

3. Register a second, read-only copy for the guardrails step:

```sh
claude mcp add iggy-readonly -- docker run -i --rm --network iggy-mcp-demo \
  -e IGGY_MCP_TRANSPORT=stdio \
  -e IGGY_MCP_IGGY_ADDRESS=iggy:8090 \
  -e IGGY_MCP_IGGY_USERNAME=iggy \
  -e IGGY_MCP_IGGY_PASSWORD=iggy \
  -e IGGY_MCP_PERMISSIONS_CREATE=false \
  -e IGGY_MCP_PERMISSIONS_UPDATE=false \
  -e IGGY_MCP_PERMISSIONS_DELETE=false \
  apache/iggy-mcp:0.5.0
```

Disable it with `/mcp` until step 6 so the agent doesn't pick it by accident.

## The demo

Paste each prompt as is. What to say while it runs is under each one.

### 1. Look around

> What's running on this Iggy server? Show me the streams and some server stats.

Calls `ping`, `get_streams`, `get_stats`. An empty server, its version, uptime and memory. Point out how small the footprint is.

### 2. Build something

> Create a stream called shop with a topic called orders. Give it 3 partitions and keep messages for 7 days.

Calls `create_stream`, `create_topic`. This is the same config you'd normally write in code or the CLI.

### 3. Generate traffic

> Send 30 realistic e-commerce order events to shop/orders as JSON. Include order id, customer id, items, total in AUD and payment status. Make a few payments fail. Set partitioning to key and messages_key to the customer id, one send per customer, so each customer's orders stay in order.

Calls `send_messages` with key partitioning. The agent makes up the data, which saves you a seed script. Same customer, same partition, so ordering is guaranteed per customer.

### 4. Ask questions of the stream

> Read every message in shop/orders from the start of each partition using the first strategy, without committing offsets. Which customers spent the most, and which orders had failed payments?

Calls `poll_messages` with `strategy: first`, read-only. The stream is the source of truth and the agent is just another consumer.

The tool returns payloads as base64, so the agent writes a short script to decode them and add them up. Without shell access it adds up the totals by hand, which is slower and harder to check on stage.

### 5. Replay

> Note the current time. Then send 5 more orders, one of them a large failed payment. Now show me only what arrived after that time, using the timestamp strategy.

Calls `send_messages`, then `poll_messages` with `strategy: timestamp`. This is the log model: nothing was consumed away, so you can go back to any point.

### 6. Guardrails

Switch `iggy` off and `iggy-readonly` on in `/mcp`.

> Delete the shop stream.

The MCP server refuses before the request reaches the broker. Then:

> OK, just tell me how many messages are in shop/orders.

That still works. Agents can get read access to production streams without the delete button. The broker also enforces the Iggy user's own permissions, so a read-only Iggy user is a second layer.

### 7. Consumer groups (optional)

Switch back to `iggy`.

> Create a consumer group called billing on shop/orders and tell me its members.

Calls `create_consumer_group` and `get_consumer_group`. A lead-in to how real services consume.

## Reset

> Delete the shop stream.

Or start again from nothing:

```sh
docker compose down
docker compose up -d
```

To remove the MCP registrations:

```sh
claude mcp remove iggy
claude mcp remove iggy-readonly
```

## Notes

- Do a full dry run on the machine you'll present from. Agent output varies from run to run, so know roughly what to expect rather than exact output.
- In MCP server 0.5.0 the `strategy` and `partitioning` parameters are described only as "optional, string", and unknown values silently fall back to a default. That's why some prompts name the strategy or partitioning to use.
- `strategy: next` auto-commits offsets and needs update permission, so it fails on the read-only server. Useful if someone asks.
