mvn clean package
cp target/generic-http-sink-connector-1.0-SNAPSHOT-shaded.jar generic-http-sink-connector-plugin/lib/
docker compose restart connect
# once "Kafka Connect started" appears:
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connector-config.json"


# Local Development & Testing Guide

Instead of rebuilding a Docker image and redeploying to Kubernetes for every change, this setup runs the Kafka Connect worker locally via Docker Compose. You only rebuild the JAR — no image builds, no kubectl.

---

## Prerequisites

- Docker Desktop running
- Java Extension Pack for VS Code installed (`Ctrl+Shift+X` → search "Extension Pack for Java")

---

## First-Time Setup

### 1. Copy the example files

```
cp docker-compose.yml.example docker-compose.yml
cp connector-config.json.example connector-config.json
```

### 2. Fill in `.env`

Copy the template and fill in the **`standard-connect-sa`** credentials. This file is read by Docker Compose to configure the Connect worker container.

```
cp .env.example .env
```

| Field | What to put |
|-------|-------------|
| `KAFKA_BOOTSTRAP_SERVER` | Bootstrap server from your Confluent Cloud cluster (e.g. `pkc-xxx.us-east-2.aws.confluent.cloud:9092`) |
| `CONNECT_SA_KAFKA_API_KEY` | API key for `standard-connect-sa` (`standard-connect-sa-kafka-api-key`) |
| `CONNECT_SA_KAFKA_API_SECRET` | Corresponding secret |

### 3. Fill in `connector-config.json`

This file is POSTed to the Connect REST API to register the connector. It is read by the Connect framework — not Docker Compose or your Java code. Fill in the **`standard-workday-connector-sa`** SR credentials.

| Placeholder | What to put |
|-------------|-------------|
| `<KAFKA_TOPIC>` | `standard-eda-bentechdata-workday` |
| `<SCHEMA_REGISTRY_URL>` | Schema Registry endpoint from Confluent Cloud (e.g. `https://psrc-xxx.us-east-2.aws.confluent.cloud`) |
| `<SCHEMA_REGISTRY_API_KEY>` | API key for `standard-workday-connector-sa` (`standard-workday-connector-sa-sr-api-key`) |
| `<SCHEMA_REGISTRY_API_SECRET>` | Corresponding secret |
| `<CONTENT_TYPE>` | `application/xml` |
| `<KAFKA_BOOTSTRAP_SERVER>` | Same bootstrap server as in `.env` |
| `<CONNECTOR_SA_KAFKA_API_KEY>` | API key for `standard-workday-connector-sa` (`standard-workday-connector-sa-kafka-api-key`) |
| `<CONNECTOR_SA_KAFKA_API_SECRET>` | Corresponding secret |

### 4. Create the `secrets/` directory

The connector's Java code reads Kafka and Schema Registry credentials from files mounted at `/etc/secrets`. Locally, the `secrets/` folder is mounted into the container in its place.

Create the following files (no file extension, plain text, value only — no newlines):

```
secrets/
├── ccloud-kafka-credentials/
│   ├── username        ← standard-workday-connector-sa-kafka-api-key  (the key itself)
│   └── password        ← corresponding secret
└── ccloud-sr-credentials/
    ├── username        ← standard-workday-connector-sa-sr-api-key  (the key itself)
    └── password        ← corresponding secret
```

PowerShell commands to create them:

```powershell
New-Item -ItemType Directory -Force secrets\ccloud-kafka-credentials
New-Item -ItemType Directory -Force secrets\ccloud-sr-credentials

Set-Content secrets\ccloud-kafka-credentials\username "<CONNECTOR_SA_KAFKA_API_KEY>" -NoNewline
Set-Content secrets\ccloud-kafka-credentials\password "<CONNECTOR_SA_KAFKA_API_SECRET>" -NoNewline
Set-Content secrets\ccloud-sr-credentials\username    "<CONNECTOR_SA_SR_API_KEY>"    -NoNewline
Set-Content secrets\ccloud-sr-credentials\password    "<CONNECTOR_SA_SR_API_SECRET>"  -NoNewline
```

> `secrets/` is gitignored — these files will never be committed.

### 5. Pre-create the Connect internal topics in Confluent Cloud

The `standard-connect-sa` has no `CREATE` permission, so the worker cannot auto-create its internal topics. Create these three topics manually in the Confluent Cloud UI or CLI before starting the worker:

| Topic | Partitions | Replication factor |
|-------|------------|--------------------|
| `standard-connect-configs` | 1 | 3 |
| `standard-connect-offsets` | 25 | 3 |
| `standard-connect-status` | 5 | 3 |

---

## Running the Connector

### 1. Build the JAR

Compiles your Java code into a JAR and places it where Docker Compose can pick it up.

```
mvn clean package
cp target/generic-http-sink-connector-1.0-SNAPSHOT-shaded.jar generic-http-sink-connector-plugin/lib/
```

### 2. Start the Connect worker

Starts the Kafka Connect worker in a local Docker container. It connects to Confluent Cloud and exposes the REST API on port 8083.

```
docker compose up
```

Wait until you see `Kafka Connect started` in the output before proceeding.

### 3. Register the connector

Tells the Connect worker which connector class to run, which topic to consume, and how to deserialize the Avro messages.

```
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connector-config.json"
```

### 4. Watch for messages

Messages from the topic will print directly in the `docker compose up` terminal in this format:

```
[standard-eda-bentechdata-workday][partition=3][offset=42] key=WMT-12345-... value=Struct{field1=value1, ...}
```

---

## After Making a Code Change

No need to rebuild the Docker image. Just rebuild the JAR and restart the container — the JAR is mounted as a volume so the container picks it up immediately.

```
mvn clean package
cp target/generic-http-sink-connector-1.0-SNAPSHOT-shaded.jar generic-http-sink-connector-plugin/lib/
docker compose restart connect
```

Then re-register the connector (Step 3 above). The worker loses its connector registrations on restart.

---

## Debugging

The Connect worker always starts with the debug port open (port 5005). You attach VS Code to it whenever you want to pause and inspect live data.

### Set a breakpoint

In [GenericHttpSinkTask.java](src/main/java/com/example/connectors/GenericHttpSinkTask.java), click in the gutter (left of the line numbers) next to the `System.out.printf` line inside `put()`. A red dot appears — execution will pause here every time a message arrives.

### Attach the debugger

Press `F5` → select **Attach to Connect**. A debug toolbar appears at the top of VS Code. The debugger is now connected.

### When a message arrives

VS Code highlights the breakpoint line in yellow — the program is paused. In the **Variables** panel on the left you can inspect the live values:

- Expand `record` → see `topic`, `kafkaPartition`, `kafkaOffset`
- Expand `record → key` → see the deserialized Avro key fields
- Expand `record → value` → see every Avro field and its value

### Debugger controls

| Action | Shortcut | Description |
|--------|----------|-------------|
| Continue | `F5` | Resume until the next message hits the breakpoint |
| Step Over | `F10` | Run the current line, pause on the next one |
| Step Into | `F11` | Follow execution inside a method call |
| Step Out | `Shift+F11` | Finish the current method and pause on the caller |
| Stop | `Shift+F5` | Detach the debugger |

### Remove a breakpoint

Click the red dot again to toggle it off. The program will no longer pause there.

---

## Troubleshooting

**Messages not appearing**
Check if the Kubernetes connector is still running and competing for the same consumer group:
```
kubectl get connectors -n confluent
```
If it is, delete it: `kubectl delete connector generic-bentech-sink-connector -n confluent`

**Check connector status**
```
curl.exe http://localhost:8083/connectors/workday-eoi-sink/status
```

**Delete and re-register the connector**
```
curl.exe -X DELETE http://localhost:8083/connectors/workday-eoi-sink
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connector-config.json"
```
