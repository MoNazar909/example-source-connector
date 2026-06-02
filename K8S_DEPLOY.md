# Kubernetes Deployment Guide — Workday HTTP Sink Connector

This guide covers everything needed to deploy the connector to Kubernetes using CFK (Confluent for Kubernetes) on minikube, connecting to Confluent Cloud.

---

## Prerequisites

- Java + Maven installed
- Docker Desktop running
- minikube installed and running (`minikube start`)
- kubectl installed and pointing at minikube
- Helm installed
- CFK operator already installed in the `confluent` namespace
- A Confluent Cloud account with a Kafka cluster and Schema Registry

---

## 1. Confluent Cloud — Service Accounts

Two service accounts are required. These are managed via Terraform in this repo but can also be created manually.

### `standard-connect-sa`
Used by the **Connect worker pod** to authenticate to the Kafka broker and manage internal Connect topics.

**Role bindings required:**

| Role | Resource type | Resource name |
|------|--------------|---------------|
| DeveloperRead | Topic | `standard-connect-configs` |
| DeveloperWrite | Topic | `standard-connect-configs` |
| DeveloperRead | Topic | `standard-connect-offsets` |
| DeveloperWrite | Topic | `standard-connect-offsets` |
| DeveloperRead | Topic | `standard-connect-status` |
| DeveloperWrite | Topic | `standard-connect-status` |
| DeveloperRead | Consumer Group | `standard-connect` (LITERAL) |
| DeveloperRead | Topic | `_confluent-license` |
| DeveloperWrite | Topic | `_confluent-license` |
| CloudClusterAdmin | Cluster | *(required for license validation by cp-server-connect)* |

**API keys needed:**
- One Kafka API key (used in K8s secret `connect-sa-kafka-key`)

---

### `standard-workday-connector-sa`
Used by the **connector task** for consuming the input topic and producing to output topics.

**Role bindings required:**

| Role | Resource type | Resource name |
|------|--------------|---------------|
| DeveloperRead | Topic | `standard-eda-bentechdata-workday` |
| DeveloperRead | Schema subject | `standard-eda-bentechdata-workday-*` |
| DeveloperWrite | Topic | `standard-eda-bentechdata-workday-response` |
| DeveloperRead | Schema subject | `standard-eda-bentechdata-workday-response-*` |
| DeveloperWrite | Schema subject | `standard-eda-bentechdata-workday-response-*` |
| DeveloperWrite | Topic | `standard-eda-bentechdata-workday-error` |
| DeveloperRead | Schema subject | `standard-eda-bentechdata-workday-error-*` |
| DeveloperWrite | Schema subject | `standard-eda-bentechdata-workday-error-*` |
| DeveloperWrite | Topic | `standard-eda-bentechdata-workday-dlq` |
| DeveloperRead | Schema subject | `standard-eda-bentechdata-workday-dlq-*` |
| DeveloperWrite | Schema subject | `standard-eda-bentechdata-workday-dlq-*` |
| DeveloperRead | Consumer Group | `connect-workday-eoi-sink` (PREFIXED) |

**API keys needed:**
- One Kafka API key (used in `secrets/ccloud-kafka-credentials/` and `consumer.override` in connector config)
- One Schema Registry API key (used in `secrets/ccloud-sr-credentials/` and converter auth in connector config)

---

## 2. Confluent Cloud — Topics to Pre-Create

The `standard-connect-sa` has no CREATE permission, so these topics must exist before deploying.

### Internal Connect topics

| Topic | Partitions | Replication factor |
|-------|------------|--------------------|
| `standard-connect-configs` | 1 | 3 |
| `standard-connect-offsets` | 25 | 3 |
| `standard-connect-status` | 5 | 3 |

### License topic

| Topic | Partitions | Replication factor | Cleanup policy |
|-------|------------|--------------------|----------------|
| `_confluent-license` | 1 | 3 | compact |

> The `_confluent-license` topic is required by `cp-server-connect`'s license manager. Create it in the Confluent Cloud UI under **Topics → + Add topic**.

### Application topics

These must also exist before the connector processes messages:

| Topic | Notes |
|-------|-------|
| `standard-eda-bentechdata-workday` | Input topic — consumed by the connector |
| `standard-eda-bentechdata-workday-response` | Success responses from Workday |
| `standard-eda-bentechdata-workday-error` | Workday business errors |
| `standard-eda-bentechdata-workday-dlq` | Dead letter queue |

---

## 3. Build the JAR and Docker Image

```powershell
# Build the shaded JAR
mvn clean package
cp target/generic-http-sink-connector-1.0-SNAPSHOT-shaded.jar generic-http-sink-connector-plugin/lib/

# Build the Docker image
docker build --no-cache -t generic-http-sink-connector:1.0.1 .

# Load it into minikube (minikube must be running)
minikube image load generic-http-sink-connector:1.0.1
```

---

## 4. Create Kubernetes Secrets

All secrets go in the `confluent` namespace.

### 4a. Connect worker Kafka auth (`connect-sa-kafka-key`)

Used by CFK to authenticate the Connect worker to the Kafka broker.
The `plain.txt` value must be two lines: `username=<key>` and `password=<secret>`.

```powershell
kubectl create secret generic connect-sa-kafka-key `
  --from-literal=plain.txt="username=<CONNECT_SA_KAFKA_API_KEY>`npassword=<CONNECT_SA_KAFKA_API_SECRET>" `
  -n confluent
```

> Values come from the Terraform output `connect_kafka_key_id` / `connect_kafka_key_secret`.

---

### 4b. Connector SA Kafka credentials (`ccloud-kafka-credentials`)

Mounted at `/mnt/secrets/ccloud-kafka-credentials/` inside the pod.
Read by the connector's internal KafkaProducer (for response/error/dlq topics).

```powershell
kubectl create secret generic ccloud-kafka-credentials `
  --from-literal=username=<CONNECTOR_SA_KAFKA_API_KEY> `
  --from-literal=password=<CONNECTOR_SA_KAFKA_API_SECRET> `
  -n confluent
```

> Values come from Terraform output `workday_connector_kafka_key_id` / `workday_connector_kafka_key_secret`.

---

### 4c. Connector SA Schema Registry credentials (`ccloud-sr-credentials`)

Mounted at `/mnt/secrets/ccloud-sr-credentials/` inside the pod.
Read by the connector's internal KafkaProducer for Avro serialization.

```powershell
kubectl create secret generic ccloud-sr-credentials `
  --from-literal=username=<CONNECTOR_SA_SR_API_KEY> `
  --from-literal=password=<CONNECTOR_SA_SR_API_SECRET> `
  -n confluent
```

> Values come from Terraform output `workday_connector_sr_key_id` / `workday_connector_sr_key_secret`.

---

### 4d. Workday tenant OAuth credentials (`bentech-tenant-creds`)

Mounted at `/mnt/secrets/bentech-tenant-creds/` inside the pod.
Contains OAuth credentials per `groupId`. Add one set of keys per employer group.

```powershell
kubectl create secret generic bentech-tenant-creds `
  --from-literal=WMT-12345_clientId=<WORKDAY_CLIENT_ID> `
  --from-literal=WMT-12345_clientsecret=<WORKDAY_CLIENT_SECRET> `
  --from-literal=WMT-12345_refreshtoken=<WORKDAY_REFRESH_TOKEN> `
  -n confluent
```

> For multiple groups, add additional `--from-literal` entries with the appropriate `groupId` prefix (e.g. `WMT-99999_clientId`).

---

## 5. Fill in `k8s/sink-connector.yaml`

The file is gitignored. Copy from the example and fill in credentials:

```powershell
cp k8s\sink-connector.yaml.example k8s\sink-connector.yaml
```

Edit `k8s/sink-connector.yaml` and replace all placeholders:

| Placeholder | Value |
|-------------|-------|
| `<SCHEMA_REGISTRY_URL>` | SR endpoint (e.g. `https://psrc-xxx.us-east-2.aws.confluent.cloud`) |
| `<CONNECTOR_SA_SR_API_KEY>` | `workday_connector_sr_key_id` from Terraform |
| `<CONNECTOR_SA_SR_API_SECRET>` | `workday_connector_sr_key_secret` from Terraform |
| `<CONNECTOR_SA_KAFKA_API_KEY>` | `workday_connector_kafka_key_id` from Terraform |
| `<CONNECTOR_SA_KAFKA_API_SECRET>` | `workday_connector_kafka_key_secret` from Terraform |
| `<KAFKA_BOOTSTRAP_SERVER>` | Bootstrap server (e.g. `pkc-xxx.us-east-2.aws.confluent.cloud:9092`) |

Also update the `bootstrapEndpoint` in `k8s/connect.yaml` to your cluster's bootstrap server.

---

## 6. Deploy the Connect Cluster

```powershell
kubectl apply -f k8s\connect.yaml
kubectl get pods -n confluent -w
```

Wait for `standard-connect-0` to reach `1/1 Running`. This takes 2–3 minutes as the pod scans all plugins.

---

## 7. Deploy the Connector

```powershell
kubectl apply -f k8s\sink-connector.yaml
kubectl get connectors -n confluent -w
```

Wait for `workday-eoi-sink` to show `Running`.

---

## 8. Check Logs

```powershell
kubectl logs standard-connect-0 -n confluent -f
```

A healthy message looks like:

```
[standard-eda-bentechdata-workday][partition=3][offset=42] key=Struct{kafka_key=WMT-12345-...} groupId=WMT-12345 apiUrl=https://...
Token refreshed for group: WMT-12345
Written to topic: standard-eda-bentechdata-workday-response
```

---

## 9. Useful Commands

```powershell
# Check connector and task status
kubectl get connectors -n confluent

# Describe connector for detailed status/errors
kubectl describe connector workday-eoi-sink -n confluent

# Restart a failed task
kubectl exec standard-connect-0 -n confluent -- curl -s -X POST http://localhost:8083/connectors/workday-eoi-sink/tasks/0/restart

# Delete and re-register the connector
kubectl delete connector workday-eoi-sink -n confluent
kubectl apply -f k8s\sink-connector.yaml

# Full teardown
kubectl delete connector workday-eoi-sink -n confluent --ignore-not-found
kubectl delete connect standard-connect -n confluent --ignore-not-found
minikube ssh -- docker rmi -f docker.io/library/generic-http-sink-connector:1.0.1

# Force remove a stuck resource (stuck in Terminating due to finalizer)
'{"metadata":{"finalizers":[]}}' | Out-File -FilePath patch.json -Encoding ascii
kubectl patch connect standard-connect -n confluent --type=merge --patch-file patch.json
```

---

## 10. Secret Summary

| K8s Secret name | Contents | Used by |
|-----------------|----------|---------|
| `connect-sa-kafka-key` | `standard-connect-sa` Kafka API key | CFK Connect worker broker auth |
| `ccloud-kafka-credentials` | `standard-workday-connector-sa` Kafka API key | Connector's internal KafkaProducer |
| `ccloud-sr-credentials` | `standard-workday-connector-sa` SR API key | Connector's internal KafkaProducer Avro serializer |
| `bentech-tenant-creds` | Workday OAuth credentials per groupId | `WorkdayApiClient` token refresh |
