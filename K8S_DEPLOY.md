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

### Schema Registry — register schemas manually

After creating the topics, register the Avro schemas in the Confluent Cloud Schema Registry UI (**Schema Registry → Subjects → + Add schema**). Schema files are in [schemas/](schemas/).

| Topic | Key subject | Key schema file | Value subject | Value schema file |
|-------|-------------|-----------------|---------------|-------------------|
| `standard-eda-bentechdata-workday` | `standard-eda-bentechdata-workday-key` | [kafka_key.avsc](schemas/kafka_key.avsc) | `standard-eda-bentechdata-workday-value` | [flink_bentechsink_workday.avsc](schemas/flink_bentechsink_workday.avsc) |
| `standard-eda-bentechdata-workday-response` | `standard-eda-bentechdata-workday-response-key` | [kafka_key.avsc](schemas/kafka_key.avsc) | `standard-eda-bentechdata-workday-response-value` | [bentechsink_response.avsc](schemas/bentechsink_response.avsc) |
| `standard-eda-bentechdata-workday-error` | `standard-eda-bentechdata-workday-error-key` | [kafka_key.avsc](schemas/kafka_key.avsc) | `standard-eda-bentechdata-workday-error-value` | [bentechsink_response.avsc](schemas/bentechsink_response.avsc) |
| `standard-eda-bentechdata-workday-dlq` | `standard-eda-bentechdata-workday-dlq-key` | [kafka_key.avsc](schemas/kafka_key.avsc) | `standard-eda-bentechdata-workday-dlq-value` | [bentechsink_response.avsc](schemas/bentechsink_response.avsc) |

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

Copy the example file, fill in the values, then create the secret:

```powershell
cp plain.txt.example plain.txt
```

Edit `plain.txt` — replace the placeholders with values from Terraform output `connect_kafka_key_id` / `connect_kafka_key_secret`:

```
username=WNTHHJU6BSQC2H3Q
password=cfltKDDkpbRD+2brTg3kCQ+...
```

Then create the secret:

```powershell
kubectl create secret generic connect-sa-kafka-key `
  --from-file=plain.txt=plain.txt `
  -n confluent
```

> `plain.txt` is gitignored — it will never be committed.

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
One key per employer group, named `client-creds-{groupId}`, value is `clientId:clientSecret:refreshToken` (colon-separated).

```powershell
kubectl create secret generic bentech-tenant-creds `
  --from-literal=client-creds-WMT-12345="<WORKDAY_CLIENT_ID>:<WORKDAY_CLIENT_SECRET>:<WORKDAY_REFRESH_TOKEN>" `
  -n confluent
```

> To add more groups, append additional `--from-literal` entries (e.g. `--from-literal=client-creds-ABC-999="..."`) and recreate the secret.
>
> Alternatively, apply [k8s/bentech-tenant-creds.yaml](k8s/bentech-tenant-creds.yaml) directly — add one `stringData` key per group and run `kubectl apply`.

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

## 10. Rebuild and Redeploy

Use this after any code change that requires a new image in minikube.

```powershell
# Delete application resources
kubectl delete connector workday-eoi-sink -n confluent --ignore-not-found
kubectl delete connect standard-connect -n confluent --ignore-not-found

# Force remove image from inside minikube
minikube ssh -- docker rmi -f docker.io/library/generic-http-sink-connector:1.0.1

# Rebuild and reload
mvn clean package
Copy-Item -Force target\generic-http-sink-connector-1.0-SNAPSHOT-shaded.jar generic-http-sink-connector-plugin\lib\
docker build --no-cache -t generic-http-sink-connector:1.0.1 .
minikube image load generic-http-sink-connector:1.0.1

# Redeploy Connect cluster
kubectl apply -f k8s\connect.yaml
kubectl get pods -n confluent -w

# Check logs while pod is starting
kubectl logs standard-connect-0 -n confluent -f
kubectl describe connect standard-connect -n confluent
```

Once the pod is `1/1 Running`:

```powershell
kubectl apply -f k8s\sink-connector.yaml
kubectl get connectors -n confluent -w
kubectl logs standard-connect-0 -n confluent -f
```

---

## 11. Secret Summary

| K8s Secret name | Contents | Used by |
|-----------------|----------|---------|
| `connect-sa-kafka-key` | `standard-connect-sa` Kafka API key | CFK Connect worker broker auth |
| `ccloud-kafka-credentials` | `standard-workday-connector-sa` Kafka API key | Connector's internal KafkaProducer |
| `ccloud-sr-credentials` | `standard-workday-connector-sa` SR API key | Connector's internal KafkaProducer Avro serializer |
| `bentech-tenant-creds` | One key per groupId: `client-creds-{groupId}` = `clientId:clientSecret:refreshToken` | Connector token refresh per employer group |
