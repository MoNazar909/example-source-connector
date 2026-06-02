# Custom Kafka Connector — Complete Beginner Setup Guide

This guide walks you through getting the source and sink connectors in this repo fully running end-to-end. By the end, the source connector will be producing dummy messages to a Kafka topic on Confluent Cloud every second, and the sink connector will be consuming those messages and printing them to logs inside a Kubernetes pod.

**Assumed starting point:** You have an IDE (e.g. IntelliJ, VS Code) and Java installed. Nothing else is assumed.

---

## Table of Contents

1. [Install Required Tools](#1-install-required-tools)
2. [Set Up Confluent Cloud](#2-set-up-confluent-cloud)
3. [Create a Kafka Cluster and Topic](#3-create-a-kafka-cluster-and-topic)
4. [Create API Keys](#4-create-api-keys)
5. [Point connect.yaml at Your Cluster](#5-point-connectyaml-at-your-cluster)
6. [Build the Connector JAR](#6-build-the-connector-jar)
7. [Build the Docker Image](#7-build-the-docker-image)
8. [Start Minikube and Install the CFK Operator](#8-start-minikube-and-install-the-cfk-operator)
9. [Create the Confluent Cloud Credentials Secret](#9-create-the-confluent-cloud-credentials-secret)
10. [Load the Docker Image into Minikube](#10-load-the-docker-image-into-minikube)
11. [Deploy the Connect Cluster](#11-deploy-the-connect-cluster)
12. [Deploy the Source and Sink Connectors](#12-deploy-the-source-and-sink-connectors)
13. [Verify End-to-End](#13-verify-end-to-end)
14. [Useful Commands (Debugging & Cleanup)](#14-useful-commands-debugging--cleanup)

---

## 1. Install Required Tools

You need five tools beyond Java. Use **Chocolatey**, a Windows package manager that installs everything and sets up PATH automatically — no manual downloading or PATH editing required.

### Step 1a — Install Chocolatey

Open **PowerShell as Administrator** and run this command:

Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))


Close and reopen PowerShell as Administrator after it finishes. Verify:

choco --version


### Step 1b — Install All Tools

Still in your **Administrator PowerShell**, run each of the following:

# Maven: build tool that compiles your Java connector code into a JAR
choco install maven -y


# Docker Desktop: builds and runs containers, needed to package your connector image
choco install docker-desktop -y

# kubectl: command-line tool for interacting with Kubernetes clusters
choco install kubernetes-cli -y


# Minikube: runs a local single-node Kubernetes cluster on your machine
choco install minikube -y

# Helm: Kubernetes package manager, used to install the CFK operator
choco install kubernetes-helm -y


Chocolatey adds all of these to your PATH automatically. **Close and reopen your terminal** after installation so the new PATH takes effect.

---

### Step 1c — Verify Everything Installed

Open a **new** PowerShell window (no admin needed from here on) and run:

mvn -version
docker --version
kubectl version --client
minikube version
helm version

All five should print version output without errors. If any one says "command not found", you will have to manually set the PATH.

---


### Step 1d — Launch Docker Desktop

Chocolatey installs Docker Desktop but does not launch it. Find it in your Start menu and open it. It must be **running in the background** for all Docker and Minikube steps that follow. Wait for the Docker whale icon in the system tray to stop animating before continuing.

---

## 2. Set Up Confluent Cloud

### Free Account Trick — Unlimited Accounts from One Email

Confluent Cloud's free trial is tied to your email address. However, if you add `+anything` before the `@` in your email, email providers like Gmail treat it as the same inbox but Confluent Cloud sees it as a completely different account.

For example, if your email is `yourname@gmail.com`, you can sign up with:
- `yourname+confluent1@gmail.com`
- `yourname+test2@gmail.com`
- `yourname+demo3@gmail.com`

All confirmation emails will still arrive in your normal `yourname@gmail.com` inbox. This effectively gives you unlimited free Confluent Cloud accounts from a single email.

### Sign Up

1. Go to https://confluent.cloud/signup
2. Sign up with your email (using the `+` trick if needed)
3. Confirm your email and log in

### Skip the Credit Card — Promo Code

After logging in, before doing anything else:

1. Click your profile/organization icon in the top right
2. Go to **Billing & payment**
3. Find the **Promo code** field
4. Enter: `CONFLUENTDEV1`
5. Click **Apply**

This gives you free credits for 30 days (or until credits are exhausted) and removes the requirement to enter a credit card. Do this before your trial prompt appears.

---

## 3. Create a Kafka Cluster and Topic

### Create a Cluster

1. In the Confluent Cloud console, click **+ Add cluster**
2. Choose **Basic**
3. Choose a cloud provider and region — **AWS us-east-2**
4. Click **Launch cluster**
5. Give it a name (e.g. `my-kafka-cluster`) and click **Launch cluster**

Wait about 1–2 minutes for the cluster to provision.

### Create a Topic

1. In the left sidebar, click **Topics**
2. Click **+ Add topic**
3. Set the topic name to exactly: `dummy-test-topic`
4. Set partitions to `1` (fine for this demo)
5. Click **Create with defaults**

This is the topic the source connector will write to and the sink connector will read from.

---

## 4. Create API Keys

Your Kubernetes connector pod needs credentials to connect to Confluent Cloud. You'll create an API key and secret for this.

1. In the left sidebar, click **API keys** (under your cluster)
2. Click **+ Add key**
3. Select **My account** (fine for a demo/dev setup)
4. Click **Next** — Confluent Cloud will generate a Key and Secret
5. **Important: copy and save both the API Key and API Secret now.** The secret is only shown once.
6. Also note your **Bootstrap server URL** — find it by going to **Cluster overview . It looks like: `pkc-xxxxxx.us-east-2.aws.confluent.cloud:9092`

You will need three values in the steps below:
- **Bootstrap server URL** (e.g. `pkc-921jm.us-east-2.aws.confluent.cloud:9092`)
- **API Key** (e.g. `ABCDEFGH12345678`)
- **API Secret** (e.g. `abc123xyz...longstring...`)

---

## 5. Point connect.yaml at Your Cluster

Open [k8s/connect.yaml](k8s/connect.yaml) in your IDE and find this line:

```yaml
      bootstrapEndpoint: pkc-921jm.us-east-2.aws.confluent.cloud:9092
```

Replace the value with your own **Bootstrap server URL** from step 4. Save the file.

---

## 6. Build the Connector JAR

Open a terminal in the root of the repo (where `pom.xml` lives).

**Step 1 — Compile and package:**
```powershell
mvn clean package
```

This compiles the Java source files and produces two JARs in the `target/` folder. You need the shaded (fat) JAR: `target/example-source-connector-1.0-SNAPSHOT-shaded.jar`.

**Step 2 — Copy the JAR to the plugin folder:**
```powershell
Remove-Item dummy-source-connector-plugin\lib\example-source-connector-1.0-SNAPSHOT-shaded.jar
Copy-Item -Force target\example-source-connector-1.0-SNAPSHOT-shaded.jar dummy-source-connector-plugin\lib\
```

The `dummy-source-connector-plugin/lib/` folder is what Docker will copy into the container image. Keeping the JAR there ensures the Docker image has the latest build.

---

## 7. Build the Docker Image

Make sure Docker Desktop is running. Then build the image:

docker build --no-cache -t generic-http-sink-connector:1.0.1 .

- `--no-cache` forces a fresh build (important after code changes)
- `-t dummy-source-connector:1.0.0` tags the image with a name and version

Verify the image was created:
```powershell
docker images
```

You should see `dummy-source-connector` with tag `1.0.0` in the list.

---

## 8. Start Minikube and Install the CFK Operator

### Start Minikube

```powershell
minikube start
```

This starts a local Kubernetes cluster. First run may take a few minutes as it downloads the VM image.

### Create the Confluent Namespace

All resources in this project live in a namespace called `confluent`:

```powershell
kubectl create namespace confluent
```

Command for deleting namespace:
```powershell
kubectl delete namespace confluent
```

If the delete command gets stuck in terminal, then run this command:
```powershell
kubectl get namespace confluent -o json | python -c "
import json, sys
data = json.load(sys.stdin)
data['spec']['finalizers'] = []
print(json.dumps(data))
" | kubectl replace --raw /api/v1/namespaces/confluent/finalize -f -
```

### Add the Confluent Helm Repository

```powershell
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
```

### Install the CFK Operator

The Confluent for Kubernetes (CFK) operator is what understands the `kind: Connect` and `kind: Connector` resource types in the YAML files:

```powershell
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace confluent
```

Wait about 30 seconds, then verify the operator pod is running:

```powershell
kubectl get pods -n confluent -w
```

You should see a pod named something like `confluent-operator-xxxxxxxxxx-xxxxx` with status `Running`.

---

## 9. Create the Confluent Cloud Credentials Secret

This Kubernetes secret holds your Confluent Cloud API Key and Secret so the connector pod can authenticate without hardcoding credentials in YAML.

In the `plain.txt` file, replace values with the values you copied in step 4:

After that, run the below command to upload the secrets:
```powershell
kubectl create secret generic confluent-cloud-credentials --from-file=plain.txt=plain.txt -n confluent
```


Verify the secret was created:
```powershell
kubectl get secrets -n confluent
```
You should see `confluent-cloud-credentials` in the list.


Run below command to delete a secret:
```powershell
kubectl delete secret confluent-cloud-credentials -n confluent
```

If that command gets stuck in the terminal, then run:
```powershell
'{"metadata":{"finalizers":[]}}' | Out-File -FilePath patch.json -Encoding ascii
kubectl patch secret confluent-cloud-credentials -n confluent --type=merge --patch-file patch.json
```



---

## 10. Load the Docker Image into Minikube

Minikube has its own internal Docker registry separate from your local one. You need to load your image into it so the Kubernetes pod can find it:

```powershell
minikube image load generic-http-sink-connector:1.0.1
```

This may take a minute depending on image size. You can verify it loaded:
```powershell
minikube image ls
```

Look for `docker.io/library/dummy-source-connector:1.0.0` in the list.

---

## 11. Deploy the Connect Cluster

Apply the Connect cluster configuration:

```powershell
kubectl apply -f k8s\connect.yaml
```

This tells the CFK operator to spin up a Kafka Connect pod using your custom Docker image, connected to your Confluent Cloud cluster.

Monitor the pod coming up:
```powershell
kubectl get pods -n confluent -w
```

Wait until the `dummy-source-connect-0` pod shows `1/1 Running`. This typically takes 1–3 minutes. Keep running the command until you see it.

If `dummy-source-connect-0` pod does not show up, run below command to see the error:
```powershell
kubectl describe connect dummy-source-connect -n confluent
```

If the pod is showing up but STATUS is 'Fail' and READY is 0/1, then run below command to see the error:
```powershell
kubectl get pods -n confluent -w
```

Once running, you can verify that your custom connector plugin was loaded successfully:
```powershell
kubectl exec generic-http-sink-connect-0 -n confluent -- curl -s http://localhost:8083/connector-plugins
```

What the command does:
It runs curl -s http://localhost:8083/connector-plugins inside the running pod.
`kubectl exec dummy-source-connect-0 -n confluent` — opens a one-time execution inside that specific pod
`--` — separates kubectl arguments from the command to run inside the pod
`curl -s http://localhost:8083/connector-plugins` — hits the Kafka Connect REST API which returns a list of all loaded connector plugins

In the JSON output, look for entries containing `DummySourceConnector` and `GenericHttpSinkConnector`. If you see them, the pod has your custom classes loaded correctly.

---

## 12. Deploy the Source and Sink Connectors

With the Connect pod running and plugins confirmed, deploy both connector instances:

```powershell
kubectl apply -f k8s\source-connector.yaml
kubectl apply -f k8s\sink-connector.yaml
```

Verify the connectors were created:
```powershell
kubectl get connectors -n confluent -w
```

You should see both `dummy-source-connector` and `generic-bentech-sink-connector` listed, with status `Created` or `Running`.

---

## 13. Verify End-to-End

Stream the logs from the Connect pod to see the sink connector receiving messages:

```powershell
kubectl logs generic-http-sink-connect-0 -n confluent -f
```

Within a few seconds you should see lines like:
```
Received record: dummy-message-0
Received record: dummy-message-1
Received record: dummy-message-2
```

These messages are produced by the source connector, sent through your Confluent Cloud Kafka topic (`dummy-test-topic`), consumed by the sink connector, and printed to the pod logs. End-to-end is working.

Press `Ctrl+C` to stop streaming logs.

You can also verify messages are flowing through Confluent Cloud directly:
1. Go to the Confluent Cloud console
2. Click on your cluster → **Topics** → `dummy-test-topic`
3. Click the **Messages** tab — you should see the `dummy-message-N` messages flowing in

---

## 14. Useful Commands (Debugging & Cleanup)

### Check pod status
```powershell
kubectl get pods -n confluent -w
```

### Stream logs (follow mode)
```powershell
kubectl logs dummy-source-connect-0 -n confluent -f
```

### Check connector instances
```powershell
kubectl get connectors -n confluent
```

### Delete a connector instance (not the pod)
```powershell
kubectl delete connector generic-http-sink-connect -n confluent
kubectl delete connector dummy-source-connector -n confluent
```

### Delete the Connect cluster (tears down the pod)
```powershell
kubectl delete connect dummy-source-connect -n confluent
```

### Rebuild and redeploy after code changes
Run these in sequence — this is the full rebuild cycle:
```powershell
mvn clean package
Remove-Item dummy-source-connector-plugin\lib\example-source-connector-1.0-SNAPSHOT-shaded.jar
Copy-Item -Force target\example-source-connector-1.0-SNAPSHOT-shaded.jar dummy-source-connector-plugin\lib\
kubectl delete connect generic-http-sink-connect-0 -n confluent
minikube image rm docker.io/library/generic-http-sink-connector:1.0.1
docker rmi dummy-source-connector:1.0.0
docker build --no-cache -t dummy-source-connector:1.0.0 .
minikube image load dummy-source-connector:1.0.0
kubectl apply -f k8s\connect.yaml
```

Then wait for the pod to reach `1/1 Running` before reapplying the connector YAMLs.

### Stop Minikube (when done for the day)
```powershell
minikube stop
```

### Resume next time
```powershell
minikube start
kubectl apply -f k8s\connect.yaml
# wait for pod to be 1/1 Running, then:
kubectl apply -f k8s\source-connector.yaml
kubectl apply -f k8s\sink-connector.yaml
```

---

## What Each File Does (Quick Reference)

| File | Purpose |
|------|---------|
| [src/.../DummySourceConnector.java](src/main/java/com/example/connectors/DummySourceConnector.java) | Kafka Connect source connector class — registers the connector with CFK |
| [src/.../DummySourceTask.java](src/main/java/com/example/connectors/DummySourceTask.java) | Produces one dummy string message per second to the Kafka topic |
| [src/.../GenericHttpSinkConnector.java](src/main/java/com/example/connectors/GenericHttpSinkConnector.java) | Kafka Connect sink connector class |
| [src/.../GenericHttpSinkTask.java](src/main/java/com/example/connectors/GenericHttpSinkTask.java) | Consumes messages from the topic and prints them to stdout (pod logs) |
| [pom.xml](pom.xml) | Maven build file — produces the shaded JAR with all dependencies bundled |
| [Dockerfile](Dockerfile) | Packages the JAR into a Confluent Connect image |
| [k8s/connect.yaml](k8s/connect.yaml) | Deploys the Kafka Connect cluster pod via CFK |
| [k8s/source-connector.yaml](k8s/source-connector.yaml) | Creates the source connector instance inside the Connect cluster |
| [k8s/sink-connector.yaml](k8s/sink-connector.yaml) | Creates the sink connector instance inside the Connect cluster |




# Delete application resources
kubectl delete connector workday-eoi-sink -n confluent --ignore-not-found
kubectl delete connect standard-connect -n confluent --ignore-not-found

# Force remove image from inside minikube
minikube ssh -- docker rmi -f docker.io/library/generic-http-sink-connector:1.0.1

# Rebuild and reload
docker build --no-cache -t generic-http-sink-connector:1.0.1 .
minikube image load generic-http-sink-connector:1.0.1

# Redeploy
kubectl apply -f k8s\connect.yaml
kubectl get pods -n confluent -w

# Check logs
kubectl logs standard-connect-0 -n confluent -f
kubectl describe connect standard-connect -n confluent


Once the pod is 1/1 Running:
kubectl apply -f k8s\sink-connector.yaml

kubectl get connectors -n confluent -w

kubectl logs standard-connect-0 -n confluent -f