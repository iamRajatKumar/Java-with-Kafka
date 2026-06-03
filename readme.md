# Kafka Setup on Windows using WSL (Ubuntu) – Local Learning Guide

## Overview

This guide explains how to install and run Apache Kafka locally on **Windows using WSL (Ubuntu)** and connect it with a **Java Producer application**.

This setup uses:

* **Windows 11**
* **WSL (Ubuntu)**
* **Java 21 (Amazon Corretto)**
* **Apache Kafka 4.0.0 (KRaft Mode)**

---

# 1. Install WSL

Open **PowerShell as Administrator**:

```powershell
wsl --install
```

Restart system if required.

Open **Ubuntu** from Start Menu.

Create:

* Username
* Password

Example:

```text
Username: rajat
Password: ********
```

---

# 2. Install Java 21 (Amazon Corretto)

Run:

```bash
wget -O - https://apt.corretto.aws/corretto.key | sudo gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg
```

```bash
echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | sudo tee /etc/apt/sources.list.d/corretto.list
```

Update packages:

```bash
sudo apt-get update
```

Install Java 21:

```bash
sudo apt-get install -y java-21-amazon-corretto-jdk
```

Verify installation:

```bash
java -version
```

---

# 3. Download Kafka

Download Kafka:

```bash
wget https://archive.apache.org/dist/kafka/4.0.0/kafka_2.13-4.0.0.tgz
```

Extract:

```bash
tar -xvzf kafka_2.13-4.0.0.tgz
```

Verify:

```bash
ls
```

Expected:

```text
kafka_2.13-4.0.0
kafka_2.13-4.0.0.tgz
```

---

# 4. Add Kafka Commands to PATH

Open bashrc:

```bash
nano ~/.bashrc
```

Add this at bottom:

```bash
PATH="$PATH:/home/rajat/kafka_2.13-4.0.0/bin"
```

Save:

* CTRL + X
* Y
* ENTER

Apply changes:

```bash
source ~/.bashrc
```

Verify:

```bash
kafka-topics.sh
```

---

# 5. Initialize Kafka Storage (Only First Time)

Generate cluster ID:

```bash
bin/kafka-storage.sh random-uuid
```

Example output:

```text
R8mapmWtSBm6rrbZxUuLXg
```

Format storage:

```bash
cd ~/kafka_2.13-4.0.0
```

```bash
bin/kafka-storage.sh format \
-t YOUR_CLUSTER_ID \
-c config/server.properties \
--standalone
```

---

# 6. Configure Kafka for WSL + Windows Communication

Get WSL IP:

```bash
hostname -I
```

Example:

```text
172.17.110.18
```

Open config:

```bash
nano ~/kafka_2.13-4.0.0/config/server.properties
```

Find:

```properties
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
```

Replace with:

```properties
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://172.17.110.18:9092
```

Important:

Remove any duplicate:

```properties
advertised.listeners=PLAINTEXT://localhost:9092
```

Only one `advertised.listeners` should exist.

---

# 7. Start Kafka

Go to Kafka folder:

```bash
cd ~/kafka_2.13-4.0.0
```

Start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Leave terminal open.

Verify Kafka running:

```bash
jps
```

Expected:

```text
Kafka
Jps
```

Verify port:

```bash
ss -tulnp | grep 9092
```

Expected:

```text
*:9092
```

---

# 8. Create Kafka Topic

Create topic:

```bash
bin/kafka-topics.sh \
--create \
--topic demo_java \
--bootstrap-server localhost:9092
```

List topics:

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# 9. Start Consumer

Open new terminal:

```bash
cd ~/kafka_2.13-4.0.0
```

Run consumer:

```bash
bin/kafka-console-consumer.sh \
--topic demo_java \
--from-beginning \
--bootstrap-server localhost:9092
```

---

# 10. Java Producer Configuration

Producer config:

```java
properties.setProperty(
    "bootstrap.servers",
    "172.17.110.18:9092"
);

properties.setProperty(
    "key.serializer",
    StringSerializer.class.getName()
);

properties.setProperty(
    "value.serializer",
    StringSerializer.class.getName()
);
```

Create topic record:

```java
ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(
                "demo_java",
                "Hello World"
        );
```

Send message:

```java
producer.send(producerRecord);

producer.flush();
producer.close();
```

---

# 11. Verify Message

Consumer terminal should display:

```text
Hello World
```

---

# Useful Commands

### Check Kafka Running

```bash
jps
```

### Stop Kafka

```bash
CTRL + C
```

### Kill Kafka Forcefully

```bash
kill -9 PID
```

### Check Kafka Port

```bash
ss -tulnp | grep 9092
```

### Get WSL IP

```bash
hostname -I
```

### List Topics

```bash
kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# Common Errors & Fixes

## Error

```text
No readable meta.properties files found
```

### Fix

Kafka storage not initialized.

Run:

```bash
bin/kafka-storage.sh format \
-t CLUSTER_ID \
-c config/server.properties \
--standalone
```

---

## Error

```text
Connection to node localhost:9092 could not be established
```

### Fix

Set:

```properties
advertised.listeners=PLAINTEXT://YOUR_WSL_IP:9092
```

And restart Kafka.

---

## Error

```text
Broker may not be available
```

### Fix

Kafka server is not running.

Check:

```bash
jps
```

Start Kafka again.
