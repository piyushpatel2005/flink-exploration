# Set up Flink

This tutorial explains how to set up a Flink cluster locally in standalone mode. 

Flink can be used in standalone mode for development and testing. For production use cases, Flink can be deployed on 
a cluster manager like Apache Mesos, Kubernetes, or Apache YARN. In order to set up Flink locally, you will need to 
have UNIX-like environment. You can use Linux, MacOS, or Windows Subsystem for Linux (WSL) on Windows.

## 1. Install Java

Before installing Flink, you need Java JDK installed in your system. Make sure to install Java from OpenJDK or 
Oracle website and set the `JAVA_HOME` environment variable. Make sure to install Java 8 or higher.

```bash
# Check if Java is installed
java -version
```

## 2. Download Flink

Download the latest version of Flink from the [official website](https://flink.apache.org/downloads).


## 3. Start Flink Cluster

Once you have extracted the Flink tarball, you can start the Flink cluster by running the following command.

```bash
# Start Flink cluster
cd flink-1.20.0
./bin/start-cluster.sh
```

After executing this command, you can visit `http://localhost:8081` in your browser to see the Flink dashboard. This 
is where you can monitor the Flink jobs nd see the status of the cluster.

## 5. Submit Flink Job

You can submit a Flink job to the cluster by running the following command.

```bash
# Submit Flink job
./bin/flink run examples/streaming/WordCount.jar
```

## 5. Stop Flink Cluster

To stop the Flink cluster, you can run the following command.

```bash
# Stop Flink cluster
./bin/stop-cluster.sh
```