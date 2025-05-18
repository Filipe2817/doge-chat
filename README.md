<h1 align="center">
    <img src=".github/assets/logo.png" height="300" />
</h1>

> 📱 Decentralized chat service

## 🏃 Running

### ☕️ Java

First, make sure you have the required dependencies installed.

```bash
mvn clean install
```

Running a client:

```bash
mvn -pl client exec:java
```

You can pass arguments to the client using the `-Dexec.args` option. For example, to make the client connect to a specific chat server, you can run:

```bash
mvn -pl client exec:java -Dexec.args="--name rui --topic uminho -as 6667"

You can list the available options for the client by running:

```bash
mvn -pl client exec:java -Dexec.args="--help"
```

> [!CAUTION]
> Currently, we are assuming that both the client and servers are running on the same host (localhost). In the future, we shall add support for pseudo-remote servers.

Running a chat server:

```bash
mvn -pl chat-server exec:java -Dexec.args="-p 5555"
```

You can list the available options for the chat server by running:

```bash
mvn -pl chat-server exec:java -Dexec.args="--help"
```

> [!NOTE]
> Make sure you understand the role of the `-p` flag, since it is crucial for the server workflow. For example, if `-p` is set to `5555`, the server will bind a PULL socket to it. Subsequent port numbers will be used for other types of sockets.

#### 💡 Tips

- Run `mvn compile` whenever a new Protobuf file is added or modified. This will generate the necessary Java classes for the Protobuf messages.
- Run `mvn install` whenever a new dependency has been added or modified. This will update the local Maven repository with the new dependencies.

### 🔴 Erlang

Running a DHT node (from inside the `search-node` directory):

```bash
sh scripts/launch_node.sh n1 # currently, we support nodes from 1 to 5
```

Running a simulated client:

```bash
python scripts/client_simulation.py
```

This will run a simulated client that will connect to the DHT node and send messages to it. The client will also receive messages from the DHT node and print them to the console.
