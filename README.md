<h1 align="center">
    <img src=".github/assets/logo.png" height="300" />
</h1>

> 📱 Decentralized chat service

## 🏃 Running

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
mvn -pl client exec:java -Dexec.args="--name rui --topic <topic> --dht localhost:7000"

You can list the available options for the client by running:

```bash
mvn -pl client exec:java -Dexec.args="--help"
```

> [!CAUTION]
> Currently, we are assuming that both the client and servers are running on the same host (localhost). In the future, we shall add support for pseudo-remote servers.

Running a chat server:

```bash
mvn -pl chat-server exec:java -Dexec.args="--subscriber-ports 5558,6558" # you can also use -sub
```

Note that these are the ports that the SUB socket will connect to, which should have PUB sockets bound to them.

You can list the available options for the chat server by running:

```bash
mvn -pl chat-server exec:java -Dexec.args="--help"
```

> [!NOTE]
> Make sure you understand the role of the `-p` flag, since it is crucial for the server workflow. For example, if `-p` is set to `5555`, the server will bind a PULL socket to it. Subsequent port numbers will be used for PUB sockets.

## 💡 Tips

- Run `mvn compile` whenever a new Protobuf file is added or modified. This will generate the necessary Java classes for the Protobuf messages.
- Run `mvn install` whenever a new dependency has been added or modified. This will update the local Maven repository with the new dependencies.
