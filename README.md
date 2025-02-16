# SO2
Segundo projeto da cadeira de sistemas operativos - 2º ano de faculdade

IST-KVS Project (2024-25)
Overview

This project focuses on implementing an accessible Key-Value Store (IST-KVS) where client processes can monitor and interact with key-value pairs using named pipes for communication with the server.
Key Features:

    Client-Server Communication: Clients can connect to the server via named pipes for monitoring and interacting with key-value pairs.
    Session Management: Clients can initiate and terminate sessions with the server, subscribe/unsubscribe to key-value changes.
    Concurrency: The server handles multiple sessions and clients concurrently, ensuring thread-safe operations.
    Notifications: Clients receive real-time updates when subscribed key-value pairs change.

Setup
Prerequisites

    C Programming Language
    POSIX APIs (for pipes, signals, etc.)

Running the Server

Start the server with the following command:

./kvs <dir_jobs> <max_threads> <backups_max> <name_of_FIFO>

    dir_jobs: Directory containing job files to process.
    max_threads: Max number of tasks the server can handle.
    backups_max: Max number of concurrent backups.
    name_of_FIFO: Name of the FIFO pipe for client-server communication.

Running the Client

Start the client with the following command:

./client <client_id> <name_of_FIFO>

    client_id: Unique identifier for the client.
    name_of_FIFO: Name of the FIFO pipe to connect to the server.

Key Operations
Client Operations

    Connect: Establishes a session with the server.
    Disconnect: Terminates the session.
    Subscribe: Subscribes to updates for a given key.
    Unsubscribe: Unsubscribes from updates for a given key.
    Delay: Adds a delay (in seconds) for testing.

Server Operations

    Handles client requests for session management, subscribing/unsubscribing, and notifying clients of key changes.

Usage

    Start the server.
    Clients can connect by providing their FIFO pipes and session ID.
    Clients can subscribe to key-value pairs and receive updates in real-time.
    Clients can disconnect, and the server will manage the sessions and clean up.

Contributing

    Fork the repository.
    Create a new branch (git checkout -b feature/your-feature).
    Commit your changes (git commit -am 'Add new feature').
    Push to the branch (git push origin feature/your-feature).
    Create a new Pull Request.

Grade

20.01/20.50
