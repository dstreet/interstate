> [!WARNING]
> This is still experimental. Use at your own risk.

# Interstate

Interstate is a Golang library for state replication between any number of local
processes. This is particularly useful for CLI tools where multiple long-lived
commands can be executed and need to share state.

The state can either be persisted so that the state survives multiple
executions, or it can be ephemeral such that when no process is utilizing the
state, it will be removed.

## How it works

Each process using Interstate is considered a node, where each node is either a
leader or a follower. There will always be exactly one leader; all additional
nodes will be operating as a follower. When a request is made to either get or
set the state, the request is forwarded to the leader unless of course the
request is made on the leader node.

Interstate uses optimistic versioning to ensure consistency before writing any
changes to the state. When a write does happen, the change is broadcasted from the
leader to all the followers.

Communication between the leader and followers is done via domain socket using a
binary protocol.

There is no leader election process in Interstate. Instead, each Interstate node
is acts in a greedy manner. Whenever a leader is removed from the system, the
first node to respond then becomes the leader.

## Getting started

```
go get github.com/dstreet/interstate
```

### Creating an ephemeral state system

```go
// Create a new in-memory datastore
store := memory.NewDatastore()

// Create a new Interstate instance where the in-memory datastore is used for
// both the leader node as well as the follower nodes.
//
// The "./" argument is the location of a directory where the
// domain socket will be created.
state := interstate.NewState("./", store, store)

// Open the state, which will start the node
if err := state.Open(); err != nil {
  log.Println("Failed to open state: %w", err)
  return
}
defer state.Close()

// Get the current state version
version, err := state.Current()
if err != nil {
  log.Println("Failed to get initial version: %w", err)
  return
}
```

### Creating a persisted state system

```go
// Create the append-only datastore for the leader node and store the data in the "./data" file
leaderStore := appendonly.NewDatastore("./data")

// Create an in-memory datastore for the follower nodes
followerStore := memory.NewDatastore()

// Create a new Interstate instance where the in-memory datastore is used for
// both the leader node as well as the follower nodes.
//
// The "./" argument is the location of a directory where the
// domain socket will be created.
state := interstate.NewState("./", leaderStore, followerStore)

// Open the state, which will start the node
if err := state.Open(); err != nil {
  log.Println("Failed to open state: %w", err)
  return
}
defer state.Close()

// Get the current state version
version, err := state.Current()
if err != nil {
  log.Println("Failed to get initial version: %w", err)
  return
}
```
