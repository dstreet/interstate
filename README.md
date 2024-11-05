# Interstate

A minimal, persistent, key-value store for Golang.

**Note**

Interstate is not meant for heavy workloads or a large number of key-value
pairs. It is, essentially, just a hashmap implemented in the filesystem.

## Basic Usage

With write operations, `Put` and `Delete`, the key is locked so no other
goroutine or process can write to the same key.

```go
store := interstate.NewStore("/tmp/interstate")

if err := store.Open(); err != nil {
  panic(err)
}

key := "my.first.key"
msg := "hello, world"

if err := store.Put(key, string(msg)); err != nil {
  panic(err)
}

fmt.Printf("Stored key %q with value: %s\n", key, msg)

data, err := store.Get(key)
if err != nil {
  panic(err)
}

fmt.Printf("Retrieved key %q with value: %s\n", key, string(data))

if err := store.Delete(key); err != nil {
  panic(err)
}

fmt.Printf("Deleted key %q\n", key)

// Data directory will be deleted on close, so only do this for ephemeral data.
if err := store.Close(); err != nil {
  panic(err)
}
```

## Advanced Usage

The lock for a key can also be held so that multiple write operations can be
performed before releasing the lock.

```go
store := interstate.NewStore("/tmp/interstate")

if err := store.Open(); err != nil {
  panic(err)
}
defer store.Close()

key := "my.first.key"

// Updater will acquire a lock on the key. If a lock is already held for the
// key, then an error will be returned.
updater, err := store.Updater(key)
if err != nil && errors.Is(err, interstate.ErrKeyLocked) {
  log.Fatalf("The key %q is already locked", key)
}

if err != nil {
  panic(err)
}

for i := 1; i++; i <= 10 {
  msg := fmt.Sprintf("Message #%d", i)

  if err := updater.Put([]byte(msg)); err != nil {
    panic(err)
  }

  fmt.Printf("Stored key %q with value: %s\n", key, msg)
}

if err := store.Delete(key); err != nil {
  panic(err)
}

fmt.Printf("Deleted key %q\n", key)

// Close will release the lock on the key
if err := updater.Close(); err != nil {
  panic(err)
}
```
In the above example, if a lock is already held on the key, `Updater` will
immediately return an error. However, sometimes it may be needed to wait for the
lock to be available. `Updater` can be configured to handle that case:

```go
updater, err := store.Updater(key, interstate.WithWaitForLock())
```

A timeout can also be set. If the timeout is exceeded, `Updater` will return a
`ErrLockTimeout` error:

```go
updater, err := store.Updater(
  key,
  interstate.WithWaitForLock(),
  // Will timeout after 5 seconds
  interstate.WithWaitTimeout(5 * time.Second)
)

if err != nil && errors.Is(err, interstate.ErrLockTimeout) {
  log.Fatalf("Lock timeout was exceeded")
}
```

When waiting for the lock, Interstate will poll the filesystem because locks are
stored as empty files. The polling interval defaults to 100ms, but can also be
configured:

```go
updater, err := store.Updater(
  key,
  interstate.WithWaitForLock(),
  // Will poll for the lock every 50ms
  interstate.WithWithPollingInterval(50 * time.Millisecond)
)
```