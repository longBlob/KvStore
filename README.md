# MoniePoint Network Available Key-Value System KvStore

## Description

The solution includes a network available distributed key value store that satisfies the following functional requirements
1. Put(Key, Value)
2. Read(Key)
3. ReadKeyRange(StartKey, EndKey)
4. BatchPut(..keys, ..values)
5. Delete(key) 

The solution also satisfies the  following constraints and non-functional requirements

1. Only use the  standard libraries of the language.
2. Low latency per item read or written
3. High throughput, especially when writing an incoming stream of random items
4. Ability to handle datasets much larger than RAM w/o degradation
5. Crash friendliness, both in terms of fast recovery and not losing data
6. Predictable behavior under heavy access load or large volume
7. Replicate data to multiple nodes


## Description
KVStore is a local key–value storage engine inspired by Bitcask and Log-Structured Merge (LSM) principles.
It uses an append only log structured design with sharding, batching, and background compaction to provide high-throughput, durable storage.

##  How It Works

## Sharded Writes
Incoming key–value writes are partitioned across multiple shards.
Each shard maintains its own append only segment files, write queue, and background flush loop.
This design increases parallelism and reduces lock contention.

## Append-Only Segments
All writes are appended to segment files.
Keys are never updated in place; newer values shadow older ones.
A lightweight in-memory index (Dictionary<string, RecordInfo>) tracks the latest offset for each key.

## Reads
Reads are served directly from the in-memory index.
The engine uses memory mapped files (with fallback to file streams) for fast, random access to values.

## Compaction
A background compaction process rewrites segments to remove stale or deleted keys.
This reduces storage overhead and keeps segments efficient for reads.

## Durability & Recovery
Writes can be configured as synchronous (safer but slower) or asynchronous (faster, eventual durability).
A periodic checkpoint persists the in-memory index.
On startup, the engine scans segments and restores the index, truncating corrupted or partial records if needed.

## Replication Hook
An optional callback allows integration with external replication or message bus systems.
Each write event can be published to remote peers or services.

## Error Handling
Records are validated with CRC32C checksums to ensure data integrity.
If a segment contains partially written or corrupted data, it is automatically truncated on recovery.
Compaction failures do not block writes; affected segments are retried later.

## Patterns / Design principles Used /  Considered 

1. Log Structure Merge
2. BitCask 
3. Raft for Leader Election - Not implemented

## Dependencies

-All projects are created with .NET 8
Build with .NET 8


## Tests

Run unit Tests to verify functionality


## Building and running the app 

Build the application using the .NET Core CLI, which is installed
with [the .NET Core SDK](https://www.microsoft.com/net/download).

change directory into the solution directory

Then run these commands from the CLI from the directory

```console
cd Kvstore
dotnet build
dotnet run
```
Use postman/curl / any api development tool to  make the following requests
1. PUT Request - http://localhost:8080/?key=name&value=FunsoAkinokun
2. GET Request - http://localhost:8080/?key=name
3. DELETE Request - http://localhost:8080/?key=name
4. POST batchPUTRequest http://localhost:8080/batchput json body 
   { "Keys": ["user1","user2","user3","user4","user5"],
   "Values": ["Alice","Bob","Cindy","Dan","Eve"] }
5. GET ReadRange Request - http://localhost:8080/range?start=user1&end=user3


## Testing
Test project is located in project  KvStoreTest



## Assumptions


## Omissions
1. Fail over- not implemented



## Improvement(s)
1. Improved logging and error handling
2. Implement Write Ahead Log
3. Leader election /Fail over / Consensus system using a raft or paxos


## AI Usage
1. I've used AI / LLM (ChatGPT) tools mostly for code formatting, some unit tests, proof read README.md files and as an assistant to code review
 