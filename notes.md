## AI tools used
    Cursor. Mostly used to fix syntax errors and redundant stuff like protobuf generation and some library specific code to get the hang of the library. Also, used it for some ideas on minor improvements and optimizations which were syntax specific.

## Streaming the file through TCP
Streaming the file through TCP, I had a few ideas. First I thought that I would decode the DBN file and stream it through a websocket connection. However, I would have to decode it for every new client connection which would be inefficient. I could decode it once and put it in memory or a ring buffer and stream the content to the client, but that would not really be scalable for larger file sizes. I considered gRPC for stronger typing and performance. In the end I decided to pre‑encode the file into frame buffer batches using Protocol Buffers and stream over gRPC because it reduces the number of messages sent over the network (batching) while keeping serialization fast and compact. I was able to reach a rate of >500k msg per second with this approach. I did a quick benchmark (bench_tcp) with a batch size of 1000 messages and got:
Avg Msg Rate: 551183 msg/s
Avg Batch Rate: 563 batch/s
Avg Throughput: 30.86 MB/s
Duration: 0.07s
Total Messages: 38212
Total Batches: 39
Total Bytes: 2243649 (2.14MB)

 I also considered zero‑copy serialization (e.g., rkyv/Cap’n Proto/FlatBuffers) but chose Protocol Buffers via prost for maturity and because i have used it before.

## Orderbook Reconstruction and Database Writing
1. Used the dbn CLI to quickly peek at the DBN as JSON and understand message shapes.
2. Built the order book with the dbn crate and MboMsg, most of the logic was available in the documentation, so it was easy to implement and only needed minor modifications. There were some invalid cancels which were ignored without blowing up the state.
3. One writer thread and one HTTP server thread, connected by a bounded crossbeam channel, which uses lock-free operations for its core data transfer. In C++, I would have used a lockless queue implementation, and in Rust, crossbeam provides this
4. Checked that sequences are increasing. If there are gaps, I would have to add detection and correction rather than assume order.
5. PostgreSQL for storage with batching. Initially used bulk inserts but to improve performance I switched to COPY FROM STDIN. Drop hot indexes before bulk load and recreate after to keep ingest fast and predictable.
6. HTTP kept minimal with health and snapshot endpoints returning the latest view.
7. Was able to get p99 around 2791 ns and average around 862 ns per message for ingestion.
8. Tried to handle errors gracefully in storage and ingestion.
9. spawned a separate thread to write the MBP snapshots to a file, also using simple crossbeam channel for communication.

## Time taken:
I would say I spent roughly 7-8 hours in total. The order book concepts were not new to me and most of the implementation was available in the documentation, so adapting it took about 2 hours. For the streaming, the logic itself was simple but I did a couple of iterations on the implementation, about 3 hours. The rest went into the database writing logic, a bit of tuning, and some small cleanups.

I have built orderbooks in the past, so the structure overall was not new, but I did spend some time on the implementation to make it more efficient and idiomatic.
