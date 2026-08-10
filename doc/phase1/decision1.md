# Phase 1 - Reconnection with Last-Event-ID in-memory buffer

**Overview of the phase:**

![alt text](phase1-overview.png)

**Decision:**

Two ring buffer designs, considered as code implementations:

-Fixed size in entries — the buffer holds the last N events; once full, appending a new one overwrites the oldest.
-Fixed size in bytes — the buffer holds, say, 64KB total. That could be 500 tiny token events or 50 large ones, depending on what's actually flowing through. Eviction is driven by total memory consumed, not by count.

I chose fixed size in entries for its simplicity. The tradeoff: no memory guarantee at all — 100 unusually large events could blow past a memory budget even though the entry count stayed fixed and within limits.

To implement this, I built a factory function that encapsulates the buffer's items. One function appends (overwriting the oldest entry once the max entry count is reached), and another replays events from a given Last-Event-ID.

**Deeper understanding:**

Two problems remain with either approach. First, this is still single-instance and in-memory,so a server restart loses everything. This is the same failure mode as phase 0, just pushed one level up. Second, because the buffer overwrites old entries, a client reconnecting after being gone for too long finds its Last-Event-ID already evicted, and receives a gapped stream rather than a complete one.

We don't use in-process pub/sub (an EventEmitter broadcasting to whoever's currently subscribed) for the live delivery path, because pub/sub is fire-and-forget: it reaches only listeners attached at the moment of emission, and drops the event for everyone else, that including a client that's mid-reconnect. What the ring buffer gives instead is closer to a replayable broadcast: several independent readers, each tracking its own cursor, replaying against one shared history-not a producer/consumer queue where a single consumer drains each item once. This matters going into phase 2: the Live Relay which will use XREAD with exactly this broadcast semantics(every client sees the full log), while only the Flush Job uses XREADGROUP's competing-consumer semantics(each entry divided round-robin across workers).

Reconnection itself works through the GET request: the client resends the request with a Last-Event-ID header, and the server replays from that position. Every SSE event includes an id: field, which the browser's EventSource API stores automatically. If the connection drops and EventSource reconnects, the browser includes that stored ID as the Last-Event-ID header on the new request. The server reads the header and resumes sending from the right position in the stream, avoiding both data loss and duplicate delivery. Because the previous request already sent its FIN, this reconnect is a brand-new TCP connection, not a resumed one.

**Alternatives Considered:**

The byte-sized buffer, rejected for Phase 1 specifically because it adds a second layer (memory) to reason about before the core resume mechanism (Last-Event-ID ->cursor->replay) is even proven out. This is worth while once a real token-size distribution is known. This is added complexity and for this phase the entry count buffer is fine.

**What Broke before the fix:**

A fault is one component deviating from spec; a failure is the whole system stopping when it shouldn't. Phase 0 has nothing between the two- any fault becomes a failure instantly. Every later phase is a fault-tolerance layer. The ring buffer and Last Event ID resume is the actual fault-toleracne mechanism which converts the dropped connection into something that is short of data loss.

**Reading applied:**

-ch8, The Trouble with Distributed Systems - in an asynchronous network, the server can't distinguish 'slow' from 'dead' from silence alone, because no universal timeout value exisits - too short and you get false positives(killing a connection that was just momentarily slow), too long and dead connections sit open, holding resources for nothing.

The chapter's big-picture list of what can go wrong, and how it maps onto this project:

-Unreliable networks- machines communicate over network with no shared memory, only message passing. TCP guarantees reliable, in-order delivery within one connection, but that guarantee doesn't survive the connection itself breaking (cable unplugged, host crash, network partition). When that happens you get a timeout, not confirmation of what was or wasn't received - the sender genuinely doesn't know whether the last packet arrived. In our system: the connection to the AI SDK call in a stream producer component that writes responses directly to the durable log instead of relying on that one fragile connection to deliver them.

-Lost or delayed messages- a packet sent over the network may be lost or delayed; so may the reply. If no reply arrives, the sender has no way to distinguish 'message never arrived' from 'message arrived, but the reply got lost'

-Retransmission can cause duplicates - and the sender still has no way of knowing exactly how much the remote node actually processed before whatever went wrong

-Clock skew - a node's clocks can drift out of sync with other nodes even with NTP (Network Time Protocol) running, and can jump forward or backward. Relying on wall-clock time is risky because you rarely have a good bound on your own clock's error. This is low relevance to this project right now: Redis Stream IDs are centrally generated by one Redis Instance, so ordering doesn't depend on comparing clocks across nodes. This becomes relevant only if the architecture goes multi-instance/multi-region later

-Process pauses - a process can pause for a substantial stretch at any point in its execution (GC pause, OS scheduling, VM suspension), get declared dead by other nodes, and then resume without ever realizing it was paused. The fix for the analogous problem is the Resume Handler : it doesn't try to detect why a client went quiet, it just resumes cleanly from Last-Event-ID whenever it comes back.

The unifying idea is determinism. Concurrency, network delay, process pauses, clock jumps, and crashes are all sources of nondeterminism — and nondeterminism is the root cause behind essentially every distributed-systems failure mode above. Where we get determinism back in this system: replaying a deterministic log of events lets us reconstruct exactly the same state after a network drop, rather than trying to guess what was lost.

**Code practise:**

Mainly used [SSE guide](singhajit.com/server-sent-events-explained) to understand reconnection and wire that logic in sse.ts 

**The phase evolution [about to get interesting ;>]:**
![alt text](phaseevolution.jpg)

**The file structure:**

ai-sse-streamer/
├── src/
│   ├── ai/aiProvider.ts          # unchanged
│   ├── buffer/ringbuffer.ts      # resolve last — delete, keep, or repurpose
│   ├── routes/sse.ts             # wired buffer logic
│   └── server.ts                 # unchanged
├── client/index.html             # unchanged
├── docs/decision.md
└── .env

**The next phase:**

Phase 0 had no memory of anything sent - disconnect and the tokens were just gone. Phase 1 adds two things on top of that same pipeline: a ring buffer that keeps reconnecting client's request and replays from that point in the buffer. The thing that hasn't changed yet is the actual point of the phase - is durability. Everything still lives in one process's memory : a server restart, or a gap longer than the buffer's capacity, loses data exactly the same way Phase 0 did. That gap is what Phase 2 closes by moving the buffer's job onto Redis Streams.
