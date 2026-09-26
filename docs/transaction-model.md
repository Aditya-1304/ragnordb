# Transaction model

## GC protection clock contract

The aggregate GC protection lease uses Unix epoch milliseconds in replicated
metadata. `RAGNORDB_TXN_MAX_CLOCK_SKEW_MS` defines the maximum permitted
pairwise wall-clock difference between any two cluster nodes; it defaults to
30,000 ms. Deployments must synchronize node clocks with chrony or NTP and
monitor measured pairwise clock offset. Alert before the observed offset
approaches the configured bound. Correctness is not guaranteed if the bound is
exceeded or clock synchronization is unavailable.

Safe-point proposals subtract the configured bound from the proposing node's
wall clock before metadata expires leases. A request owner applies the same
bound in the other direction: it refuses protected work when its wall clock
plus the bound reaches the shared lease deadline, and caps each foreground
operation's timeout at that conservative validity horizon. Lease configuration
must leave enough time for the skew allowance, a GC sweep, and its metadata
proposal before renewal is due.

The random owner incarnation remains the fencing identity across process
restarts. Clock skew and owner fencing solve separate problems: a new process
cannot renew an old process's lease, and a fast safe-point proposer cannot
expire a live owner's lease within the configured skew bound.
