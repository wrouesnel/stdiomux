
### Changes from upstream

Only the sender changes: either end can be stock yamux.

- Data frames queue apart from control frames (window updates, pings, opens and
  resets), which go first. Each stream has one data frame queued at a time, so streams
  take turns rather than queueing behind each other's backlog. A stream's FIN waits for
  its data.
- Data frames are sized to take about 5 ms to deliver at the measured rate: 4 KB up to
  `MaxMessageSize`, 16 KB until it's measured.
- While data flows, the session pings every 20 ms, with IDs that have the top bit set,
  to watch for a queue building in the connection beneath it. When the queueing delay
  goes over 5 ms, it paces data to 80% of the delivery rate measured from window
  updates. Pacing eases off when the queue drains.
