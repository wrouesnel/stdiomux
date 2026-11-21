# stdiomux

Many streams over one byte stream, such as an ssh session's stdin and stdout.

## Packages

- `yamux/`: a fork of [libp2p/go-yamux](https://github.com/libp2p/go-yamux) v5.1.0. It
  stays wire-compatible with yamux. Its changes are for interactive streams that share a
  connection with bulk transfers: a keystroke shouldn't wait behind megabytes of queued
  download.

## Licence

`yamux/` keeps upstream's licence, the Mozilla Public License 2.0 (`yamux/LICENSE`), with
parts under the BSD licence in `yamux/LICENSE-BSD`. Everything else is under the MIT
licence in `LICENSE`.


## Changes from upstream in yamux

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
