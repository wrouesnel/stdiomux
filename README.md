# stdiomux

Many streams over one byte stream, such as an ssh session's stdin and stdout.

## Packages

- The root package, `stdiomux`, runs a yamux session over a pair of pipes, such as an ssh
  session's stdin and stdout (`NewConn`, `Client`, `Server`), and carries typed frames on
  its streams (`FrameWriter`, `FrameReader`): a type byte, a big-endian uint32 length and a
  payload. A stream usually starts with a frame saying what it's for. `Pipe` joins two
  connections, half-closing each as the other finishes.
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
- Data frames are sized to take about 5 ms to deliver at the measured rate: from 4 KB up
  to `MaxMessageSize`, and 16 KB until the rate has been measured.
- While data flows, the session pings about every 20 ms, with IDs that have the top bit
  set. A reply shows how long the ping queued in the connection beneath, and that
  everything written before it has been delivered, which gives the delivery rate. When
  the queueing delay goes over 10 ms, data is paced to 90% of the delivery rate until
  the queue drains, then at the delivery rate, rising 5% a ping while no queue builds.

On a 10 Mbit link, with another stream downloading, a stream's round trip takes 11-22 ms
against 160-210 ms with stock yamux. Downloads lose 1-5% at 10 Mbit and about 6% at
100 Mbit with 25 ms of delay each way.
