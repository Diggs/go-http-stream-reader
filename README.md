## What is it?

go-http-stream-reader enables Go applications to consume long running HTTP request streams such as the [Twitter Streaming API](https://dev.twitter.com/streaming/overview). It automatically reconnects in case of errors (e.g. temporary network interruption, stalled or closed connection etc.) and does so in a scalable manner that respects the back-off rules of the remote host.

Currently the back-off rules are modelled exactly on [Twitters Rules](https://dev.twitter.com/streaming/overview/connecting) but could be made configurable if needed (feel free to open an issue or send a PR).

Streams are read using the [bufio Scanner](http://golang.org/pkg/bufio/#Scanner), and by default expects newline (```\n```) delimited data. Different delimiting routines could easily be implemented, again feel free to open an issue or send a PR.

## Usage

Open a stream by using the ```NewStream``` function, passing in the remote URL to connect to. Then read line data from the stream using the ```Data``` channel:

```go
s := stream.NewStream("https://userstream.twitter.com/1.1/user.json", true)
for {
  select {
  case line := <-s.Data:
    glog.Infof("Line: %s", string(line))
  }
}

```

To close the stream use ```stream.Close()```, if you need your line reading routine to exit cleanly when the stream is closed, either check the status of the ```Data``` channel:

```go
case line, open := <-s.Data:
  if !open {
    glog.Info("Stream closed.")
    return
  }
  glog.Infof("Line: %s", string(line))
}
```

or select the ```Exit``` channel in addition to the ```Data``` channel:

```go
for {
  select {
  case line := <-s.Data:
    glog.Infof("Line: %s", string(line))
  case <-s.Exit
    glog.Info("Stream closed.")
    return
  }
}
```