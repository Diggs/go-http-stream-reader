## What is it?

go-http-stream-reader enables Go applications to consume long running HTTP request streams such as the [Twitter Streaming API](https://dev.twitter.com/streaming/overview). It automatically reconnects in case of errors (e.g. temporary network interruption, stalled or closed connection etc.) and does so in a scalable manner that respects the back-off rules of the remote host.

Currently the back-off rules are modelled exactly on [Twitters Rules](https://dev.twitter.com/streaming/overview/connecting) but could be made configurable if needed (feel free to open an issue or send a PR).

Streams are read using the [bufio Scanner](http://golang.org/pkg/bufio/#Scanner), and by default expects newline (```\n```) delimited data. Different delimiting routines could easily be implemented, again feel free to open an issue or send a PR.

## Usage

See GoDocs here for full api documentation: [https://godoc.org/github.com/Diggs/go-http-stream-reader](https://godoc.org/github.com/Diggs/go-http-stream-reader)

Open a stream by using the ```NewStream``` function, passing in the remote URL to connect to. If needed, set the ```HttpClient```, ```HttpRequest``` or ```Headers``` properties of the HttpStream to modify the connection behaviour (for example using a custom http.Client to enable self-signed certs, or a custom http.Request to use a different method than the default GET). Finally call ```Connect()``` to begin reading data from the stream using the ```Data``` channel:

```go
s := stream.NewStream("https://userstream.twitter.com/1.1/user.json")
s.Headers = map[string]string{"Authorization":"foobar"}
s.Connect()
for {
  select {
  case data := <-s.Data:
    glog.Infof("Data: %s", string(data))
  case err := <-s.Error:
    glog.Infof("Error: %v", err)
  case <-s.Exit:
    glog.Info("Stream closed.")
    return
  }
}

```

To close the stream use ```stream.Close()```, if you need your line reading routine to exit cleanly when the stream is closed, either check the status of the ```Data``` channel or ensure you have a reader on the ```Exit``` channel like above.

The ```Error``` channel can be read to receive any errors (resumable OR fatal) that occur during the lifetime of the stream. If a fatal error does occur it will be written to the ```Error``` channel before the stream gracefully closes.