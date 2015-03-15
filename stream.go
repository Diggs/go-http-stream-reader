package stream

import (
	"bufio"
	"github.com/diggs/glog"
	"math"
	"net/http"
	"time"
)

/**
TODO:
Fix up close
Code clean up
Args for headers and SSL
Tests
**/

const (
	STREAM_INACTIVITY_TIMEOUT_SECONDS int = 90
)

type httpStream struct {
	Url               string
	Data              chan []byte
	Exit              chan bool
	tcpErrCount       int
	httpErrCount      int
	httpThrottleCount int
}

func (s *httpStream) Connect() error {
	go s.enterStreamLoop()
	return nil
}

func (s *httpStream) Close() {
	s.Exit <- true
}

func (s *httpStream) connect() (*http.Response, error) {

	glog.Debugf("Establishing connection to %s...", s.Url)

	client := &http.Client{}

	req, err := http.NewRequest("GET", s.Url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *httpStream) enterStreamLoop() {

	glog.Debug("Entering stream loop...")
	for {
		select {
		case <-s.Exit:
			glog.Debug("Exit signalled; leaving stream loop.")
			return
		default:
			resp, err := s.connect()
			// TODO Differentiate between transient tcp/ip errors and fatal errors (such as malformed url etc.)
			if err != nil {
				glog.Debugf("Encountered error establishing connection: %v", err)
				s.tcpErrCount++
				// Backoff linearly, starting at 250ms, capping at 16 seconds
				backoff := s.tcpErrCount * 250
				if backoff > 16000 {
					backoff = 16000
				}
				glog.Debugf("Backing off %d milliseconds", backoff)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				continue
			}

			switch resp.StatusCode {
			case 200, 304:
				// Successful connection, start reading lines...
				glog.Debug("Connection established...")
				s.tcpErrCount = 0
				s.httpErrCount = 0
				s.httpThrottleCount = 0
				s.enterReadLineLoop(resp)
			case 420:
				// Back off exponentially, starting at 1 minute, with no cap
				glog.Debug("Encountered 420 backoff code")
				backoff := int(math.Pow(2, float64(s.httpThrottleCount)))
				glog.Debugf("Backing off %d minute(s)", backoff)
				s.httpThrottleCount++
				time.Sleep(time.Duration(backoff) * time.Minute)
			default:
				// TODO: Fatal errors... 401 etc.
				// Back off exponentially, starting at 5 seconds, capping at 320 seconds
				glog.Debugf("Encountered %d status code", resp.StatusCode)
				backoff := int(math.Pow(2, float64(s.httpErrCount)) * 5)
				if backoff > 320 {
					backoff = 320
				}
				s.httpErrCount++
				glog.Debugf("Backing off %d second(s)", backoff)
				time.Sleep(time.Duration(backoff) * time.Second)
			}
			resp.Body.Close()
		} 
	}
}

func (s *httpStream) enterReadLineLoop(resp *http.Response) {

	glog.Debug("Entering read line loop...")

	scanner := bufio.NewScanner(resp.Body)
	for {
		select {
		case <-s.Exit:
			glog.Debug("Exit signalled; leaving readLine loop.")
			return
		default:
			lineCh, errCh := s.readLine(resp, scanner)
			select {
			case err := <-errCh:
				glog.Debugf("Stream error; leaving readLine loop: %v", err)
				return
			case data := <-lineCh:
				glog.Debugf("Read line from stream: %d bytes.", len(data))
				if len(data) > 0 { // drop empty heartbeat lines
					s.Data <- data
				}
			case <-time.After(time.Duration(STREAM_INACTIVITY_TIMEOUT_SECONDS) * time.Second):
				glog.Debugf("Stream inactive for %d seconds; leaving readLine loop.", STREAM_INACTIVITY_TIMEOUT_SECONDS)
				return
			}
		}
	}
}

func (s *httpStream) readLine(resp *http.Response, scanner *bufio.Scanner) (chan []byte, chan error) {
	glog.Debug("Scanning for line...")
	lineCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		if ok := scanner.Scan(); !ok {
			errCh <- scanner.Err()
			return
		}
		lineCh <- scanner.Bytes()[:]
	}()
	return lineCh, errCh
}

func NewStream(url string, autoConnect bool) *httpStream {
	s := httpStream{}
	s.Url = url
	s.Data = make(chan []byte)
	s.Exit = make(chan bool)
	if autoConnect {
		s.Connect()
	}
	return &s
}
