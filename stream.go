package stream

import (
	"bufio"
	"github.com/diggs/glog"
	"github.com/diggs/go-backoff"
	"net/http"
	"time"
)

/**
TODO:
Fix up close
Args for headers and SSL
Tests
**/

const (
	STREAM_INACTIVITY_TIMEOUT_SECONDS int = 90
)

type httpStream struct {
	Url                 string
	Data                chan []byte
	Exit                chan bool
	tcpBackoff          *backoff.Backoff
	httpBackoff         *backoff.Backoff
	httpThrottleBackoff *backoff.Backoff
}

func (s *httpStream) Connect() error {
	go s.enterReadStreamLoop()
	return nil
}

func (s *httpStream) Close() {
	s.Exit <- true
}

func (s *httpStream) resetBackoffs() {
	s.tcpBackoff.Reset()
	s.httpBackoff.Reset()
	s.httpThrottleBackoff.Reset()
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

func (s *httpStream) enterReadStreamLoop() {

	glog.Debug("Entering read stream loop...")
	for {
		select {
		case <-s.Exit:
			glog.Debug("Exit signalled; leaving read stream loop.")
			return
		default:
			resp, err := s.connect()
			// TODO Differentiate between transient tcp/ip errors and fatal errors (such as malformed url etc.)
			if err != nil {
				glog.Debugf("Encountered error establishing connection: %v", err)
				glog.Debugf("Backing off %d milliseconds", s.tcpBackoff.GetBackoffDuration()/time.Millisecond)
				s.tcpBackoff.Backoff()
				continue
			}

			switch resp.StatusCode {
			case 200, 304:
				glog.Debug("Connection established...")
				s.resetBackoffs()
				s.enterReadLineLoop(resp)
			case 420:
				glog.Debug("Encountered 420 backoff code")
				glog.Debugf("Backing off %d minute(s)", s.httpThrottleBackoff.GetBackoffDuration()/time.Minute)
				s.httpThrottleBackoff.Backoff()
			default:
				// TODO: Fatal errors... 401 etc.
				glog.Debugf("Encountered %v status code", resp.StatusCode)
				glog.Debugf("Backing off %d second(s)", s.httpBackoff.GetBackoffDuration()/time.Second)
				s.httpBackoff.Backoff()
			}
			resp.Body.Close()
		}
	}
}

func (s *httpStream) enterReadLineLoop(resp *http.Response) {

	glog.Debug("Entering read line loop...")

	scanner := bufio.NewScanner(resp.Body)
	for {
		lineCh, errCh := s.readLine(resp, scanner)
		select {
		case data := <-lineCh:
			glog.Debugf("Read line from stream: %d bytes.", len(data))
			if len(data) > 0 { // drop empty heartbeat lines
				s.Data <- data
			}
		case <-s.Exit:
			glog.Debug("Exit signalled; leaving readLine loop.")
			return
		case err := <-errCh:
			glog.Debugf("Stream error; leaving readLine loop: %v", err)
			return
		case <-time.After(time.Duration(STREAM_INACTIVITY_TIMEOUT_SECONDS) * time.Second):
			glog.Debugf("Stream inactive for %d seconds; leaving readLine loop.", STREAM_INACTIVITY_TIMEOUT_SECONDS)
			return
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
	// Back off linearly, starting at 250ms, capping at 16 seconds
	s.tcpBackoff = backoff.NewLinear(250*time.Millisecond, 16*time.Second)
	// Back off exponentially, starting at 5 seconds, capping at 320 seconds
	s.httpBackoff = backoff.NewExponential(5*time.Second, 320*time.Second)
	// Back off exponentially, starting at 1 minute, with no cap
	s.httpThrottleBackoff = backoff.NewExponential(time.Minute, 0)
	if autoConnect {
		s.Connect()
	}
	return &s
}
