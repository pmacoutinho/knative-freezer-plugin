package freezer

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentRequestsDuringRestoreAllWait verifies that when an application
// is checkpointed, every request that arrives during the restore window is held
// until the restored process is accepting connections, not just the request
// that triggers the restore. With the pre-fix behaviour (concurrent requests
// take the !frozen fast path and return immediately) this test fails, because
// requests are "forwarded" before the user port is up.
func TestConcurrentRequestsDuringRestoreAllWait(t *testing.T) {
	// Reserve a free port for the (initially down) user container, then release
	// it so nothing is listening until the simulated restore brings it up.
	uln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	userPort := portOf(uln.Addr())
	_ = uln.Close()

	var appReady int32 // 0 = user port down, 1 = accepting
	resumeReceived := make(chan struct{})
	var once sync.Once

	// Fake checkpoint/restore daemon: acknowledge the resume request and signal
	// that the (asynchronous) app bring-up may begin.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(resumeReceived) })
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port := hostPortFromURL(srv.URL)

	// Simulate the restored process coming up ~200ms after resume is issued.
	const restoreDelay = 200 * time.Millisecond
	go func() {
		<-resumeReceived
		time.Sleep(restoreDelay)
		ln, err := net.Listen("tcp", "127.0.0.1:"+userPort)
		if err != nil {
			return
		}
		atomic.StoreInt32(&appReady, 1)
		go func() {
			for {
				c, e := ln.Accept()
				if e != nil {
					return
				}
				c.Close()
			}
		}()
	}()

	p := &freezerPlug{
		hostIP:      host,
		freezerPort: port,
		userPort:    userPort,
		frozen:      true,
	}

	const n = 6
	var wg sync.WaitGroup
	var violations int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, "http://app/", nil)
			if _, err := p.ApproveRequest(req); err != nil {
				t.Errorf("ApproveRequest returned error: %v", err)
			}
			// Once ApproveRequest returns, the request is about to be forwarded.
			// If the user port is not yet accepting, this request would hit a
			// connection-refused: a deliverability gap.
			if atomic.LoadInt32(&appReady) == 0 {
				atomic.AddInt32(&violations, 1)
			}
		}()
		time.Sleep(15 * time.Millisecond) // stagger so several land mid-restore
	}
	wg.Wait()

	if violations > 0 {
		t.Fatalf("%d/%d requests were forwarded before the restored process was accepting connections", violations, n)
	}
}

func portOf(a net.Addr) string {
	_, port, _ := net.SplitHostPort(a.String())
	return port
}

func hostPortFromURL(u string) (string, string) {
	u = strings.TrimPrefix(u, "http://")
	host, port, _ := net.SplitHostPort(u)
	return host, port
}
