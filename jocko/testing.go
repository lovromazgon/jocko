package jocko

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/hashicorp/raft"
	dynaport "github.com/travisjeffery/go-dynaport"
	"github.com/travisjeffery/jocko/jocko/config"
)

var (
	nodeNumber int32
)

type T interface {
	Name() string
	Fatalf(format string, args ...interface{})
	Log(args ...interface{})
	FailNow()
}

func NewTestServer(t T, cbBroker func(cfg *config.Config), cbServer func(cfg *config.Config)) (*Server, string) {
	ports := dynaport.Get(4)
	nodeID := atomic.AddInt32(&nodeNumber, 1)

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("jocko-test-server-%d", nodeID))
	if err != nil {
		panic(err)
	}

	config := config.DefaultConfig()
	config.ID = nodeID
	config.NodeName = fmt.Sprintf("%s-node-%d", t.Name(), nodeID)
	config.DataDir = tmpDir
	config.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	config.RaftAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[1])
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.BindPort = ports[2]
	config.LeaveDrainTime = 1 * time.Millisecond
	config.ReconcileInterval = 300 * time.Millisecond

	// Tighten the Serf timing
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfLANConfig.MemberlistConfig.RetransmitMult = 2
	config.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond

	// Tighten the Raft timing
	config.RaftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	config.RaftConfig.HeartbeatTimeout = 200 * time.Millisecond
	config.RaftConfig.ElectionTimeout = 200 * time.Millisecond

	if cbBroker != nil {
		cbBroker(config)
	}

	b, err := NewBroker(config)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	if cbServer != nil {
		cbServer(config)
	}

	return NewServer(config, b, nil), tmpDir
}

func TestJoin(t T, s1 *Server, other ...*Server) {
	addr := fmt.Sprintf("127.0.0.1:%d",
		s1.config.SerfLANConfig.MemberlistConfig.BindPort)
	for _, s2 := range other {
		if num, err := s2.handler.(*Broker).serf.Join([]string{addr}, true); err != nil {
			t.Fatalf("err: %v", err)
		} else if num != 1 {
			t.Fatalf("bad: %d", num)
		}
	}
}

// WaitForLeader waits for one of the servers to be leader, failing the test if no one is the leader. Returns the leader (if there is one) and non-leaders.
func WaitForLeader(t T, servers ...*Server) (*Server, []*Server) {
	tmp := struct {
		leader    *Server
		followers map[*Server]bool
	}{nil, make(map[*Server]bool)}

	Retry(t, func() error {
		for _, s := range servers {
			if raft.Leader == s.handler.(*Broker).raft.State() {
				tmp.leader = s
			} else {
				tmp.followers[s] = true
			}
		}
		if tmp.leader == nil {
			return errors.New("no leader")
		}
		return nil
	})

	followers := make([]*Server, 0, len(tmp.followers))
	for f := range tmp.followers {
		followers = append(followers, f)
	}
	return tmp.leader, followers
}

func Retry(t T, f func() error) {
	err := retry.Do(
		f,
		retry.Delay(25*time.Millisecond),
		retry.DelayType(retry.FixedDelay),
		retry.Attempts(50),
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}
