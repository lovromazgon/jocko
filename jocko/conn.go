package jocko

import (
	"bufio"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/travisjeffery/jocko/protocol"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Conn implemenets net.Conn for connections to Jocko brokers. It's used as an internal client for replication fetches and leader and ISR requests.
type Conn struct {
	conn          net.Conn
	rlock         sync.Mutex
	rbuf          bufio.Reader
	rdeadline     connDeadline
	wlock         sync.Mutex
	wbuf          bufio.Writer
	wdeadline     connDeadline
	clientID      string
	correlationID int32
}

// NewConn creates a new *Conn.
func NewConn(conn net.Conn, clientID string) (*Conn, error) {
	return &Conn{
		conn:     conn,
		clientID: clientID,
		rbuf:     *bufio.NewReader(conn),
		wbuf:     *bufio.NewWriter(conn),
	}, nil
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// SetDeadline sets the read and write deadlines associated
// with the connection. It is equivalent to calling both
// SetReadDeadline and SetWriteDeadline. See net.Conn SetDeadline.
func (c *Conn) SetDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	c.wdeadline.setDeadline(t)
	return nil
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wdeadline.setDeadline(t)
	return nil
}

// Read implements the Conn Read method. Don't use it.
func (c *Conn) Read(b []byte) (int, error) {
	return 0, nil
}

// Write implements the Conn Write method. Don't use it.
func (c *Conn) Write(b []byte) (int, error) {
	return 0, nil
}

// Close closes the connection.
func (c *Conn) Close() error { return c.conn.Close() }

// LeaderAndISR sends a leader and ISR request and returns the response.
func (c *Conn) LeaderAndISR(req *kmsg.LeaderAndISRRequest) (*kmsg.LeaderAndISRResponse, error) {
	var resp kmsg.LeaderAndISRResponse
	err := c.writeOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// CreateTopics sends a create topics request and returns the response.
func (c *Conn) CreateTopics(req *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error) {
	var resp kmsg.CreateTopicsResponse
	err := c.writeOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Produce sends a produce request and returns the response.
func (c *Conn) Produce(req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
	var resp kmsg.ProduceResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Offsets sends an offsets request and returns the response.
func (c *Conn) Offsets(req *kmsg.ListOffsetsRequest) (*kmsg.ListOffsetsResponse, error) {
	var resp kmsg.ListOffsetsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// StopReplica sends a stop replica request and returns the response.
func (c *Conn) StopReplica(req *kmsg.StopReplicaRequest) (*kmsg.StopReplicaResponse, error) {
	var resp kmsg.StopReplicaResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateMetadata sends an update metadata request and returns the response.
func (c *Conn) UpdateMetadata(req *kmsg.UpdateMetadataRequest) (*kmsg.UpdateMetadataResponse, error) {
	var resp kmsg.UpdateMetadataResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ControlledShutdown sends a controlled shutdown request and returns the response.
func (c *Conn) ControlledShutdown(req *kmsg.ControlledShutdownRequest) (*kmsg.ControlledShutdownResponse, error) {
	var resp kmsg.ControlledShutdownResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// OffsetCommit sends an offset commit and returns the response.
func (c *Conn) OffsetCommit(req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error) {
	var resp kmsg.OffsetCommitResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// SaslHandshake sends a sasl handshake request and returns the response.
func (c *Conn) SaslHandshake(req *kmsg.SASLHandshakeRequest) (*kmsg.SASLHandshakeResponse, error) {
	var resp kmsg.SASLHandshakeResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// OffsetFetch sends an offset fetch and returns the response.
func (c *Conn) OffsetFetch(req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error) {
	var resp kmsg.OffsetFetchResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// FindCoordinator sends a find coordinator request and returns the response.
func (c *Conn) FindCoordinator(req *kmsg.FindCoordinatorRequest) (*kmsg.FindCoordinatorResponse, error) {
	var resp kmsg.FindCoordinatorResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Heartbeat sends a heartbeat request and returns the response.
func (c *Conn) Heartbeat(req *kmsg.HeartbeatRequest) (*kmsg.HeartbeatResponse, error) {
	var resp kmsg.HeartbeatResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// LeaveGroup sends a leave group request and returns the response.
func (c *Conn) LeaveGroup(req *kmsg.LeaveGroupRequest) (*kmsg.LeaveGroupResponse, error) {
	var resp kmsg.LeaveGroupResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// SyncGroup sends a sync group request and returns the response.
func (c *Conn) SyncGroup(req *kmsg.SyncGroupRequest) (*kmsg.SyncGroupResponse, error) {
	var resp kmsg.SyncGroupResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DescribeGroups sends a describe groups request and returns the response.
func (c *Conn) DescribeGroups(req *kmsg.DescribeGroupsRequest) (*kmsg.DescribeGroupsResponse, error) {
	var resp kmsg.DescribeGroupsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListGroups sends a list groups request and returns the response.
func (c *Conn) ListGroups(req *kmsg.ListGroupsRequest) (*kmsg.ListGroupsResponse, error) {
	var resp kmsg.ListGroupsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// APIVersions sends an api version request and returns the response.
func (c *Conn) APIVersions(req *kmsg.ApiVersionsRequest) (*kmsg.ApiVersionsResponse, error) {
	var resp kmsg.ApiVersionsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteTopics sends a delete topic request and returns the response.
func (c *Conn) DeleteTopics(req *kmsg.DeleteTopicsRequest) (*kmsg.DeleteTopicsResponse, error) {
	var resp kmsg.DeleteTopicsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// JoinGroup sends a join group request and returns the response.
func (c *Conn) JoinGroup(req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, error) {
	var resp kmsg.JoinGroupResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Metadata sends a metadata request and returns the response.
func (c *Conn) Metadata(req *kmsg.MetadataRequest) (*kmsg.MetadataResponse, error) {
	var resp kmsg.MetadataResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Fetch sends a fetch request and returns the response.
func (c *Conn) Fetch(req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
	var resp kmsg.FetchResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// AlterConfigs sends an alter configs request and returns the response.
func (c *Conn) AlterConfigs(req *kmsg.AlterConfigsRequest) (*kmsg.AlterConfigsResponse, error) {
	var resp kmsg.AlterConfigsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// DescribeConfigs sends an describe configs request and returns the response.
func (c *Conn) DescribeConfigs(req *kmsg.DescribeConfigsRequest) (*kmsg.DescribeConfigsResponse, error) {
	var resp kmsg.DescribeConfigsResponse
	err := c.readOperation(func(deadline time.Time, id int32) error {
		return c.writeRequest(req)
	}, func(deadline time.Time, size int) error {
		return c.readResponse(&resp, size, req.GetVersion())
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Conn) readResponse(resp kmsg.Response, size int, version int16) error {
	b, err := c.rbuf.Peek(size)
	if err != nil {
		return err
	}

	resp.SetVersion(version)
	err = resp.ReadFrom(b)

	c.rbuf.Discard(size)
	return err
}

func (c *Conn) writeRequest(body kmsg.Request) error {
	req := &protocol.Request{
		CorrelationID: c.correlationID,
		ClientID:      c.clientID,
		Body:          body,
	}
	b, err := protocol.Encode(req)
	if err != nil {
		return err
	}
	_, err = c.wbuf.Write(b)
	if err != nil {
		return err
	}
	return c.wbuf.Flush()
}

type wop func(deadline time.Time, id int32) error
type rop func(deadline time.Time, size int) error

func (c *Conn) readOperation(write wop, read rop) error {
	return c.do(&c.rdeadline, write, read)
}

func (c *Conn) writeOperation(write wop, read rop) error {
	return c.do(&c.wdeadline, write, read)
}

func (c *Conn) do(d *connDeadline, write wop, read rop) error {
	id, err := c.doRequest(d, write)
	if err != nil {
		return err
	}
	deadline, size, lock, err := c.waitResponse(d, id)
	if err != nil {
		return err
	}

	if err = read(deadline, size); err != nil {
		switch err.(type) {
		case *kerr.Error:
		default:
			c.conn.Close()
		}
	}

	d.unsetConnReadDeadline()
	lock.Unlock()
	return err
}

func (c *Conn) doRequest(d *connDeadline, write wop) (int32, error) {
	c.wlock.Lock()
	c.correlationID++
	id := c.correlationID
	err := write(d.setConnWriteDeadline(c.conn), id)
	d.unsetConnWriteDeadline()
	if err != nil {
		c.conn.Close()
	}
	c.wlock.Unlock()
	return c.correlationID, nil
}

func (c *Conn) waitResponse(d *connDeadline, id int32) (deadline time.Time, size int, lock *sync.Mutex, err error) {
	for {
		var rsz int32
		var rid int32

		c.rlock.Lock()
		deadline = d.setConnReadDeadline(c.conn)

		if rsz, rid, err = c.peekResponseSizeAndID(); err != nil {
			d.unsetConnReadDeadline()
			c.conn.Close()
			c.rlock.Unlock()
			return
		}

		if id == rid {
			c.skipResponseSizeAndID()
			size, lock = int(rsz-4), &c.rlock
			return
		}

		c.rlock.Unlock()
		runtime.Gosched()
	}
}

func (c *Conn) readDeadline() time.Time {
	return c.rdeadline.deadline()
}

func (c *Conn) writeDeadline() time.Time {
	return c.wdeadline.deadline()
}

func (c *Conn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.rbuf.Peek(8)
	if err != nil {
		return 0, 0, nil
	}
	size, id := protocol.MakeInt32(b[:4]), protocol.MakeInt32(b[4:])
	return size, id, nil
}

func (c *Conn) skipResponseSizeAndID() {
	c.rbuf.Discard(8)
}

type connDeadline struct {
	mutex sync.Mutex
	value time.Time
	rconn net.Conn
	wconn net.Conn
}

func (d *connDeadline) deadline() time.Time {
	d.mutex.Lock()
	t := d.value
	d.mutex.Unlock()
	return t
}

func (d *connDeadline) setDeadline(t time.Time) {
	d.mutex.Lock()
	d.value = t

	if d.rconn != nil {
		d.rconn.SetReadDeadline(t)
	}

	if d.wconn != nil {
		d.wconn.SetWriteDeadline(t)
	}

	d.mutex.Unlock()
}

func (d *connDeadline) setConnReadDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.rconn = conn
	d.rconn.SetReadDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) setConnWriteDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.wconn = conn
	d.wconn.SetWriteDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) unsetConnReadDeadline() {
	d.mutex.Lock()
	d.rconn = nil
	d.mutex.Unlock()
}

func (d *connDeadline) unsetConnWriteDeadline() {
	d.mutex.Lock()
	d.wconn = nil
	d.mutex.Unlock()
}
