package jocko

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	serverVerboseLogs bool
)

func init() {
	spew.Config.Indent = ""

	e := os.Getenv("JOCKODEBUG")
	if strings.Contains(e, "server=1") {
		serverVerboseLogs = true
	}
}

// Broker is the interface that wraps the Broker's methods.
type Handler interface {
	Run(context.Context, <-chan *Context, chan<- *Context)
	Leave() error
	Shutdown() error
}

// Server is used to handle the TCP connections, decode requests,
// defer to the broker, and encode the responses.
type Server struct {
	config       *config.Config
	protocolLn   *net.TCPListener
	handler      Handler
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	metrics      *Metrics
	requestCh    chan *Context
	responseCh   chan *Context
	tracer       trace.Tracer
}

func NewServer(config *config.Config, handler Handler, metrics *Metrics) *Server {
	s := &Server{
		config:     config,
		handler:    handler,
		metrics:    metrics,
		shutdownCh: make(chan struct{}),
		requestCh:  make(chan *Context, 1024),
		responseCh: make(chan *Context, 1024),
		tracer: otel.Tracer("server", trace.WithInstrumentationAttributes(
			attribute.Int("node_id", int(config.ID)),
			attribute.String("addr", config.Addr),
		)),
	}
	return s
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	if s.protocolLn, err = net.ListenTCP("tcp", protocolAddr); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			default:
				conn, err := s.protocolLn.Accept()
				if err != nil {
					log.Error.Printf("server/%d: listener accept error: %s", s.config.ID, err)
					continue
				}

				go s.handleRequests(conn)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-s.shutdownCh:
				break
			case respCtx := <-s.responseCh:
				trace.SpanFromContext(respCtx).End()
				if err := s.handleResponse(respCtx); err != nil {
					log.Error.Printf("server/%d: handle response error: %s", s.config.ID, err)
				}
			}
		}
	}()

	log.Debug.Printf("server/%d: run handler", s.config.ID)
	go s.handler.Run(ctx, s.requestCh, s.responseCh)

	return nil
}

func (s *Server) Leave() error {
	return s.handler.Leave()
}

// Shutdown closes the service.
func (s *Server) Shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	close(s.shutdownCh)

	if err := s.handler.Shutdown(); err != nil {
		return err
	}
	if err := s.protocolLn.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleRequests(conn net.Conn) {
	defer conn.Close()

	for {
		ctx := context.Background()
		ctx, span := s.tracer.Start(ctx, "request")

		data, err := s.readRequestData(ctx, conn)
		if err != nil {
			if err != io.EOF {
				log.Error.Printf("failed to read request data: %s", err)
			}
			span.RecordError(err)
			span.End()
			conn.Write([]byte(err.Error()))
			break
		}

		span.SetAttributes(attribute.Int("size", len(data)))

		header, n, err := s.decodeRequestHeader(ctx, data)
		if err != nil {
			log.Error.Printf("failed to decode request header: %s", err)
			span.RecordError(err)
			span.End()
			conn.Write([]byte(err.Error()))
			break
		}

		span.SetAttributes(
			attribute.Int("api_key", int(header.APIKey)),
			attribute.Int("correlation_id", int(header.CorrelationID)),
			attribute.String("client_id", header.ClientID),
		)

		req, err := s.decodeRequest(ctx, data[n:], header)
		if err != nil {
			log.Error.Printf("failed to decode request: %s", err)
			span.RecordError(err)
			span.End()
			conn.Write([]byte(err.Error()))
			break
		}

		ctx, _ = s.tracer.Start(ctx, "server: queue request")

		reqCtx := &Context{
			parent: ctx,
			header: header,
			req:    req,
			conn:   conn,
		}

		log.Debug.Printf("server/%d: handle request: %s", s.config.ID, reqCtx)

		s.requestCh <- reqCtx
	}
}

func (s *Server) readRequestData(ctx context.Context, conn io.Reader) ([]byte, error) {
	p := make([]byte, 4)
	_, err := io.ReadFull(conn, p)
	if err != nil {
		if err != io.EOF {
			err = fmt.Errorf("conn read error: %w", err)
		}
		return nil, err
	}

	size := protocol.Encoding.Uint32(p)
	if size == 0 {
		return nil, errors.New("request size is 0")
	}

	b := make([]byte, size+4) // +4 since we're going to copy the size into b
	copy(b, p)

	if _, err = io.ReadFull(conn, b[4:]); err != nil {
		return nil, fmt.Errorf("conn read error: %w", err)
	}

	return b, nil
}

func (s *Server) decodeRequestHeader(ctx context.Context, data []byte) (_ *protocol.RequestHeader, n int, err error) {
	ctx, span := s.tracer.Start(ctx, "server: decode request header")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	d := protocol.NewDecoder(data)
	header := new(protocol.RequestHeader)
	err = header.Decode(d)
	if err != nil {
		return nil, 0, err
	}

	return header, d.Offset(), nil
}

func (s *Server) decodeRequest(ctx context.Context, data []byte, header *protocol.RequestHeader) (req kmsg.Request, err error) {
	ctx, span := s.tracer.Start(ctx, "server: decode request")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	switch kmsg.Key(header.APIKey) {
	case kmsg.Produce:
		req = &kmsg.ProduceRequest{}
	case kmsg.Fetch:
		req = &kmsg.FetchRequest{}
	case kmsg.ListOffsets:
		req = &kmsg.ListOffsetsRequest{}
	case kmsg.Metadata:
		req = &kmsg.MetadataRequest{}
	case kmsg.LeaderAndISR:
		req = &kmsg.LeaderAndISRRequest{}
	case kmsg.StopReplica:
		req = &kmsg.StopReplicaRequest{}
	case kmsg.UpdateMetadata:
		req = &kmsg.UpdateMetadataRequest{}
	case kmsg.ControlledShutdown:
		req = &kmsg.ControlledShutdownRequest{}
	case kmsg.OffsetCommit:
		req = &kmsg.OffsetCommitRequest{}
	case kmsg.OffsetFetch:
		req = &kmsg.OffsetFetchRequest{}
	case kmsg.FindCoordinator:
		req = &kmsg.FindCoordinatorRequest{}
	case kmsg.JoinGroup:
		req = &kmsg.JoinGroupRequest{}
	case kmsg.Heartbeat:
		req = &kmsg.HeartbeatRequest{}
	case kmsg.LeaveGroup:
		req = &kmsg.LeaveGroupRequest{}
	case kmsg.SyncGroup:
		req = &kmsg.SyncGroupRequest{}
	case kmsg.DescribeGroups:
		req = &kmsg.DescribeGroupsRequest{}
	case kmsg.ListGroups:
		req = &kmsg.ListGroupsRequest{}
	case kmsg.SASLHandshake:
		req = &kmsg.SASLHandshakeRequest{}
	case kmsg.ApiVersions:
		req = &kmsg.ApiVersionsRequest{}
	case kmsg.CreateTopics:
		req = &kmsg.CreateTopicsRequest{}
	case kmsg.DeleteTopics:
		req = &kmsg.DeleteTopicsRequest{}
	default:
		return nil, fmt.Errorf("unknown API key: %d", header.APIKey)
	}

	req.SetVersion(header.APIVersion)
	err = req.ReadFrom(data)
	if err != nil {
		return nil, err
	}

	return req, err
}

func (s *Server) handleResponse(respCtx *Context) error {
	log.Debug.Printf("server/%d: handle response: %s", s.config.ID, respCtx)

	span := trace.SpanFromContext(respCtx)
	defer span.End()
	_, responseSpan := s.tracer.Start(respCtx, "server: handle response")
	defer responseSpan.End()

	b, err := protocol.Encode(respCtx.res)
	if err != nil {
		return err
	}
	_, err = respCtx.conn.Write(b)
	return err
}

// Addr returns the address on which the Server is listening
func (s *Server) Addr() net.Addr {
	return s.protocolLn.Addr()
}

func (s *Server) ID() int32 {
	return s.config.ID
}
