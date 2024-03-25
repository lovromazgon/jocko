package jocko

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/travisjeffery/jocko/jocko/config"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
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

	p := make([]byte, 4)
	for {
		ctx := context.Background()

		_, err := io.ReadFull(conn, p)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error.Printf("conn read error: %s", err)
			break
		}

		ctx, span := s.tracer.Start(ctx, "request")
		ctx, decodeSpan := s.tracer.Start(ctx, "server: decode request")

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break // TODO: should this even happen?
		}

		b := make([]byte, size+4) // +4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			// TODO: handle request
			span.RecordError(err, trace.WithAttributes(attribute.String("msg", "failed to read from connection")))
			span.End()
			panic(err)
		}

		d := protocol.NewDecoder(b)
		header := new(protocol.RequestHeader)
		if err := header.Decode(d); err != nil {
			// TODO: handle err
			span.RecordError(err, trace.WithAttributes(attribute.String("msg", "failed to decode header")))
			span.End()
			panic(err)
		}

		span.SetAttributes(
			attribute.Int("api_key", int(header.APIKey)),
			attribute.Int("correlation_id", int(header.CorrelationID)),
			attribute.String("client_id", header.ClientID),
			attribute.Int64("size", int64(size)),
		)

		var req protocol.VersionedDecoder

		switch header.APIKey {
		case protocol.ProduceKey:
			req = &protocol.ProduceRequest{}
		case protocol.FetchKey:
			req = &protocol.FetchRequest{}
		case protocol.OffsetsKey:
			req = &protocol.OffsetsRequest{}
		case protocol.MetadataKey:
			req = &protocol.MetadataRequest{}
		case protocol.LeaderAndISRKey:
			req = &protocol.LeaderAndISRRequest{}
		case protocol.StopReplicaKey:
			req = &protocol.StopReplicaRequest{}
		case protocol.UpdateMetadataKey:
			req = &protocol.UpdateMetadataRequest{}
		case protocol.ControlledShutdownKey:
			req = &protocol.ControlledShutdownRequest{}
		case protocol.OffsetCommitKey:
			req = &protocol.OffsetCommitRequest{}
		case protocol.OffsetFetchKey:
			req = &protocol.OffsetFetchRequest{}
		case protocol.FindCoordinatorKey:
			req = &protocol.FindCoordinatorRequest{}
		case protocol.JoinGroupKey:
			req = &protocol.JoinGroupRequest{}
		case protocol.HeartbeatKey:
			req = &protocol.HeartbeatRequest{}
		case protocol.LeaveGroupKey:
			req = &protocol.LeaveGroupRequest{}
		case protocol.SyncGroupKey:
			req = &protocol.SyncGroupRequest{}
		case protocol.DescribeGroupsKey:
			req = &protocol.DescribeGroupsRequest{}
		case protocol.ListGroupsKey:
			req = &protocol.ListGroupsRequest{}
		case protocol.SaslHandshakeKey:
			req = &protocol.SaslHandshakeRequest{}
		case protocol.APIVersionsKey:
			req = &protocol.APIVersionsRequest{}
		case protocol.CreateTopicsKey:
			req = &protocol.CreateTopicRequests{}
		case protocol.DeleteTopicsKey:
			req = &protocol.DeleteTopicsRequest{}
		}

		if err := req.Decode(d, header.APIVersion); err != nil {
			log.Error.Printf("server/%d: %s: decode request failed: %s", s.config.ID, header, err)
			span.RecordError(err, trace.WithAttributes(attribute.String("msg", "failed to decode request")))
			span.End()
			panic(err)
		}

		decodeSpan.End()

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

func (s *Server) handleResponse(respCtx *Context) error {
	log.Debug.Printf("server/%d: handle response: %s", s.config.ID, respCtx)

	span := trace.SpanFromContext(respCtx)
	defer span.End()
	_, responseSpan := s.tracer.Start(respCtx, "server: handle response")
	defer responseSpan.End()

	b, err := protocol.Encode(respCtx.res.(protocol.Encoder))
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
