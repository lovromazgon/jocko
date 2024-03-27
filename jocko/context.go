package jocko

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/travisjeffery/jocko/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Context struct {
	mu     sync.Mutex
	conn   io.ReadWriter
	err    error
	header *protocol.RequestHeader
	parent context.Context
	req    kmsg.Request
	res    *protocol.Response
	vals   map[interface{}]interface{}
}

func (ctx *Context) Request() kmsg.Request {
	return ctx.req
}

func (ctx *Context) Response() *protocol.Response {
	return ctx.res
}

func (ctx *Context) Header() *protocol.RequestHeader {
	return ctx.header
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (ctx *Context) Done() <-chan struct{} {
	return nil
}

func (ctx *Context) Err() error {
	return ctx.err
}

func (ctx *Context) String() string {
	return fmt.Sprintf("ctx: %s", ctx.header)
}

func (ctx *Context) Value(key interface{}) interface{} {
	ctx.mu.Lock()
	if ctx.vals == nil {
		ctx.vals = make(map[interface{}]interface{})
	}
	val := ctx.vals[key]
	if val == nil {
		val = ctx.parent.Value(key)
	}
	ctx.mu.Unlock()
	return val
}
