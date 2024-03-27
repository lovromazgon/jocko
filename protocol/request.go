package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

type Body interface {
	Encoder
	Key() int16
	Version() int16
}

type Request struct {
	CorrelationID int32
	ClientID      string
	Body          kmsg.Request
}

func (r *Request) Encode(pe PacketEncoder) (err error) {
	pe.Push(&SizeField{})
	pe.PutInt16(r.Body.Key())
	pe.PutInt16(r.Body.GetVersion())
	pe.PutInt32(r.CorrelationID)
	if err = pe.PutString(r.ClientID); err != nil {
		return err
	}
	if err = pe.PutRawBytes(r.Body.AppendTo(nil)); err != nil {
		return err
	}
	pe.Pop()
	return nil
}
