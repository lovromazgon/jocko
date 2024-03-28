package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

type Response struct {
	Size          int32
	CorrelationID int32
	Body          kmsg.Response
}

func (r Response) Encode(pe PacketEncoder) (err error) {
	pe.Push(&SizeField{})
	pe.PutInt32(r.CorrelationID)
	if err != nil {
		return err
	}
	err = pe.PutRawBytes(r.Body.AppendTo(nil))
	if err != nil {
		return err
	}
	pe.Pop()
	return nil
}

func (r Response) Decode(pd PacketDecoder, version int16) (err error) {
	r.Size, err = pd.Int32()
	if err != nil {
		return err
	}
	if r.CorrelationID, err = pd.Int32(); err != nil {
		return err
	}
	if r.Body != nil {
		r.Body.SetVersion(version)
		return r.Body.ReadFrom(pd.Raw())
	}
	return nil
}
