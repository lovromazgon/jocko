package mock

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Client for testing
type Client struct {
	msgCount int
	msgs     [][]byte
}

// NewClient is a client that fetches given number of msgs
func NewClient(msgCount int) *Client {
	return &Client{
		msgCount: msgCount,
	}
}

func (p *Client) Messages() [][]byte {
	return p.msgs
}

func (p *Client) Fetch(fetchRequest *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
	if len(p.msgs) >= p.msgCount {
		return &kmsg.FetchResponse{}, nil
	}
	msgs := [][]byte{
		[]byte("msg " + strconv.Itoa(len(p.msgs))),
	}
	response := &kmsg.FetchResponse{
		Topics: []kmsg.FetchResponseTopic{{
			Topic: fetchRequest.Topics[0].Topic,
			Partitions: []kmsg.FetchResponseTopicPartition{{
				RecordBatches: msgs[0],
			}},
		}},
	}
	p.msgs = append(p.msgs, msgs...)
	return response, nil
}

func (p *Client) CreateTopics(createRequest *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error) {
	return nil, nil
}

func (p *Client) LeaderAndISR(request *kmsg.LeaderAndISRRequest) (*kmsg.LeaderAndISRResponse, error) {
	return nil, nil
}
