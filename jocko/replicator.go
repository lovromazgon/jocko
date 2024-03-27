package jocko

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/travisjeffery/jocko/log"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Client is used to request other brokers.
type client interface {
	Fetch(fetchRequest *kmsg.FetchRequest) (*kmsg.FetchResponse, error)
	CreateTopics(createRequest *kmsg.CreateTopicsRequest) (*kmsg.CreateTopicsResponse, error)
	LeaderAndISR(request *kmsg.LeaderAndISRRequest) (*kmsg.LeaderAndISRResponse, error)
	// others
}

// Replicator fetches from the partition's leader producing to itself the follower, thereby replicating the partition.
type Replicator struct {
	config              ReplicatorConfig
	replica             *Replica
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	leader              client
	backoff             *backoff.ExponentialBackOff
}

type ReplicatorConfig struct {
	MinBytes int32
	// todo: make this a time.Duration
	MaxWaitTime time.Duration
}

// NewReplicator returns a new replicator instance.
func NewReplicator(config ReplicatorConfig, replica *Replica, leader client) *Replicator {
	if config.MinBytes == 0 {
		config.MinBytes = 1
	}
	bo := backoff.NewExponentialBackOff()
	bo.Reset()
	r := &Replicator{
		config:  config,
		replica: replica,
		leader:  leader,
		done:    make(chan struct{}, 2),
		msgs:    make(chan []byte, 2),
		backoff: bo,
	}
	return r
}

// Replicate start fetching messages from the leader and appending them to the local commit log.
func (r *Replicator) Replicate() {
	go r.fetchMessages()
	go r.appendMessages()
}

func (r *Replicator) fetchMessages() {
	var fetchRequest *kmsg.FetchRequest
	var fetchResponse *kmsg.FetchResponse
	var err error
	for {
		select {
		case <-r.done:
			return
		default:
			fetchRequest = &kmsg.FetchRequest{
				ReplicaID:     r.replica.BrokerID,
				MaxWaitMillis: int32(r.config.MaxWaitTime.Milliseconds()),
				MinBytes:      r.config.MinBytes,
				Topics: []kmsg.FetchRequestTopic{{
					Topic: r.replica.Partition.Topic,
					Partitions: []kmsg.FetchRequestTopicPartition{{
						Partition:   r.replica.Partition.ID,
						FetchOffset: r.offset,
					}},
				}},
			}
			fetchResponse, err = r.leader.Fetch(fetchRequest)
			// TODO: probably shouldn't panic. just let this replica fall out of ISR.
			if err != nil {
				log.Error.Printf("replicator: fetch messages error: %s", err)
				goto BACKOFF
			}
			for _, resp := range fetchResponse.Topics {
				for _, p := range resp.Partitions {
					if p.ErrorCode != 0 {
						log.Error.Printf("replicator: partition response error: %d", p.ErrorCode)
						goto BACKOFF
					}
					if p.RecordBatches == nil {
						goto BACKOFF
					}
					offset := int64(protocol.Encoding.Uint64(p.RecordBatches[:8]))
					if offset > r.offset {
						r.msgs <- p.RecordBatches
						r.highwaterMarkOffset = p.HighWatermark
						r.offset = offset
					}
				}
			}

			r.backoff.Reset()
			continue

		BACKOFF:
			time.Sleep(r.backoff.NextBackOff())
		}
	}
}

func (r *Replicator) appendMessages() {
	for {
		select {
		case <-r.done:
			return
		case msg := <-r.msgs:
			_, err := r.replica.Log.Append(msg)
			if err != nil {
				panic(err)
			}
		}
	}
}

// Close the replicator object when we are no longer following
func (r *Replicator) Close() error {
	close(r.done)
	return nil
}
