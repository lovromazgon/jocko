package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

var APIVersions = []kmsg.ApiVersionsResponseApiKey{
	{ApiKey: kmsg.Produce.Int16(), MinVersion: 0, MaxVersion: 5},
	{ApiKey: kmsg.Fetch.Int16(), MinVersion: 0, MaxVersion: 3},
	{ApiKey: kmsg.ListOffsets.Int16(), MinVersion: 0, MaxVersion: 2},
	{ApiKey: kmsg.Metadata.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.LeaderAndISR.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.StopReplica.Int16(), MinVersion: 0, MaxVersion: 0},
	{ApiKey: kmsg.FindCoordinator.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.JoinGroup.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.Heartbeat.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.LeaveGroup.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.SyncGroup.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.DescribeGroups.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.ListGroups.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.ApiVersions.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.CreateTopics.Int16(), MinVersion: 0, MaxVersion: 1},
	{ApiKey: kmsg.DeleteTopics.Int16(), MinVersion: 0, MaxVersion: 1},
}
