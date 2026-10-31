package toshitest

import (
	"context"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// defaultStartingDeadline is how late a missed instant may be published when
// SetServiceAlarm names no deadline.
const defaultStartingDeadline = 10 * time.Minute

// maxOccurrences is how many occurrences GetServiceAlarm returns.
const maxOccurrences = 20

// alarmKey names one alarm: its service and its name.
type alarmKey struct {
	service string
	name    string
}

// alarm is an alarm the fake holds, and the occurrences RunServiceAlarmNow
// made for it, newest first.
type alarm struct {
	alarm       *workspacev1.ServiceAlarm
	occurrences []*workspacev1.ServiceAlarmOccurrence
}

// setServiceAlarm creates the alarm at generation 1, or replaces it at the
// next generation, resumed.
func (f *Fake) setServiceAlarm(_ context.Context, request *workspacev1.SetServiceAlarmRequest) (*workspacev1.SetServiceAlarmResponse, error) {
	set := &workspacev1.ServiceAlarm{
		Name:             request.GetName(),
		Timezone:         request.GetTimezone(),
		Channel:          request.GetChannel(),
		Event:            request.GetEvent(),
		Data:             slices.Clone(request.GetData()),
		Misfire:          request.GetMisfire(),
		StartingDeadline: request.GetStartingDeadline(),
		Generation:       1,
	}
	switch schedule := request.GetSchedule().(type) {
	case *workspacev1.SetServiceAlarmRequest_Cron:
		set.Schedule = &workspacev1.ServiceAlarm_Cron{Cron: schedule.Cron}
	case *workspacev1.SetServiceAlarmRequest_At:
		set.Schedule = &workspacev1.ServiceAlarm_At{At: schedule.At}
		set.NextAt = schedule.At
	}
	if set.Misfire == workspacev1.ServiceAlarmMisfire_SERVICE_ALARM_MISFIRE_UNSPECIFIED {
		set.Misfire = workspacev1.ServiceAlarmMisfire_SERVICE_ALARM_MISFIRE_COALESCE
	}
	if set.StartingDeadline == nil {
		set.StartingDeadline = durationpb.New(defaultStartingDeadline)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	key := alarmKey{service: request.GetServiceId(), name: request.GetName()}
	if record, ok := f.alarms[key]; ok {
		set.Generation = record.alarm.GetGeneration() + 1
		record.alarm = set
	} else {
		f.alarms[key] = &alarm{alarm: set}
	}
	return &workspacev1.SetServiceAlarmResponse{Alarm: proto.CloneOf(set)}, nil
}

func (f *Fake) getServiceAlarm(_ context.Context, request *workspacev1.GetServiceAlarmRequest) (*workspacev1.GetServiceAlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, err := f.alarm(request.GetServiceId(), request.GetName())
	if err != nil {
		return nil, err
	}
	response := &workspacev1.GetServiceAlarmResponse{Alarm: proto.CloneOf(record.alarm)}
	for _, occurrence := range record.occurrences[:min(len(record.occurrences), maxOccurrences)] {
		response.Occurrences = append(response.Occurrences, proto.CloneOf(occurrence))
	}
	return response, nil
}

func (f *Fake) listServiceAlarms(_ context.Context, request *workspacev1.ListServiceAlarmsRequest) (*workspacev1.ListServiceAlarmsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	response := &workspacev1.ListServiceAlarmsResponse{}
	for key, record := range f.alarms {
		if key.service == request.GetServiceId() {
			response.Alarms = append(response.Alarms, proto.CloneOf(record.alarm))
		}
	}
	slices.SortFunc(response.Alarms, func(a, b *workspacev1.ServiceAlarm) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	return response, nil
}

func (f *Fake) deleteServiceAlarm(_ context.Context, request *workspacev1.DeleteServiceAlarmRequest) (*workspacev1.DeleteServiceAlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.alarm(request.GetServiceId(), request.GetName()); err != nil {
		return nil, err
	}
	delete(f.alarms, alarmKey{service: request.GetServiceId(), name: request.GetName()})
	return &workspacev1.DeleteServiceAlarmResponse{}, nil
}

func (f *Fake) pauseServiceAlarm(_ context.Context, request *workspacev1.PauseServiceAlarmRequest) (*workspacev1.PauseServiceAlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, err := f.alarm(request.GetServiceId(), request.GetName())
	if err != nil {
		return nil, err
	}
	record.alarm.Paused = true
	return &workspacev1.PauseServiceAlarmResponse{Alarm: proto.CloneOf(record.alarm)}, nil
}

func (f *Fake) resumeServiceAlarm(_ context.Context, request *workspacev1.ResumeServiceAlarmRequest) (*workspacev1.ResumeServiceAlarmResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, err := f.alarm(request.GetServiceId(), request.GetName())
	if err != nil {
		return nil, err
	}
	record.alarm.Paused = false
	return &workspacev1.ResumeServiceAlarmResponse{Alarm: proto.CloneOf(record.alarm)}, nil
}

// runServiceAlarmNow records a manual occurrence at the alarm's generation. A
// request key the fake has already answered returns the same occurrence.
func (f *Fake) runServiceAlarmNow(_ context.Context, request *workspacev1.RunServiceAlarmNowRequest) (*workspacev1.RunServiceAlarmNowResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if occurrence, ok := f.alarmRuns[request.GetRequestKey()]; ok && request.GetRequestKey() != "" {
		return &workspacev1.RunServiceAlarmNowResponse{Occurrence: proto.CloneOf(occurrence)}, nil
	}
	record, err := f.alarm(request.GetServiceId(), request.GetName())
	if err != nil {
		return nil, err
	}
	occurrence := &workspacev1.ServiceAlarmOccurrence{
		Generation: record.alarm.GetGeneration(),
		Manual:     true,
	}
	record.occurrences = append([]*workspacev1.ServiceAlarmOccurrence{occurrence}, record.occurrences...)
	if request.GetRequestKey() != "" {
		f.alarmRuns[request.GetRequestKey()] = occurrence
	}
	return &workspacev1.RunServiceAlarmNowResponse{Occurrence: proto.CloneOf(occurrence)}, nil
}

// alarm returns the alarm of a service with this name. The caller holds f.mu.
func (f *Fake) alarm(service, name string) (*alarm, error) {
	record, ok := f.alarms[alarmKey{service: service, name: name}]
	if !ok {
		return nil, notFound("alarm", service+"/"+name)
	}
	return record, nil
}
