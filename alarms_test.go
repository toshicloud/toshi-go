package toshi_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"

	"go.toshicloud.io/toshi/toshitest"
)

// TestServiceAlarmsThroughTheClient proves a service's alarms are set, read,
// listed, paused, resumed, run now and deleted through the client, with the
// scope the client fills in.
func TestServiceAlarmsThroughTheClient(t *testing.T) {
	ctx := context.Background()
	tt := toshitest.New(t)
	client := tt.Client()
	alarms := client.Workload().Services()

	set, err := alarms.SetAlarm(ctx, &workspacev1.SetServiceAlarmRequest{
		ServiceId: "svc-1",
		Name:      "nightly",
		Schedule:  &workspacev1.SetServiceAlarmRequest_Cron{Cron: "0 3 * * *"},
		Timezone:  "Europe/Berlin",
		Endpoint:  "reports",
		Event:     "report.due",
		Data:      []byte(`{"kind":"nightly"}`),
	})
	if err != nil {
		t.Fatalf("SetAlarm: %v", err)
	}
	if got := set.GetAlarm(); got.GetName() != "nightly" || got.GetCron() != "0 3 * * *" ||
		got.GetTimezone() != "Europe/Berlin" || got.GetEndpoint() != "reports" ||
		got.GetEvent() != "report.due" || string(got.GetData()) != `{"kind":"nightly"}` ||
		got.GetGeneration() != 1 {
		t.Fatalf("SetAlarm answered %v", got)
	}

	got, err := alarms.GetAlarm(ctx, &workspacev1.GetServiceAlarmRequest{ServiceId: "svc-1", Name: "nightly"})
	if err != nil {
		t.Fatalf("GetAlarm: %v", err)
	}
	if alarm := got.GetAlarm(); alarm.GetName() != "nightly" || alarm.GetCron() != "0 3 * * *" ||
		alarm.GetEndpoint() != "reports" || alarm.GetEvent() != "report.due" {
		t.Fatalf("GetAlarm answered %v", alarm)
	}
	if occurrences := got.GetOccurrences(); len(occurrences) != 0 {
		t.Fatalf("GetAlarm answered occurrences %v before any run", occurrences)
	}

	if _, err := alarms.SetAlarm(ctx, &workspacev1.SetServiceAlarmRequest{
		ServiceId: "svc-1",
		Name:      "hourly",
		Schedule:  &workspacev1.SetServiceAlarmRequest_Cron{Cron: "0 * * * *"},
		Endpoint:  "reports",
		Event:     "report.hourly",
	}); err != nil {
		t.Fatalf("SetAlarm hourly: %v", err)
	}
	list, err := alarms.ListAlarms(ctx, &workspacev1.ListServiceAlarmsRequest{ServiceId: "svc-1"})
	if err != nil {
		t.Fatalf("ListAlarms: %v", err)
	}
	if names := alarmNames(list.GetAlarms()); len(names) != 2 || names[0] != "hourly" || names[1] != "nightly" {
		t.Fatalf("ListAlarms answered %v, want hourly and nightly by name", names)
	}

	paused, err := alarms.PauseAlarm(ctx, &workspacev1.PauseServiceAlarmRequest{ServiceId: "svc-1", Name: "nightly"})
	if err != nil {
		t.Fatalf("PauseAlarm: %v", err)
	}
	if !paused.GetAlarm().GetPaused() {
		t.Fatalf("PauseAlarm answered %v, want the alarm paused", paused.GetAlarm())
	}

	resumed, err := alarms.ResumeAlarm(ctx, &workspacev1.ResumeServiceAlarmRequest{ServiceId: "svc-1", Name: "nightly"})
	if err != nil {
		t.Fatalf("ResumeAlarm: %v", err)
	}
	if resumed.GetAlarm().GetPaused() {
		t.Fatalf("ResumeAlarm answered %v, want the alarm resumed", resumed.GetAlarm())
	}

	ran, err := alarms.RunAlarmNow(ctx, &workspacev1.RunServiceAlarmNowRequest{ServiceId: "svc-1", Name: "nightly"})
	if err != nil {
		t.Fatalf("RunAlarmNow: %v", err)
	}
	if occurrence := ran.GetOccurrence(); !occurrence.GetManual() || occurrence.GetGeneration() != 1 {
		t.Fatalf("RunAlarmNow answered occurrence %v, want a manual run at generation 1", occurrence)
	}

	if _, err := alarms.DeleteAlarm(ctx, &workspacev1.DeleteServiceAlarmRequest{ServiceId: "svc-1", Name: "nightly"}); err != nil {
		t.Fatalf("DeleteAlarm: %v", err)
	}
	if _, err := alarms.GetAlarm(ctx, &workspacev1.GetServiceAlarmRequest{ServiceId: "svc-1", Name: "nightly"}); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetAlarm after DeleteAlarm: %v, want NOT_FOUND", err)
	}

	for _, call := range tt.Calls() {
		request, ok := call.Request.(interface {
			GetProject() string
			GetEnvironment() string
		})
		if !ok {
			t.Fatalf("%s carried %T; want a request with a scope", call.Procedure, call.Request)
		}
		if request.GetProject() != "default" || request.GetEnvironment() != "production" {
			t.Errorf("%s's scope is %q/%q", call.Procedure, request.GetProject(), request.GetEnvironment())
		}
	}
}

// alarmNames returns the names of alarms.
func alarmNames(alarms []*workspacev1.ServiceAlarm) []string {
	names := make([]string, len(alarms))
	for i, alarm := range alarms {
		names[i] = alarm.GetName()
	}
	return names
}
