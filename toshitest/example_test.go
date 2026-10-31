package toshitest_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"connectrpc.com/connect"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	workspacev1connect "go.toshicloud.io/api/workspacev1/workspacev1connect"

	"go.toshicloud.io/toshi/toshitest"
)

// ExampleNew starts a fake Toshi, describes the job to run, and reads its
// output. In a test the argument is the test, which then closes the fake.
func ExampleNew() {
	tt := toshitest.New(nil) // nil: the example closes the fake itself
	defer tt.Close()

	tt.Job("job-1").Stdout("deployed\n").ExitCode(0)
	run, err := tt.Client().Run(context.Background(), &workspacev1.CreateJobRequest{})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.Copy(os.Stdout, run.Stdout()); err != nil {
		log.Print(err)
	}
	if err := run.Wait(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// deployed
}

// ExampleOn answers one procedure with a hook.
func ExampleOn() {
	tt := toshitest.New(nil)
	defer tt.Close()

	toshitest.On(tt, workspacev1connect.ProjectServiceListProjectsProcedure,
		func(_ context.Context, _ *workspacev1.ListProjectsRequest) (*workspacev1.ListProjectsResponse, error) {
			return &workspacev1.ListProjectsResponse{
				Projects: []*workspacev1.Project{{Name: "default"}},
			}, nil
		})

	response, err := tt.Client().Project().ListProjects(context.Background(), connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response.Msg.GetProjects()[0].GetName())
	// Output:
	// default
}

// ExampleFake_serviceAlarms sets a service's alarm, runs it once and reads it
// back. The fake serves any Connect client, here the generated one.
func ExampleFake_serviceAlarms() {
	tt := toshitest.New(nil)
	defer tt.Close()

	ctx := context.Background()
	workload := workspacev1connect.NewWorkloadServiceClient(tt.HTTPClient(), tt.URL())
	_, err := workload.SetServiceAlarm(ctx, connect.NewRequest(&workspacev1.SetServiceAlarmRequest{
		ServiceId: "svc-1",
		Name:      "nightly",
		Schedule:  &workspacev1.SetServiceAlarmRequest_Cron{Cron: "0 3 * * *"},
		Channel:   "reports",
		Event:     "report.due",
	}))
	if err != nil {
		log.Fatal(err)
	}
	_, err = workload.RunServiceAlarmNow(ctx, connect.NewRequest(&workspacev1.RunServiceAlarmNowRequest{
		ServiceId: "svc-1", Name: "nightly", RequestKey: "run-1",
	}))
	if err != nil {
		log.Fatal(err)
	}
	got, err := workload.GetServiceAlarm(ctx, connect.NewRequest(&workspacev1.GetServiceAlarmRequest{
		ServiceId: "svc-1", Name: "nightly",
	}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(got.Msg.GetAlarm().GetCron(), len(got.Msg.GetOccurrences()), got.Msg.GetOccurrences()[0].GetManual())
	// Output:
	// 0 3 * * * 1 true
}
