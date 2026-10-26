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
