package toshi_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"go.toshicloud.io/api/workspacev1"

	"go.toshicloud.io/toshi"
)

// The examples that call a server have no Output comment: go test compiles
// them but does not run them. README.md copies some of them; readme_test.go
// keeps the copies in step.

func ExampleNew() {
	ctx := context.Background()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	jobs, err := client.Workload.Jobs.List(ctx, &workspacev1.ListJobsRequest{})
	if err != nil {
		log.Fatal(err)
	}
	for _, job := range jobs.GetJobs() {
		fmt.Println(job.GetId(), job.GetStatus().GetState())
	}
}

func ExampleJobsClient_All() {
	ctx := context.Background()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	for job, err := range client.Workload.Jobs.All(ctx, &workspacev1.ListJobsRequest{}) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(job.GetId(), job.GetStatus().GetState())
	}
}

func ExampleWithScope() {
	ctx := context.Background()
	client, err := toshi.New(
		toshi.WithServer("https://toshi.example.com"),
		toshi.WithTokenFile("/run/secrets/toshi-token"),
		toshi.WithScope("default", "production"),
	)
	if err != nil {
		log.Fatal(err)
	}
	// The request names no scope: it lists the jobs of default/production.
	production, err := client.Workload.Jobs.List(ctx, &workspacev1.ListJobsRequest{})
	if err != nil {
		log.Fatal(err)
	}
	// A field the request sets is kept: this lists default/staging.
	staging, err := client.Workload.Jobs.List(ctx, &workspacev1.ListJobsRequest{
		Environment: "staging",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(production.GetJobs()), len(staging.GetJobs()))
}

func ExampleWithRetryPolicy() {
	policy := toshi.DefaultRetryPolicy()
	fmt.Printf("%d attempts, backoff from %v up to %v\n",
		policy.MaxAttempts, policy.InitialBackoff, policy.MaxBackoff)

	// Ride out a longer outage than the default allows.
	policy.MaxAttempts = 10
	policy.MaxBackoff = 30 * time.Second
	client, err := toshi.New(
		toshi.WithServer("https://toshi.example.com"),
		toshi.WithToken("example-token"),
		toshi.WithRetryPolicy(policy),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = client // Use the client as in the other examples.
	// Output:
	// 5 attempts, backoff from 200ms up to 10s
}

func ExampleWithInterceptors() {
	logCalls := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			slog.InfoContext(ctx, "toshi call",
				"procedure", req.Spec().Procedure,
				"duration", time.Since(start),
				"err", err)
			return resp, err
		}
	})
	client, err := toshi.New(
		toshi.WithScope("default", "production"),
		toshi.WithInterceptors(logCalls),
		toshi.WithUserAgent("myapp/1.2"),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = client // Use the client as in the other examples.
}

func ExampleWithTokenSource() {
	var token atomic.Value // Store each new token here, for example from a refresh loop.
	token.Store("first-token")
	client, err := toshi.New(toshi.WithTokenSource(func(context.Context) (string, error) {
		return token.Load().(string), nil
	}))
	if err != nil {
		log.Fatal(err)
	}
	_ = client // Use the client as in the other examples.
}

func ExampleClient_Run() {
	ctx := context.Background()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	run, err := client.Run(ctx, &workspacev1.CreateJobRequest{
		Spec: &workspacev1.JobSpec{
			Command: &workspacev1.Command{Argv: []string{"go", "test", "./..."}},
		},
	})
	if connect.CodeOf(err) == connect.CodeResourceExhausted {
		// RESOURCE_EXHAUSTED is never retried: it comes back to the caller.
		log.Fatalf("job not created: %v", err)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Read standard output and standard error at the same time.
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := io.Copy(os.Stderr, run.Stderr()); err != nil {
			log.Print(err)
		}
	})
	if _, err := io.Copy(os.Stdout, run.Stdout()); err != nil {
		log.Print(err)
	}
	wg.Wait()

	var exit *toshi.JobExitError
	err = run.Wait()
	if errors.As(err, &exit) {
		log.Fatalf("job %s: exit code %d", exit.JobID, exit.End.GetExitCode())
	}
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleJobsClient_Wait() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := client.Workload.Jobs.Wait(ctx, &workspacev1.WaitJobRequest{
		Id:     "job-123",
		States: []workspacev1.JobState{workspacev1.JobState_JOB_STATE_ENDED},
	})
	if err != nil {
		log.Fatal(err)
	}
	status := resp.GetJob().GetStatus()
	fmt.Println(status.GetState(), status.GetExitCode())
}

func ExampleJobsClient_Watch() {
	ctx := context.Background()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	watch, err := client.Workload.Jobs.Watch(ctx, &workspacev1.WatchJobsRequest{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = watch.Close() }()

	// With no cursor, the watch lists the jobs first.
	for _, job := range watch.Snapshot() {
		fmt.Println(job.GetId(), job.GetStatus().GetState())
	}
	// Then it reports each change, resuming after a dropped connection.
	for watch.Receive() {
		msg := watch.Msg()
		fmt.Println(msg.GetType(), msg.GetJob().GetId(), msg.GetJob().GetStatus().GetState())
	}
	if err := watch.Err(); err != nil {
		log.Fatal(err)
	}
}

func ExampleSandboxesClient_Execute() {
	ctx := context.Background()
	client, err := toshi.New(toshi.WithScope("default", "production"))
	if err != nil {
		log.Fatal(err)
	}
	proc, err := client.Workload.Sandboxes.Execute(ctx, &workspacev1.ExecuteStart{
		SandboxId: "sbx-123",
		Command:   &workspacev1.Command{Argv: []string{"tr", "a-z", "A-Z"}},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Read standard output and standard error while writing standard input.
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := io.Copy(os.Stdout, proc.Stdout()); err != nil {
			log.Print(err)
		}
	})
	wg.Go(func() {
		if _, err := io.Copy(os.Stderr, proc.Stderr()); err != nil {
			log.Print(err)
		}
	})
	if _, err := io.Copy(proc.Stdin(), strings.NewReader("hello\n")); err != nil {
		log.Print(err)
	}
	if err := proc.Stdin().Close(); err != nil {
		log.Print(err)
	}
	wg.Wait()

	var exit *toshi.ExitError
	err = proc.Wait()
	if errors.As(err, &exit) {
		log.Fatalf("command failed: %v", exit)
	}
	if err != nil {
		// A *toshi.StreamLostError: the connection dropped before the exit.
		log.Fatal(err)
	}
}

func ExampleStreamLostError() {
	// Process.Wait returns a *StreamLostError when the connection drops
	// before the command's exit.
	var err error = &toshi.StreamLostError{
		ExecutionID: "exec-1",
		RequestKey:  "key-1",
		Err:         io.ErrUnexpectedEOF,
	}

	var lost *toshi.StreamLostError
	if errors.As(err, &lost) {
		// Standard input cannot be replayed, so the caller decides whether
		// to start the command again with the same request key.
		fmt.Println("request key:", lost.RequestKey)
	}
	fmt.Println(errors.Is(err, io.ErrUnexpectedEOF))
	fmt.Println(err)
	// Output:
	// request key: key-1
	// true
	// toshi: execution exec-1 stream lost (request key key-1): unexpected EOF
}
