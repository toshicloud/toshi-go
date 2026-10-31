# Testing with toshitest


`toshitest` starts a fake Toshi inside a test: a TLS server speaking HTTP/2
that serves every service of the Workspace API, and a client wired to it. A
test describes what the calls answer, then calls the client as it would call a
real one. `toshitest.New(t)` closes the fake with `t.Cleanup`; the example
below runs without a test, so it passes nil and closes the fake itself.

<!-- toshitest.ExampleNew -->
```go
import (
	"context"
	"io"
	"log"
	"os"

	"go.toshicloud.io/api/workspacev1"
	"go.toshicloud.io/toshi/toshitest"
)

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
```

`Job`, `Sandbox` and `Execute` set the state of a job, a sandbox or a command
and the output and exit code it answers. `On`, `OnServerStream` and `OnBidi`
replace the fake's answer for one procedure with a hook, typed at the call
site; a hook that does not fit its procedure fails the test at setup. `Calls`
returns what the client sent, in order. The package documentation lists what
the fake answers and what needs a hook.

The fake keeps a service's alarms too: the example below calls the alarm
procedures through the module's `Workload` client.

<!-- toshitest.ExampleFake_serviceAlarms -->
```go
import (
	"context"
	"fmt"
	"log"

	"go.toshicloud.io/api/workspacev1"
	"go.toshicloud.io/toshi/toshitest"
)

tt := toshitest.New(nil)
defer tt.Close()

ctx := context.Background()
client := tt.Client()
_, err := client.Workload().Services().SetAlarm(ctx, &workspacev1.SetServiceAlarmRequest{
	ServiceId: "svc-1",
	Name:      "nightly",
	Schedule:  &workspacev1.SetServiceAlarmRequest_Cron{Cron: "0 3 * * *"},
	Channel:   "reports",
	Event:     "report.due",
})
if err != nil {
	log.Fatal(err)
}
_, err = client.Workload().Services().RunAlarmNow(ctx, &workspacev1.RunServiceAlarmNowRequest{
	ServiceId: "svc-1", Name: "nightly", RequestKey: "run-1",
})
if err != nil {
	log.Fatal(err)
}
got, err := client.Workload().Services().GetAlarm(ctx, &workspacev1.GetServiceAlarmRequest{
	ServiceId: "svc-1", Name: "nightly",
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(got.GetAlarm().GetCron(), len(got.GetOccurrences()), got.GetOccurrences()[0].GetManual())
// Output:
// 0 3 * * * 1 true
```

