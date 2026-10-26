package integration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	"go.toshicloud.io/toshi"
)

const (
	installationID = "4242"
	account        = "acme"
	projectName    = "default"
	environment    = "production"
)

func TestIntegration(t *testing.T) {
	bin := os.Getenv("TOSHI_INTEGRATION_BIN")
	if bin == "" {
		t.Skip("TOSHI_INTEGRATION_BIN is not set: it names the toshi binary this test runs against")
	}
	bin, err := filepath.Abs(bin)
	if err != nil {
		t.Fatal(err)
	}

	// Before anything: the served commit.
	version, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("toshi version: %v", err)
	}
	commit := ""
	for _, line := range strings.Split(string(version), "\n") {
		if rest, ok := strings.CutPrefix(line, "commit: "); ok {
			commit = strings.TrimSpace(rest)
		}
	}
	if commit == "" {
		t.Fatalf("toshi version printed no commit:\n%s", version)
	}
	t.Logf("served Toshi commit: %s", commit)
	if want := os.Getenv("TOSHI_INTEGRATION_EXPECT_COMMIT"); want != "" && want != commit {
		t.Fatalf("served Toshi commit %s, want %s", commit, want)
	}

	dir := t.TempDir()
	caPath, certPath, keyPath := mintTLS(t, dir)
	appKeyPath := writeAppKey(t, dir)
	webhookPath := writeSecretFile(t, dir, "webhook.secret", 32)
	operatorPath := writeSecretFile(t, dir, "operator.token", 32)
	localKeyPath := writeSecretFile(t, dir, "secrets.key", 32)
	tokenPath := filepath.Join(dir, "tenant.token")

	servePort := freePort(t)
	fakePort := freePort(t)
	baseURL := fmt.Sprintf("https://127.0.0.1:%d", servePort)
	configPath := filepath.Join(dir, "toshi.toml")
	writeConfig(t, configPath, dir, servePort, fakePort, certPath, keyPath, appKeyPath, webhookPath, operatorPath, localKeyPath)

	fakeLog := filepath.Join(dir, "fake.log")
	serveLog := filepath.Join(dir, "serve.log")
	startChild(t, fakeLog, nil, bin, "dev", "github", "serve",
		"--i-know-this-is-fake",
		"--listen", fmt.Sprintf("127.0.0.1:%d", fakePort),
		"--installation", installationID+":"+account,
	)
	waitPort(t, fakePort, 20*time.Second)
	startChild(t, serveLog, nil, bin, "serve", "--config", configPath)

	pool := certPool(t, caPath)
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		Timeout:   5 * time.Second,
	}
	waitReady(t, httpClient, baseURL+"/readyz", 60*time.Second)

	operatorEnv := map[string]string{
		"TOSHI_SERVER":     baseURL,
		"TOSHI_CA_FILE":    caPath,
		"TOSHI_TOKEN_FILE": operatorPath,
	}

	// Make a workspace through the documented dev GitHub flow.
	send := run(t, nil, bin, "dev", "github", "send",
		"--i-know-this-is-fake",
		"--url", baseURL+"/webhooks/github",
		"--ca-file", caPath,
		"--secret-file", webhookPath,
		"--installation", installationID,
		"--repo", account+"/app",
		"--event", "installation",
		"--action", "created",
	)
	t.Logf("installation event: %s", strings.TrimSpace(send))
	waitTenant(t, bin, operatorEnv, installationID, 30*time.Second)

	// A project-wide token, so the shared secret set is reachable too. An
	// environment-limited token is refused the shared set by the API.
	token := strings.TrimSpace(run(t, operatorEnv, bin, "tokens", "create",
		"--tenant", installationID,
		"--name", "sdk-integration",
		"--project", projectName,
	))
	if token == "" {
		t.Fatal("tokens create printed no token")
	}
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	client, err := toshi.New(
		toshi.WithServer(baseURL),
		toshi.WithToken(token),
		toshi.WithCAFile(caPath),
		toshi.WithScope(projectName, environment),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var firstID, secondID string

	t.Run("ListProjects", func(t *testing.T) {
		resp, err := client.Projects.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{}))
		if err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
		found := false
		for _, project := range resp.Msg.GetProjects() {
			if project.GetName() != projectName {
				continue
			}
			for _, env := range project.GetEnvironments() {
				if env.GetName() == environment {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("projects %v do not hold %s/%s", resp.Msg.GetProjects(), projectName, environment)
		}
	})

	t.Run("CreateWorkload", func(t *testing.T) {
		create := func(key string) (*workspacev1.CreateWorkloadRequest, *workspacev1.Workload) {
			t.Helper()
			req := &workspacev1.CreateWorkloadRequest{
				Labels:     map[string]string{"suite": "sdk-integration"},
				Spec:       &workspacev1.CreateWorkloadRequest_Job{Job: &workspacev1.JobSpec{Command: &workspacev1.Command{Argv: []string{"true"}}}},
				RequestKey: key,
			}
			resp, err := client.Workspace.CreateWorkload(ctx, connect.NewRequest(req))
			if err != nil {
				t.Fatalf("CreateWorkload: %v", err)
			}
			if resp.Msg.GetWorkload().GetId() == "" {
				t.Fatal("CreateWorkload returned no workload")
			}
			return req, resp.Msg.GetWorkload()
		}
		_, first := create("sdk-fixed-key")
		_, again := create("sdk-fixed-key")
		if first.GetId() != again.GetId() {
			t.Fatalf("same key made %q and %q", first.GetId(), again.GetId())
		}
		third, filled := create("")
		if third.GetRequestKey() == "" {
			t.Fatal("the client left the request key empty")
		}
		firstID, secondID = first.GetId(), filled.GetId()
	})

	t.Run("GetAndList", func(t *testing.T) {
		got, err := client.Workspace.GetWorkload(ctx, connect.NewRequest(&workspacev1.GetWorkloadRequest{Id: firstID}))
		if err != nil {
			t.Fatalf("GetWorkload: %v", err)
		}
		if got.Msg.GetWorkload().GetLabels()["suite"] != "sdk-integration" {
			t.Fatalf("labels %v", got.Msg.GetWorkload().GetLabels())
		}
		list, err := client.Workspace.ListWorkloads(ctx, connect.NewRequest(&workspacev1.ListWorkloadsRequest{
			Labels: map[string]string{"suite": "sdk-integration"},
		}))
		if err != nil {
			t.Fatalf("ListWorkloads: %v", err)
		}
		ids := map[string]bool{}
		for _, workload := range list.Msg.GetWorkloads() {
			ids[workload.GetId()] = true
		}
		if len(ids) != 2 || !ids[firstID] || !ids[secondID] {
			t.Fatalf("workloads %v, want %q and %q", ids, firstID, secondID)
		}
	})

	t.Run("WatchWorkloads", func(t *testing.T) {
		watchCtx, watchCancel := context.WithTimeout(ctx, 20*time.Second)
		defer watchCancel()
		watch, err := client.WatchWorkloads(watchCtx, &workspacev1.WatchWorkloadsRequest{})
		if err != nil {
			t.Fatalf("WatchWorkloads: %v", err)
		}
		defer func() { _ = watch.Close() }()
		if len(watch.Snapshot()) < 2 {
			t.Fatalf("snapshot %v, want the created workloads", watch.Snapshot())
		}
		_, err = client.Workspace.PatchWorkload(ctx, connect.NewRequest(&workspacev1.PatchWorkloadRequest{
			Id:         secondID,
			Workload:   &workspacev1.Workload{Labels: map[string]string{"suite": "sdk-integration", "stage": "watch"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}))
		if err != nil {
			t.Fatalf("PatchWorkload: %v", err)
		}
		for {
			if !watch.Receive() {
				t.Fatalf("watch ended: %v", watch.Err())
			}
			msg := watch.Msg()
			if msg.GetWorkload().GetId() == secondID && msg.GetWorkload().GetLabels()["stage"] == "watch" {
				if watch.Cursor() == "" {
					t.Fatal("cursor is empty after a change")
				}
				return
			}
		}
	})

	t.Run("Events", func(t *testing.T) {
		watchCtx, watchCancel := context.WithTimeout(ctx, 20*time.Second)
		defer watchCancel()
		first, err := client.WatchEvents(watchCtx, &workspacev1.WatchEventsRequest{})
		if err != nil {
			t.Fatalf("WatchEvents: %v", err)
		}
		created := false
		for !created {
			if !first.Receive() {
				t.Fatalf("event watch ended: %v", first.Err())
			}
			event := first.Event()
			if event.GetType() == workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED && event.GetWorkloadId() == firstID {
				created = true
			}
		}
		start := first.Cursor()
		if start == "" {
			t.Fatal("no cursor after a listed event")
		}
		if err := first.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		second, err := client.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: start})
		if err != nil {
			t.Fatalf("WatchEvents from cursor: %v", err)
		}
		defer func() { _ = second.Close() }()
		_, err = client.Workspace.PatchWorkload(ctx, connect.NewRequest(&workspacev1.PatchWorkloadRequest{
			Id:         firstID,
			Workload:   &workspacev1.Workload{Labels: map[string]string{"suite": "sdk-integration", "stage": "events"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		}))
		if err != nil {
			t.Fatalf("PatchWorkload: %v", err)
		}
		for {
			if !second.Receive() {
				t.Fatalf("event watch from cursor ended: %v", second.Err())
			}
			event := second.Event()
			if event.GetType() == workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED && event.GetWorkloadId() == firstID {
				t.Fatal("the watch started again from the oldest event")
			}
			if event.GetWorkloadId() == firstID && event.GetType() == workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED {
				return
			}
		}
	})

	t.Run("WaitWorkload", func(t *testing.T) {
		waitCtx, waitCancel := context.WithTimeout(ctx, 4*time.Second)
		defer waitCancel()
		started := time.Now()
		_, err := client.WaitWorkload(waitCtx, &workspacev1.WaitWorkloadRequest{
			Id:      firstID,
			States:  []workspacev1.WorkloadState{workspacev1.WorkloadState_WORKLOAD_STATE_ENDED},
			Timeout: durationpb.New(time.Second),
		})
		elapsed := time.Since(started)
		if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Fatalf("WaitWorkload: %v, want DEADLINE_EXCEEDED", err)
		}
		if elapsed < 3*time.Second {
			t.Fatalf("WaitWorkload returned after %v, want at least 3s", elapsed)
		}
	})

	t.Run("Execute", func(t *testing.T) {
		_, err := client.Execute(ctx, &workspacev1.ExecuteStart{
			WorkloadId: firstID,
			Command:    &workspacev1.Command{Argv: []string{"true"}},
		})
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("Execute: %v, want FAILED_PRECONDITION", err)
		}
		var lost *toshi.StreamLostError
		if errors.As(err, &lost) {
			t.Fatalf("Execute: %v is a StreamLostError", err)
		}
	})

	t.Run("Secrets", func(t *testing.T) {
		if _, err := client.Secrets.CreateSecretSet(ctx, connect.NewRequest(&workspacev1.CreateSecretSetRequest{})); err != nil {
			t.Fatalf("CreateSecretSet: %v", err)
		}
		if _, err := client.Secrets.CreateSecretSet(ctx, connect.NewRequest(&workspacev1.CreateSecretSetRequest{Shared: true})); err != nil {
			t.Fatalf("CreateSecretSet shared: %v", err)
		}
		set, err := client.Secrets.SetSecret(ctx, connect.NewRequest(&workspacev1.SetSecretRequest{Name: "sdk_key", Value: []byte("sdk-value")}))
		if err != nil {
			t.Fatalf("SetSecret: %v", err)
		}
		if got := set.Msg.GetSecret().GetVersion(); got != 1 {
			t.Fatalf("secret version %d, want 1 (SetSecret retried?)", got)
		}
		list, err := client.Secrets.ListSecrets(ctx, connect.NewRequest(&workspacev1.ListSecretsRequest{}))
		if err != nil {
			t.Fatalf("ListSecrets: %v", err)
		}
		if names := secretNames(list.Msg.GetSecrets()); !names["sdk_key"] {
			t.Fatalf("environment secrets %v, want sdk_key", names)
		}
		shared, err := client.Secrets.ListSecrets(ctx, connect.NewRequest(&workspacev1.ListSecretsRequest{Shared: true}))
		if err != nil {
			t.Fatalf("ListSecrets shared: %v", err)
		}
		if names := secretNames(shared.Msg.GetSecrets()); names["sdk_key"] {
			t.Fatalf("the shared set holds sdk_key: %v", names)
		}
	})

	t.Run("DeleteAndWaitOperation", func(t *testing.T) {
		deleted, err := client.Workspace.DeleteWorkload(ctx, connect.NewRequest(&workspacev1.DeleteWorkloadRequest{Id: secondID}))
		if err != nil {
			t.Fatalf("DeleteWorkload: %v", err)
		}
		operation := deleted.Msg.GetOperation()
		if operation.GetId() == "" {
			t.Fatal("DeleteWorkload returned no operation")
		}
		done, err := client.WaitOperation(ctx, &workspacev1.WaitOperationRequest{
			Id:      operation.GetId(),
			Timeout: durationpb.New(5 * time.Second),
		})
		if err != nil {
			t.Fatalf("WaitOperation: %v", err)
		}
		if !done.GetDone() {
			t.Fatalf("operation %q is not done", done.GetId())
		}
	})

	t.Run("EnvironmentOptions", func(t *testing.T) {
		t.Setenv("TOSHI_SERVER", baseURL)
		t.Setenv("TOSHI_TOKEN", "")
		t.Setenv("TOSHI_TOKEN_FILE", tokenPath)
		t.Setenv("TOSHI_CA_FILE", caPath)
		envClient, err := toshi.New()
		if err != nil {
			t.Fatalf("New from environment: %v", err)
		}
		if _, err := envClient.Workspace.ListWorkloads(ctx, connect.NewRequest(&workspacev1.ListWorkloadsRequest{
			Project:     projectName,
			Environment: environment,
		})); err != nil {
			t.Fatalf("ListWorkloads from the environment client: %v", err)
		}
	})
}

func secretNames(secrets []*workspacev1.Secret) map[string]bool {
	names := make(map[string]bool, len(secrets))
	for _, secret := range secrets {
		names[secret.GetName()] = true
	}
	return names
}

// mintTLS makes a CA and a leaf for 127.0.0.1 and localhost.
func mintTLS(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sdk integration CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writePEM(t, caPath, "CERTIFICATE", caDER, 0o644)
	writePEM(t, certPath, "CERTIFICATE", leafDER, 0o644)
	writePEM(t, keyPath, "PRIVATE KEY", keyDER, 0o600)
	return caPath, certPath, keyPath
}

// writeAppKey writes a throwaway RSA key for the fake GitHub App.
func writeAppKey(t *testing.T, dir string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "app.pem")
	writePEM(t, path, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), 0o600)
	return path
}

// writeSecretFile writes n random bytes as base64 on one line.
func writeSecretFile(t *testing.T, dir, name string, n int) string {
	t.Helper()
	secret := make([]byte, n)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(secret)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writePEM(t *testing.T, path, blockType string, der []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), mode); err != nil {
		t.Fatal(err)
	}
}

// writeConfig writes the control plane's configuration.
func writeConfig(t *testing.T, path, dir string, servePort, fakePort int, certPath, keyPath, appKeyPath, webhookPath, operatorPath, localKeyPath string) {
	t.Helper()
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`listen = "127.0.0.1:%d"
data_dir = %q
log_level = "debug"

tls_cert_file = %q
tls_key_file = %q

[store]
driver = "sqlite"

[operator]
token_file = %q

[secrets]
local_key_file = %q

[github]
app_id = 1
base_url = "http://127.0.0.1:%d"
private_key_file = %q
webhook_secret_file = %q
`, servePort, dataDir, certPath, keyPath, operatorPath, localKeyPath, fakePort, appKeyPath, webhookPath)
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func certPool(t *testing.T, caPath string) *x509.CertPool {
	t.Helper()
	b, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		t.Fatal("CA file holds no certificate")
	}
	return pool
}

// waitPort waits for a loopback port to accept a connection.
func waitPort(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("127.0.0.1:%d did not start", port)
}

func waitReady(t *testing.T, client *http.Client, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready", url)
}

// waitTenant waits for the installation's tenant to appear.
func waitTenant(t *testing.T, bin string, env map[string]string, tenant string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out := runNoFail(bin, env, "tenants", "list", "--json")
		last = out
		var payload struct {
			Tenants []struct {
				ID string `json:"id"`
			} `json:"tenants"`
		}
		if err := json.Unmarshal([]byte(out), &payload); err == nil {
			for _, got := range payload.Tenants {
				if got.ID == tenant {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("tenant %s did not appear:\n%s", tenant, last)
}

// startChild runs a child process, logging to path, and kills only it in
// cleanup. On failure the log's tail is printed.
func startChild(t *testing.T, path string, extraEnv map[string]string, bin string, args ...string) *exec.Cmd {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = file
	cmd.Stderr = file
	cmd.Env = childEnv(extraEnv)
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		t.Fatalf("%s: %v", bin, err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		_ = file.Close()
		if t.Failed() {
			if b, err := os.ReadFile(path); err == nil {
				t.Logf("%s (tail):\n%s", path, tail(b, 8000))
			}
		}
	})
	return cmd
}

// run runs a command with the extra environment and fails on a non-zero exit.
func run(t *testing.T, env map[string]string, bin string, args ...string) string {
	t.Helper()
	out, err := runCommand(bin, env, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", bin, strings.Join(args, " "), err, out)
	}
	return out
}

func runNoFail(bin string, env map[string]string, args ...string) string {
	out, _ := runCommand(bin, env, args...)
	return out
}

func runCommand(bin string, env map[string]string, args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = childEnv(env)
	cmd.Stderr = nil
	out, err := cmd.Output()
	return string(out), err
}

// childEnv removes inherited TOSHI_* settings and appends the given ones.
func childEnv(extra map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "TOSHI_") {
			continue
		}
		env = append(env, entry)
	}
	for name, value := range extra {
		env = append(env, name+"="+value)
	}
	return env
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[len(b)-n:])
}
