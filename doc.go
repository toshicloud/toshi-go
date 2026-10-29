// Package toshi is a client for the Toshi Workspace API, toshi.workspace.v1.
//
// New returns a Client with one method per service of the API. Workload is
// written by this module: a job's calls under Workload().Jobs(), a sandbox's
// under Workload().Sandboxes(), a service's under Workload().Services(), and
// the calls that span the three on Workload() itself. The other methods are
// the generated Connect clients of their service, from the module
// go.toshicloud.io/api. The client sends its token as a bearer
// token and fills an empty project and environment from WithScope. It
// retries reads, and the writes that take a request_key, when the server is
// unavailable, the connection drops or a RESOURCE_EXHAUSTED refusal names a
// wait, waiting at least that long; every other error goes back to the
// caller.
//
// The methods add what a single call does not: Run creates a job and follows
// it as a JobRun, the Wait methods ask again until the answer is final, the
// Watch methods resume after a dropped connection from the last cursor or
// byte offset received, and Execute runs a command as a Process with
// standard input, output readers and an exit error.
package toshi
