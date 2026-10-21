// Package toshi is a client for the Toshi Workspace API, toshi.workspace.v1.
//
// New returns a Client whose fields are the generated Connect clients of the
// API's five services, from the module github.com/toshicloud/toshi-api. The
// client sends its token as a bearer token and fills an empty project and
// environment from WithScope. It retries reads, and the writes that take a
// request_key, when the server is unavailable or the connection drops;
// every other error, RESOURCE_EXHAUSTED included, goes back to the caller.
//
// The methods on Client cover what a single call does not: WaitWorkload and
// WaitOperation ask again until the answer is final, WatchWorkloads,
// WatchEvents and WatchOutput resume after a dropped connection from the
// last cursor or byte offset received, and Execute runs a command as a
// Process with standard input, output readers and an exit error.
package toshi
