// Package webhook verifies the webhooks Toshi sends.
//
// Toshi signs every webhook POST with the symmetric scheme of the Standard
// Webhooks specification (https://www.standardwebhooks.com/): three headers
// carry the event's id, the time of the try and a list of signatures, each
// "v1," and the base64 HMAC-SHA256 of "<id>.<timestamp>.<body>" under the
// endpoint's secret. The secret is given to the receiver as "whsec_" and the
// base64 of its bytes.
//
// A receiver reads the raw body and calls Verify before it trusts the event:
//
//	body, err := io.ReadAll(r.Body)
//	if err != nil { ... }
//	if err := webhook.Verify(r.Header, body, secret, time.Now()); err != nil {
//		http.Error(w, "unverified webhook", http.StatusUnauthorized)
//		return
//	}
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The headers a signed POST carries.
const (
	// HeaderID is the event's id: the same on every try, so a receiver can
	// drop a repeat.
	HeaderID = "webhook-id"
	// HeaderTimestamp is the try's time in unix seconds; a retry carries its
	// own.
	HeaderTimestamp = "webhook-timestamp"
	// HeaderSignature is a space-separated list of signatures, each "v1,"
	// and the base64 HMAC-SHA256. While an endpoint's secret rotates it
	// holds one signature under each secret.
	HeaderSignature = "webhook-signature"
)

// Tolerance is how far, either way, Verify accepts a timestamp from now.
const Tolerance = 5 * time.Minute

// secretPrefix marks a symmetric secret.
const secretPrefix = "whsec_"

// signatureVersion marks a symmetric signature in the signature header.
const signatureVersion = "v1"

// The ways Verify refuses a POST. Their text holds neither the secret nor a
// signature.
var (
	// ErrMissingHeader means one of the three headers is missing or not in
	// its form: an id that is empty or holds a full stop or a space, or a
	// timestamp that is not a whole number of seconds.
	ErrMissingHeader = errors.New("webhook: a webhook header is missing or malformed")
	// ErrOutsideTolerance means the timestamp is more than Tolerance from now.
	ErrOutsideTolerance = errors.New("webhook: timestamp outside the tolerance")
	// ErrBadSignature means no "v1" signature matches the id, timestamp and
	// body under the secret.
	ErrBadSignature = errors.New("webhook: no signature matches")
	// ErrSecret means the secret is not "whsec_" and the base64 of 24 to 64
	// bytes.
	ErrSecret = errors.New("webhook: the secret is not whsec_ and base64 of 24 to 64 bytes")
)

// FormatSecret returns a secret's bytes in the form Toshi gives a receiver:
// "whsec_" and their base64.
func FormatSecret(raw []byte) string {
	return secretPrefix + base64.StdEncoding.EncodeToString(raw)
}

// Verify checks a webhook POST: it refuses a timestamp more than Tolerance
// from now, then accepts the POST when one "v1" entry of the signature
// header equals the HMAC-SHA256 of the id, the timestamp and body under
// secret, compared in constant time. Entries of another version are skipped.
// secret is the endpoint's secret as Toshi gives it, "whsec_" and base64;
// body is the request body exactly as received.
//
// The error is nil, ErrSecret, ErrMissingHeader, ErrOutsideTolerance or
// ErrBadSignature.
func Verify(h http.Header, body []byte, secret string, now time.Time) error {
	key, err := parseSecret(secret)
	if err != nil {
		return err
	}
	id, ts, list := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if id == "" || strings.ContainsAny(id, ". \t\r\n") || ts == "" || list == "" {
		return ErrMissingHeader
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrMissingHeader
	}
	if d := now.Sub(time.Unix(sec, 0)); d > Tolerance || d < -Tolerance {
		return ErrOutsideTolerance
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte(id + "." + ts + "."))
	m.Write(body)
	want := m.Sum(nil)
	for _, entry := range strings.Fields(list) {
		version, sig, ok := strings.Cut(entry, ",")
		if !ok || version != signatureVersion {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(sig)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return ErrBadSignature
}

// parseSecret reverses FormatSecret.
func parseSecret(s string) ([]byte, error) {
	rest, ok := strings.CutPrefix(s, secretPrefix)
	if !ok {
		return nil, ErrSecret
	}
	b, err := base64.StdEncoding.DecodeString(rest)
	if err != nil || len(b) < 24 || len(b) > 64 {
		return nil, ErrSecret
	}
	return b, nil
}
