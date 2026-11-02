package webhook_test

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"go.toshicloud.io/toshi/webhook"
)

// testSecret returns a secret in its "whsec_" form, built at run time from a
// seed so no literal of that form sits in the tree.
func testSecret(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return webhook.FormatSecret(sum[:])
}

// vector is a POST Toshi's signer produced: the secret's seed, the three
// headers and the body.
type vector struct {
	name, id, timestamp, body, signature string
}

// vectors were signed by Toshi's own signer with the secret of the seed
// "sdk-test-webhook-secret"; "rotation" carries a second signature under
// the seed "sdk-test-webhook-secret-previous".
var vectors = []vector{
	{"json", "msg_2KWPBgLlAfxdpx2AI54pPJ85f4W", "1700000000", `{"type":"job.ended","data":{"id":"job-1","exit_code":0}}`, "v1,2fZ/4I1cxjdjNpEOxtEharf346Elw65NT+H+/hU8bAk="},
	{"empty body", "evt-0001", "1730000000", "", "v1,O1Or6sa9E9odKVi0myGaOUyx3MzMcMbcdo7WEzR+hGI="},
	{"utf-8", "evt_ünï", "1762000000", "héllo wörld ✓\n", "v1,HeBcS1zAQC1xlER7W9UxaZZv9qaZaxUqlAbMOlkl+48="},
	{"rotation", "evt-rotate", "1762090000", `{"type":"service.started"}`, "v1,i7YYduR72D+M3Uh/A+eXrdlI/3gHlpx4JdIIBtkR2lQ= v1,qFKwIw7BbybKLLMvb3GzyOHsAlWXqciKSIKQ2YcgRu0="},
}

func (v vector) header() http.Header {
	h := http.Header{}
	h.Set(webhook.HeaderID, v.id)
	h.Set(webhook.HeaderTimestamp, v.timestamp)
	h.Set(webhook.HeaderSignature, v.signature)
	return h
}

func (v vector) at(offset time.Duration) time.Time {
	sec, err := strconv.ParseInt(v.timestamp, 10, 64)
	if err != nil {
		panic(err)
	}
	return time.Unix(sec, 0).Add(offset)
}

func TestHeaderNames(t *testing.T) {
	for got, want := range map[string]string{
		webhook.HeaderID:        "webhook-id",
		webhook.HeaderTimestamp: "webhook-timestamp",
		webhook.HeaderSignature: "webhook-signature",
	} {
		if got != want {
			t.Errorf("header %q, want %q", got, want)
		}
	}
	if webhook.Tolerance != 5*time.Minute {
		t.Errorf("Tolerance is %v, want 5m", webhook.Tolerance)
	}
}

func TestVerifyAcceptsToshiSignatures(t *testing.T) {
	secret := testSecret("sdk-test-webhook-secret")
	for _, v := range vectors {
		for _, offset := range []time.Duration{0, webhook.Tolerance, -webhook.Tolerance} {
			if err := webhook.Verify(v.header(), []byte(v.body), secret, v.at(offset)); err != nil {
				t.Errorf("%s at %v: %v", v.name, offset, err)
			}
		}
	}
	rotation := vectors[3]
	if err := webhook.Verify(rotation.header(), []byte(rotation.body), testSecret("sdk-test-webhook-secret-previous"), rotation.at(0)); err != nil {
		t.Errorf("rotation under the previous secret: %v", err)
	}
	h := vectors[0].header()
	h.Set(webhook.HeaderSignature, "v2,c2lnbmVk "+vectors[0].signature)
	if err := webhook.Verify(h, []byte(vectors[0].body), secret, vectors[0].at(0)); err != nil {
		t.Errorf("a v1 entry after another version: %v", err)
	}
}

func TestVerifyRefuses(t *testing.T) {
	secret := testSecret("sdk-test-webhook-secret")
	v := vectors[0]
	without := func(name string) http.Header {
		h := v.header()
		h.Del(name)
		return h
	}
	with := func(name, value string) http.Header {
		h := v.header()
		h.Set(name, value)
		return h
	}
	tests := []struct {
		name   string
		header http.Header
		body   string
		secret string
		now    time.Time
		want   error
	}{
		{"no id", without(webhook.HeaderID), v.body, secret, v.at(0), webhook.ErrMissingHeader},
		{"no timestamp", without(webhook.HeaderTimestamp), v.body, secret, v.at(0), webhook.ErrMissingHeader},
		{"no signature", without(webhook.HeaderSignature), v.body, secret, v.at(0), webhook.ErrMissingHeader},
		{"an id with a full stop", with(webhook.HeaderID, "evt.1"), v.body, secret, v.at(0), webhook.ErrMissingHeader},
		{"a timestamp not in seconds", with(webhook.HeaderTimestamp, "1700000000.5"), v.body, secret, v.at(0), webhook.ErrMissingHeader},
		{"a second past the tolerance", v.header(), v.body, secret, v.at(webhook.Tolerance + time.Second), webhook.ErrOutsideTolerance},
		{"a second before the tolerance", v.header(), v.body, secret, v.at(-webhook.Tolerance - time.Second), webhook.ErrOutsideTolerance},
		{"a changed body", v.header(), v.body + " ", secret, v.at(0), webhook.ErrBadSignature},
		{"another secret", v.header(), v.body, testSecret("another"), v.at(0), webhook.ErrBadSignature},
		{"another id", with(webhook.HeaderID, "evt-2"), v.body, secret, v.at(0), webhook.ErrBadSignature},
		{"only another version", with(webhook.HeaderSignature, "v2"+v.signature[2:]), v.body, secret, v.at(0), webhook.ErrBadSignature},
		{"a signature that is not base64", with(webhook.HeaderSignature, "v1,!!!"), v.body, secret, v.at(0), webhook.ErrBadSignature},
		{"a secret without whsec_", v.header(), v.body, secret[len("whsec_"):], v.at(0), webhook.ErrSecret},
		{"a secret of 16 bytes", v.header(), v.body, webhook.FormatSecret(make([]byte, 16)), v.at(0), webhook.ErrSecret},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := webhook.Verify(tt.header, []byte(tt.body), tt.secret, tt.now)
			if !errors.Is(err, tt.want) {
				t.Errorf("Verify = %v, want %v", err, tt.want)
			}
		})
	}
}
