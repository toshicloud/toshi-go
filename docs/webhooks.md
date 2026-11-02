# Webhooks

Toshi signs every webhook POST with the symmetric scheme of the
[Standard Webhooks](https://www.standardwebhooks.com/) specification.
`go.toshicloud.io/toshi/webhook` verifies it.

## What a signed POST carries

| Header | Constant | Holds |
|---|---|---|
| `webhook-id` | `webhook.HeaderID` | the event's id, the same on every try, so a receiver can drop a repeat |
| `webhook-timestamp` | `webhook.HeaderTimestamp` | the try's time in unix seconds; a retry carries its own |
| `webhook-signature` | `webhook.HeaderSignature` | a space-separated list of signatures, each `v1,` and a base64 HMAC-SHA256 |

A signature is the HMAC-SHA256, under the endpoint's secret, of the id, the
timestamp and the body joined by full stops: `<id>.<timestamp>.<body>`.
While an endpoint's secret rotates, the header holds one signature under
the new secret and one under the previous one, so a receiver that still
holds either keeps verifying.

The secret reaches the receiver as `whsec_` and the base64 of its bytes,
24 to 64 of them. `webhook.FormatSecret` writes bytes in that form, for a
test that signs its own POSTs.

## Verify

`webhook.Verify(header, body, secret, now)` takes the request's headers,
the body exactly as received, the secret in its `whsec_` form and the
current time. Read the body before decoding it: a body that was decoded
and encoded again is not the body Toshi signed.

<!-- webhook.ExampleVerify -->
```go
import (
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"go.toshicloud.io/toshi/webhook"
)

// The endpoint's secret, "whsec_" and base64, as Toshi gave it.
secret := os.Getenv("TOSHI_WEBHOOK_SECRET")
http.HandleFunc("/toshi/webhook", func(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	if err := webhook.Verify(r.Header, body, secret, time.Now()); err != nil {
		http.Error(w, "unverified webhook", http.StatusUnauthorized)
		return
	}
	// body is the event, exactly as Toshi sent it.
	w.WriteHeader(http.StatusNoContent)
})
log.Fatal(http.ListenAndServe(":8080", nil))
```

`Verify` returns nil when one `v1` signature in the list matches, compared
in constant time; it skips entries of another version. Otherwise it returns
one of these, for `errors.Is`:

| Error | When |
|---|---|
| `webhook.ErrSecret` | the secret is not `whsec_` and the base64 of 24 to 64 bytes |
| `webhook.ErrMissingHeader` | a header is missing, the id is empty or holds a full stop or a space, or the timestamp is not a whole number of seconds |
| `webhook.ErrOutsideTolerance` | the timestamp is more than `webhook.Tolerance`, five minutes, from `now` either way |
| `webhook.ErrBadSignature` | no `v1` signature matches |

No error's text holds the secret or a signature.
