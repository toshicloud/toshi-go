package webhook_test

import (
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"go.toshicloud.io/toshi/webhook"
)

func ExampleVerify() {
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
}
