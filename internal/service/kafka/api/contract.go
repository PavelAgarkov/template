package api

import "net/http"

type Proxy interface {
	ReceiveShkOnPlaceBytesBufferStreamV1(w http.ResponseWriter, request *http.Request)
	ReceiveTareMoveBytesBufferStreamV1(w http.ResponseWriter, request *http.Request)
}
