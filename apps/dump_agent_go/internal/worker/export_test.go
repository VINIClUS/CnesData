package worker

// Exports for black-box tests in worker_test package.
// File name suffix `_test.go` ensures these symbols are only visible
// during `go test` builds — they are not in the production API.

var SerializeBPA = serializeBPA
var SerializeSIA = serializeSIA
