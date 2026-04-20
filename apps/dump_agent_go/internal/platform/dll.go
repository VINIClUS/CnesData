package platform

import "errors"

// ErrFBClientNotFound indica falha em localizar fbclient nativo.
var ErrFBClientNotFound = errors.New("fbclient_not_found")

// FBClientPath resolve caminho para fbclient runtime.
// Se driver Go for pure-Go, retorna "" sem erro (driver não precisa).
func FBClientPath() (string, error) {
	return fbClientPathImpl()
}
