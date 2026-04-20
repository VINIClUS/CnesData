// Package fbdriver wrapper fino sobre database/sql para Firebird.
// Driver concreto é importado na main (pure-Go vs CGO) por build tags.
package fbdriver

import "fmt"

// ConnConfig parâmetros de conexão FB.
type ConnConfig struct {
	Host     string
	Port     int
	Path     string
	User     string
	Password string
	Charset  string
}

// BuildDSN constrói DSN compatível com nakagami/firebirdsql.
// Default charset = WIN1252 (obrigatório para CNES legado).
func BuildDSN(c ConnConfig) string {
	if c.Charset == "" {
		c.Charset = "WIN1252"
	}
	return fmt.Sprintf("%s:%s@%s:%d/%s?charset=%s",
		c.User, c.Password, c.Host, c.Port, c.Path, c.Charset)
}
