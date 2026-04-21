package platform

// SingleInstanceLock identifica lock exclusivo por nome.
type SingleInstanceLock interface {
	Release() error
}
