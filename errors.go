package interstate

import "errors"

var (
	ErrVersionMismatch     = errors.New("version mismatch")
	ErrLeaderClosed        = errors.New("server closed")
	ErrLeaderAlreadyExists = errors.New("leader already exists")
)
