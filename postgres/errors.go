package postgres

import (
	"errors"
	"fmt"
)

type LockError struct {
	Err         error
	Message     string
	Method      string
	SessionName string
	Driver      string
}

func (e *LockError) Error() string {
	return fmt.Sprintf("%s::%s::%s::%s::%s", e.Driver, e.Method, e.SessionName, e.Message, e.Err.Error())
}

func (e *LockError) Unwrap() error {
	return e.Err
}

var (
	ErrorLockTimeout           = errors.New("timeout")
	ErrorLockAcquisitionFailed = errors.New("failed to acquire lock")
	ErrorLockDoesNotExist      = errors.New("lock does not exist")
	ErrorLockNotOwned          = errors.New("lock not owned")
	ErrorLockUnknown           = errors.New("unknown error")
)
