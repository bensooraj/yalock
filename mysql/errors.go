package mysql

import "fmt"

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
