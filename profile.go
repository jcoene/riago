package riago

import (
	"fmt"
	"time"
)

// Profile represents the instrumentation artifacts from a single operation.
type Profile struct {
	Name     string
	Object   string
	Error    error
	Retries  int32
	Total    time.Duration
	ConnWait time.Duration
	ConnLock time.Duration
	Request  time.Duration
	Response time.Duration
	start    time.Time
}

func (p *Profile) String() string {
	return fmt.Sprintf("op=%s obj=%s success=%v retries=%d total=%v conn_wait=%v conn_lock=%v request=%v response=%v", p.Name, p.Object, p.Error == nil, p.Retries, p.Total, p.ConnWait, p.ConnLock, p.Request, p.Response)
}

// Create a new Profile instance with a given name and object.
func NewProfile(name string, object string) *Profile {
	return &Profile{
		Name:   name,
		Object: object,
		start:  time.Now(),
	}
}
