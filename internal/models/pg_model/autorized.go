package pg_model

import "time"

type Authorized struct {
	ID        int64
	Token     string
	Client    string
	CreatedAt *time.Time
}
