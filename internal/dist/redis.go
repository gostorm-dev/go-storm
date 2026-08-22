package dist

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis wraps a go-redis client with the queue operations both sides use.
type Redis struct {
	Client *redis.Client

	// Timing knobs. Defaults match long-standing production values;
	// they are fields rather than constants so tests can shorten
	// timeout and expiry paths from tens of seconds to milliseconds.
	PopTimeout   time.Duration // BLPop wait before reporting an empty queue
	IdleTimeout  time.Duration // agent exits after this long with no jobs
	HeartbeatTTL time.Duration // agent registry entry expires without a heartbeat
	AgentsWait   time.Duration // coordinator gives up waiting for agents
}

// NewRedis creates a client for the given address, e.g. "localhost:6379".
func NewRedis(addr string) *Redis {
	return &Redis{
		Client:       redis.NewClient(&redis.Options{Addr: addr}),
		PopTimeout:   2 * time.Second,
		IdleTimeout:  5 * time.Second,
		HeartbeatTTL: 5 * time.Second,
		AgentsWait:   30 * time.Second,
	}
}

// Ping verifies the connection.
func (r *Redis) Ping(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}
