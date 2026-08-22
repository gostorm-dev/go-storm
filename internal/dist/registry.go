package dist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// The agent registry: every live agent holds a short-TTL key that its
// heartbeats keep renewing. An expired entry means a dead agent — the
// coordinator sees this through Agents(), no explicit failure message
// needed.

// AgentInfo describes a registered agent.
type AgentInfo struct {
	ID        string    `json:"id"`
	Hostname  string    `json:"hostname"`
	StartedAt time.Time `json:"started_at"`
}

// RegisterAgent records the agent in Redis with a short TTL. The agent must
// keep calling Heartbeat or its entry expires, which is how the coordinator
// detects a dead agent.
func (r *Redis) RegisterAgent(ctx context.Context, id string) error {
	info := AgentInfo{ID: id, StartedAt: time.Now()}
	if host, err := os.Hostname(); err == nil {
		info.Hostname = host
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, agentKey(id), string(b), r.HeartbeatTTL).Err()
}

// Heartbeat renews the agent's TTL. Run it in a goroutine while the agent
// is alive. Note: Redis EXPIRE has whole-second resolution — keep
// HeartbeatTTL in whole seconds or the renewal is shorter than it appears.
func (r *Redis) Heartbeat(ctx context.Context, id string) error {
	return r.Client.Expire(ctx, agentKey(id), r.HeartbeatTTL).Err()
}

// UnregisterAgent removes the agent entry on clean shutdown.
func (r *Redis) UnregisterAgent(ctx context.Context, id string) error {
	return r.Client.Del(ctx, agentKey(id)).Err()
}

// Agents lists the currently alive agents (those whose TTL hasn't expired).
func (r *Redis) Agents(ctx context.Context) ([]AgentInfo, error) {
	var agents []AgentInfo

	iter := r.Client.Scan(ctx, 0, agentsPattern, 100).Iterator()
	for iter.Next(ctx) {
		val, err := r.Client.Get(ctx, iter.Val()).Result()
		if errors.Is(err, redis.Nil) {
			continue // expired between SCAN and GET
		}
		if err != nil {
			return nil, err
		}
		var info AgentInfo
		if err := json.Unmarshal([]byte(val), &info); err != nil {
			continue
		}
		agents = append(agents, info)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return agents, nil
}

// waitForAgents blocks until at least n agents have registered, up to
// AgentsWait so run-dist can't hang forever.
func (r *Redis) waitForAgents(ctx context.Context, n int) error {
	deadline := time.Now().Add(r.AgentsWait)

	for {
		agents, err := r.Agents(ctx)
		if err != nil {
			return err
		}
		if len(agents) >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d agents (only %d registered)", n, len(agents))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
