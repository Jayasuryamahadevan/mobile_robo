package memory

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
	"github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)

// Graph projections describe presently valid relationships, not historical
// intervals that remain in the authoritative bitemporal fact store.
func TestReplaySkipsHistoricalRelationshipFacts(t *testing.T) {
	url := os.Getenv("MEMORY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("MEMORY_TEST_DATABASE_URL not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := NewStore(ctx, config.Config{DBURL: url})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := auth.Claims{
		Tenant: "relationship-replay-" + time.Now().UTC().Format("20060102150405.000000000"),
		User: "test",
		Agent: "phone_robot",
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := s.DeleteAll(c, p); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	firstAt := time.Now().UTC().Add(-48 * time.Hour)
	secondAt := time.Now().UTC().Add(-24 * time.Hour)
	first, err := s.PutFact(ctx, p, FactInput{
		Subject: "self", Predicate: "relationship.friend", Value: json.RawMessage(`"alex"`),
		ValidFrom: &firstAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.PutFact(ctx, p, FactInput{
		Subject: "self", Predicate: "relationship.friend", Value: json.RawMessage(`"sam"`),
		ValidFrom: &secondAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first == current {
		t.Fatal("expected a versioned replacement fact")
	}

	var historical string
	err = s.DB.QueryRow(ctx, `SELECT id::text FROM memory_facts
		WHERE tenant_id=$1 AND user_id=$2 AND predicate='relationship.friend'
		AND recorded_until IS NULL AND valid_until <= now()`,
		p.Tenant, p.User).Scan(&historical)
	if err != nil {
		t.Fatal(err)
	}

	// Any accidental graph write will fail against this unreachable backend.
	s.Config.GraphEnabled = true
	s.Vector = NewVectors(config.Config{Neo4jURL: "http://127.0.0.1:1"})
	if err := s.handleJob(ctx, Job{
		Kind: "fact.project", Aggregate: historical, Tenant: p.Tenant, User: p.User,
	}); err != nil {
		t.Fatalf("historical relationship must not be projected: %v", err)
	}

	var before int64
	if err := s.DB.QueryRow(ctx, `SELECT COALESCE(MAX(id),0) FROM memory_outbox
		WHERE tenant_id=$1 AND user_id=$2`, p.Tenant, p.User).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReplayUser(ctx, p.Tenant, p.User); err != nil {
		t.Fatal(err)
	}

	var historicalJobs, currentJobs int
	err = s.DB.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE aggregate_id=$4::uuid),
		COUNT(*) FILTER (WHERE aggregate_id=$5::uuid)
		FROM memory_outbox WHERE tenant_id=$1 AND user_id=$2 AND id>$3
		AND event_type='fact.project'`,
		p.Tenant, p.User, before, historical, current).Scan(&historicalJobs, &currentJobs)
	if err != nil {
		t.Fatal(err)
	}
	if historicalJobs != 0 || currentJobs != 1 {
		t.Fatalf("unexpected replay: historical=%d current=%d", historicalJobs, currentJobs)
	}
}
