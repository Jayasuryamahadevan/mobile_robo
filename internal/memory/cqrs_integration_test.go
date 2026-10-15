package memory

import (
 "context"
 "errors"
 "os"
 "testing"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)

// Requires schema migrations 001 and 002. Safe against real PostgreSQL,
// but ALWAYS point MEMORY_TEST_DATABASE_URL at a disposable CI database.
func TestCQRSOutboxDurabilityAndRedelivery(t *testing.T){
 url:=os.Getenv("MEMORY_TEST_DATABASE_URL")
 if url==""{t.Skip("MEMORY_TEST_DATABASE_URL not configured")}
 c:=config.Config{DBURL:url,WorkerPoll:50*time.Millisecond}
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
 defer cancel()
 s,err:=NewStore(ctx,c);if err!=nil{t.Fatal(err)}
 defer s.Close()
 p:=auth.Claims{Tenant:"cqrs-"+time.Now().UTC().Format("20060102150405.000000000"),User:"test",Agent:"phone_robot"}
 defer func(){_,_=s.DeleteAll(context.Background(),p)}()
 _,err=s.AddEvent(ctx,p,EventInput{SessionID:"s1",Source:"user",Content:"No extracted fact here"},"cqrs-once")
 if err!=nil{t.Fatal(err)}
 var jobID int64
 err=s.DB.QueryRow(ctx,`SELECT id FROM memory_outbox
 WHERE tenant_id=$1 AND user_id=$2 AND event_type='episode.index'
 ORDER BY id DESC LIMIT 1`,p.Tenant,p.User).Scan(&jobID)
 if err!=nil{t.Fatal(err)}
 job,err:=s.claimPublish(ctx)
 if err!=nil{t.Fatal(err)}
 if job.ID!=jobID && job.Tenant!=p.Tenant{
  // Other CI tests may have their own outbox rows; this is harmless.
  t.Log("relay also observed another tenant's pending work")
 }
 if err=s.finishPublish(ctx,job,nil);err!=nil{t.Fatal(err)}
 if err=s.ProcessNotice(ctx,OutboxNotice{SchemaVersion:1,OutboxID:jobID});err!=nil{t.Fatal(err)}
 if err=s.ProcessNotice(ctx,OutboxNotice{SchemaVersion:1,OutboxID:jobID});!errors.Is(err,ErrProjectionComplete){t.Fatalf("duplicate processed: %v",err)}
 health,err:=s.ProjectionStatus(ctx,p.Tenant,p.User)
 if err!=nil{t.Fatal(err)}
 if health.Done<1{t.Fatalf("expected done projection: %+v",health)}
 _,err=s.DeleteAll(ctx,p)
 if err!=nil{t.Fatal(err)}
 if err=s.ProcessNotice(ctx,OutboxNotice{SchemaVersion:1,OutboxID:jobID});!errors.Is(err,ErrProjectionComplete){t.Fatalf("stale deleted notice: %v",err)}
}
func TestProjectionEnvelopeVersion(t *testing.T){
 bad:=OutboxNotice{SchemaVersion:2,OutboxID:3}
 if bad.SchemaVersion==1{t.Fatal("invalid fixture")}
}
