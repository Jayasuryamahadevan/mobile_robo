package memory

import (
 "context"
 "errors"
 "fmt"
 "log/slog"
 "time"

 "github.com/jackc/pgx/v5"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
)

var ErrProjectionBusy = errors.New("projection is already leased or retry is scheduled")
var ErrProjectionComplete = errors.New("projection has already been completed or removed")
var ErrProjectionDead = errors.New("projection exhausted retry budget")

// OutboxNotice is the ONLY body carried by the broker. It never contains PII.
// The receiver reads the authoritative work item from PostgreSQL.
type OutboxNotice struct {
 SchemaVersion int   `json:"schema_version"`
 OutboxID      int64 `json:"outbox_id"`
}

// claimPublish leases publication, without changing the processing state.
// A relay may publish the same outbox ID repeatedly after a crash.
func (s *Store) claimPublish(ctx context.Context) (Job, error) {
 var j Job
 err := s.DB.QueryRow(ctx, `
 WITH chosen AS (
   SELECT id FROM memory_outbox
   WHERE status IN ('pending','running')
     AND (published_at IS NULL OR published_at < now()-interval '10 minutes')
     AND publish_retry_at <= now()
     AND (publish_lease_until IS NULL OR publish_lease_until < now())
   ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
 )
 UPDATE memory_outbox o SET
   publish_lease_until = now() + interval '25 seconds',
   publish_attempt = o.publish_attempt + 1
 FROM chosen WHERE o.id = chosen.id
 RETURNING o.id,o.event_type,o.tenant_id,o.user_id,coalesce(o.aggregate_id::text,''),o.publish_attempt
 `).Scan(&j.ID,&j.Kind,&j.Tenant,&j.User,&j.Aggregate,&j.Attempt)
 return j,err
}

func (s *Store) finishPublish(ctx context.Context,j Job,err error) error {
 if err == nil {
  _, e := s.DB.Exec(ctx,`UPDATE memory_outbox SET published_at=now(), publish_lease_until=NULL
    WHERE id=$1`,j.ID)
  return e
 }
 wait := 1 << min(j.Attempt,8)
 note := err.Error(); if len(note)>512 {note=note[:512]}
 _,e:=s.DB.Exec(ctx,`UPDATE memory_outbox
 SET publish_lease_until=NULL,publish_retry_at=now()+($2::integer * interval '1 second'),
 last_error=$3 WHERE id=$1 AND status IN ('pending','running')`,j.ID,wait,note)
 return e
}

// RunRelay is the transaction-outbox -> JetStream bridge. A positive broker
// PubAck is required BEFORE the outbox is marked published.
func (s *Store) RunRelay(ctx context.Context,publish func(context.Context,OutboxNotice)error) error {
 poll:=time.NewTicker(s.Config.WorkerPoll)
 defer poll.Stop()
 for ctx.Err()==nil {
  c,cancel:=context.WithTimeout(ctx,3*time.Second)
  j,err:=s.claimPublish(c)
  cancel()
  if errors.Is(err,pgx.ErrNoRows) {
   select {case <-ctx.Done(): return nil;case <-poll.C:continue}
  }
  if err!=nil {
   slog.Error("outbox relay claim","error",err)
   select {case <-ctx.Done():return nil;case <-poll.C:continue}
  }
  pc,pcancel:=context.WithTimeout(ctx,8*time.Second)
  err=publish(pc,OutboxNotice{SchemaVersion:1,OutboxID:j.ID})
  pcancel()
  finishCtx,fcancel:=context.WithTimeout(context.Background(),3*time.Second)
  finishErr:=s.finishPublish(finishCtx,j,err)
  fcancel()
  if finishErr!=nil{slog.Error("relay publish state","outbox_id",j.ID,"error",finishErr)}
  if err!=nil{slog.Warn("JetStream publish retry","outbox_id",j.ID,"error",err)}
 }
 return nil
}

// processClaim uses the same DB state machine as the legacy polling worker.
// Redelivery is safe: completed jobs are not processed a second time.
func (s *Store) processClaim(ctx context.Context,id int64)(Job,error){
 var j Job
 err:=s.DB.QueryRow(ctx,`
 WITH picked AS (
 SELECT id FROM memory_outbox
 WHERE id=$1 AND (
   (status='pending' AND available_at<=now())
   OR (status='running' AND lease_until<now())
 ) FOR UPDATE SKIP LOCKED
 )
 UPDATE memory_outbox o
 SET status='running',attempt=o.attempt+1,lease_until=now()+interval '70 seconds'
 FROM picked WHERE o.id=picked.id
 RETURNING o.id,o.event_type,o.tenant_id,o.user_id,coalesce(o.aggregate_id::text,''),o.attempt`,id).
 Scan(&j.ID,&j.Kind,&j.Tenant,&j.User,&j.Aggregate,&j.Attempt)
 if !errors.Is(err,pgx.ErrNoRows){return j,err}
 var status string
 err=s.DB.QueryRow(ctx,`SELECT status FROM memory_outbox WHERE id=$1`,id).Scan(&status)
 if errors.Is(err,pgx.ErrNoRows){return j,ErrProjectionComplete}
 if err!=nil{return j,err}
 switch status {
 case "done":return j,ErrProjectionComplete
 case "dead":return j,ErrProjectionDead
 default:return j,ErrProjectionBusy
 }
}

// withProjectionLock serializes external writes with DeleteAll's advisory
// transaction lock. This avoids delayed vector writes resurrecting deleted
// data AFTER a projection purge has completed.
func (s *Store) withProjectionLock(ctx context.Context,j Job,fn func()error)error {
 conn,err:=s.DB.Acquire(ctx);if err!=nil{return err}
 defer conn.Release()
 lockKey:=j.Tenant+":"+j.User
 _,err=conn.Exec(ctx,`SELECT pg_advisory_lock(hashtext($1),0)`,lockKey)
 if err!=nil{return err}
 defer func(){
  c,cancel:=context.WithTimeout(context.Background(),4*time.Second)
  if _,e:=conn.Exec(c,`SELECT pg_advisory_unlock(hashtext($1),0)`,lockKey);e!=nil{
   slog.Error("projection advisory unlock failure","error",e,"outbox_id",j.ID)
   // Never return a session holding a PostgreSQL advisory lock to the pool.
   _ = conn.Conn().Close(context.Background())
  }
  cancel()
 }()
 return fn()
}

// ProcessNotice returns one of the completion errors, so the broker handler
// can Ack, Term, or Nak with delay. Success means the DB acknowledgment is
// committed; transient errors must be retried.
func (s *Store) ProcessNotice(ctx context.Context,n OutboxNotice)error {
 if n.SchemaVersion!=1 || n.OutboxID<=0{return fmt.Errorf("unsupported or invalid outbox message")}
 j,err:=s.processClaim(ctx,n.OutboxID)
 if err!=nil{return err}
 err=s.withProjectionLock(ctx,j,func()error{return s.handleJob(ctx,j)})
 doneCtx,cancel:=context.WithTimeout(context.Background(),3*time.Second)
 defer cancel()
 if ackErr:=s.finish(doneCtx,j,err);ackErr!=nil{return fmt.Errorf("persist projection status: %w",ackErr)}
 if err!=nil {
  if j.Attempt>=12{return ErrProjectionDead}
  return fmt.Errorf("projection failed: %w",err)
 }
 return nil
}

// ReplayUser rebuilds projection jobs from still-authoritative source rows.
// It does not change, recreate, or implicitly trust any deleted memory.
func (s *Store) ReplayUser(ctx context.Context,tenant,user string)(int64,error){
 tx,err:=s.scoped(ctx,auth.Claims{Tenant:tenant,User:user});if err!=nil{return 0,err}
 defer tx.Rollback(ctx)
 _,err=tx.Exec(ctx,`SELECT pg_advisory_xact_lock(hashtext($1),0)`,tenant+":"+user)
 if err!=nil{return 0,err}
 var count int64
 for _,statement:=range []string{
  `INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
     SELECT 'episode.index',e.tenant_id,e.user_id,e.id
     FROM memory_events e JOIN memory_episodes ep ON ep.event_id=e.id
     WHERE e.tenant_id=$1 AND e.user_id=$2 AND e.deleted_at IS NULL
     AND ep.deleted_at IS NULL`,
  `INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
     SELECT 'fact.project',f.tenant_id,f.user_id,f.id
     FROM memory_facts f WHERE f.tenant_id=$1 AND f.user_id=$2
     AND f.recorded_until IS NULL
     AND f.valid_from <= now() AND (f.valid_until IS NULL OR f.valid_until > now())`,
 }{
  tag,e:=tx.Exec(ctx,statement,tenant,user);if e!=nil{return 0,e}
  count+=tag.RowsAffected()
 }
 return count,tx.Commit(ctx)
}
