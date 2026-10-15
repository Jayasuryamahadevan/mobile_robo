package memory

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "log/slog"
 "regexp"
 "strings"
 "time"

 "github.com/jackc/pgx/v5"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
)

type Job struct {ID int64; Kind,Tenant,User,Aggregate string; Attempt int}
func (s *Store) claim(ctx context.Context)(Job,error){
 var j Job
 err:=s.DB.QueryRow(ctx,`WITH picked AS (
  SELECT id FROM memory_outbox WHERE
   (status='pending' AND available_at<=now()) OR (status='running' AND lease_until<now())
  ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
 )
 UPDATE memory_outbox o SET status='running',attempt=o.attempt+1,lease_until=now()+interval '60 seconds'
 FROM picked WHERE o.id=picked.id
 RETURNING o.id,o.event_type,o.tenant_id,o.user_id,coalesce(o.aggregate_id::text,''),o.attempt`).Scan(&j.ID,&j.Kind,&j.Tenant,&j.User,&j.Aggregate,&j.Attempt)
 return j,err
}
func (s *Store) finish(ctx context.Context,j Job,workErr error)error{
 if workErr==nil{
  _,e:=s.DB.Exec(ctx,`UPDATE memory_outbox SET status='done',lease_until=NULL,last_error=NULL WHERE id=$1`,j.ID);return e
 }
 delay:=1<<min(j.Attempt,8)
 state:="pending";if j.Attempt>=12{state="dead"}
 msg:=workErr.Error();if len(msg)>512{msg=msg[:512]}
 _,e:=s.DB.Exec(ctx,`UPDATE memory_outbox SET status=$2,available_at=now()+($3::int * interval '1 second'),lease_until=NULL,last_error=$4 WHERE id=$1`,j.ID,state,delay,msg)
 return e
}
func (s *Store) RunWorker(ctx context.Context)error{
 ticker:=time.NewTicker(s.Config.WorkerPoll);defer ticker.Stop()
 for {
  if ctx.Err()!=nil{return nil}
  pollCtx,cancel:=context.WithTimeout(ctx,3*time.Second)
  job,err:=s.claim(pollCtx);cancel()
  if errors.Is(err,pgx.ErrNoRows){
   select {case <-ctx.Done():return nil;case <-ticker.C:continue}
  }
  if err!=nil{slog.Error("outbox claim", "error",err);select{case <-ctx.Done():return nil;case <-ticker.C:continue}}
  processCtx,cancel:=context.WithTimeout(ctx,40*time.Second)
  workErr:=s.withProjectionLock(processCtx,job,func()error{return s.handleJob(processCtx,job)})
  cancel()
  doneCtx,doneCancel:=context.WithTimeout(context.Background(),3*time.Second)
  if e:=s.finish(doneCtx,job,workErr);e!=nil{slog.Error("outbox ack","error",e,"job",job.ID)}
  doneCancel()
  if workErr!=nil{slog.Warn("job retried","type",job.Kind,"job",job.ID,"attempt",job.Attempt,"error",workErr)}
 }
}
var rememberRE=regexp.MustCompile(`(?i)^remember:\s*([a-z][a-z0-9_.-]{0,63})\s*=\s*(.{1,1000})$`)
func (s *Store) handleJob(ctx context.Context,j Job)error{
 switch j.Kind {
 case "episode.index":
  if !s.Config.VectorEnabled {return nil}
  var content string
  err:=s.DB.QueryRow(ctx,`SELECT ep.content FROM memory_episodes ep
  JOIN memory_events e ON e.id=ep.event_id
  WHERE ep.event_id=$1 AND ep.tenant_id=$2 AND ep.user_id=$3 AND ep.deleted_at IS NULL AND e.deleted_at IS NULL`,j.Aggregate,j.Tenant,j.User).Scan(&content)
  if errors.Is(err,pgx.ErrNoRows){return nil};if err!=nil{return err}
  vec,err:=s.Vector.Embed(ctx,content);if err!=nil{return err}
  _,err=s.DB.Exec(ctx,`UPDATE memory_episodes SET embedding=$2::vector WHERE event_id=$1 AND tenant_id=$3 AND user_id=$4 AND deleted_at IS NULL`,j.Aggregate,VectorLiteral(vec),j.Tenant,j.User)
  if err!=nil{return err}
  // Qdrant is a derived index. Reads must verify all IDs against PostgreSQL.
  if err=s.Vector.EnsureQdrant(ctx);err!=nil{return err}
  return s.Vector.UpsertEpisode(ctx,j.Aggregate,j.Tenant,j.User,vec)
 case "fact.project":
  if !s.Config.GraphEnabled{return nil}
  var subject,predicate,value string;var valid time.Time
  err:=s.DB.QueryRow(ctx,`SELECT subject,predicate,value::text,valid_from FROM memory_facts
  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND recorded_until IS NULL`,j.Aggregate,j.Tenant,j.User).Scan(&subject,&predicate,&value,&valid)
  if errors.Is(err,pgx.ErrNoRows){return nil};if err!=nil{return err}
  if !strings.HasPrefix(predicate,"relationship."){return nil}
  var target string
  if err=json.Unmarshal([]byte(value),&target);err!=nil{return fmt.Errorf("relationship value must be a string: %w",err)}
  if len(target)>128{return errors.New("relationship target too long")}
  return s.Vector.ProjectFact(ctx,j.Aggregate,j.Tenant,j.User,subject,predicate,target,valid)
 case "event.extract":
  var content,source string
  err:=s.DB.QueryRow(ctx,`SELECT content,source FROM memory_events WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND deleted_at IS NULL`,j.Aggregate,j.Tenant,j.User).Scan(&content,&source)
  if errors.Is(err,pgx.ErrNoRows){return nil};if err!=nil{return err}
  if source!="user"{return nil}
  matches:=rememberRE.FindStringSubmatch(strings.TrimSpace(content))
  if len(matches)!=3{return nil}
  val,_:=json.Marshal(matches[2])
  _,err=s.PutFact(ctx,auth.Claims{Tenant:j.Tenant,User:j.User,Agent:"phone_robot"},FactInput{Subject:"self",Predicate:strings.ToLower(matches[1]),Value:val,SessionID:"memory-extraction"})
  return err
 case "user.purge":
  if s.Config.VectorEnabled{
   if err:=s.Vector.PurgeQdrant(ctx,j.Tenant,j.User);err!=nil{return err}
  }
  if s.Config.GraphEnabled{
   if err:=s.Vector.PurgeNeo4j(ctx,j.Tenant,j.User);err!=nil{return err}
  }
  _,err:=s.DB.Exec(ctx,`UPDATE memory_deletions SET projections_complete_at=now() WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,j.Aggregate,j.Tenant,j.User)
  if err!=nil{return err}
  // A user may have written new memories between the authoritative delete
  // and eventual projection purge. Re-enqueue their still-live projections
  // while the shared user advisory lock is held by the projector.
  _,err=s.DB.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
    SELECT 'episode.index',e.tenant_id,e.user_id,e.id
    FROM memory_events e JOIN memory_episodes ep ON ep.event_id=e.id
    WHERE e.tenant_id=$1 AND e.user_id=$2
      AND e.deleted_at IS NULL AND ep.deleted_at IS NULL`,j.Tenant,j.User)
  if err!=nil{return err}
  _,err=s.DB.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
    SELECT 'fact.project',f.tenant_id,f.user_id,f.id
    FROM memory_facts f WHERE f.tenant_id=$1 AND f.user_id=$2 AND f.recorded_until IS NULL`,j.Tenant,j.User)
  return err
 default:return fmt.Errorf("unknown outbox type %q",j.Kind)
 }
}
