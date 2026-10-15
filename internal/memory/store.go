package memory

import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "fmt"
 "sort"
 "strings"
 "time"

 "github.com/jackc/pgx/v5"
 "github.com/jackc/pgx/v5/pgxpool"
 "github.com/redis/go-redis/v9"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)

var ErrConflict=errors.New("idempotency key was reused with different content")
type Store struct {
 DB *pgxpool.Pool
 Cache *redis.Client
 Vector *Vectors
 Config config.Config
}
func NewStore(ctx context.Context,c config.Config)(*Store,error) {
 cfg,err:=pgxpool.ParseConfig(c.DBURL)
 if err!=nil{return nil,err}
 cfg.MaxConns=24
 cfg.MinConns=2
 cfg.MaxConnLifetime=30*time.Minute
 p,err:=pgxpool.NewWithConfig(ctx,cfg)
 if err!=nil{return nil,err}
 if err=p.Ping(ctx);err!=nil {p.Close();return nil,err}
 s:=&Store{DB:p,Config:c,Vector:NewVectors(c)}
 if c.RedisAddr!="" {s.Cache=redis.NewClient(&redis.Options{Addr:c.RedisAddr,PoolSize:16,ReadTimeout:30*time.Millisecond,WriteTimeout:30*time.Millisecond})}
 return s,nil
}
func (s *Store) Close(){if s.Cache!=nil {_=s.Cache.Close()};s.DB.Close()}
func (s *Store) Ready(ctx context.Context)error{return s.DB.Ping(ctx)}
func (s *Store) scoped(ctx context.Context,p auth.Claims)(pgx.Tx,error) {
 tx,err:=s.DB.Begin(ctx)
 if err!=nil {return nil,err}
 _,err=tx.Exec(ctx,"SELECT set_config('app.tenant_id',$1,true),set_config('app.user_id',$2,true)",p.Tenant,p.User)
 if err!=nil {_=tx.Rollback(ctx);return nil,err}
 return tx,nil
}
func digest(content string)string{h:=sha256.Sum256([]byte(content));return hex.EncodeToString(h[:])}
func (s *Store) addEventTx(ctx context.Context,tx pgx.Tx,p auth.Claims,e EventInput,key string)(string,bool,error){
 if err:=ValidateEvent(&e);err!=nil{return "",false,err}
 when:=time.Now().UTC()
 if e.OccurredAt!=nil {when=e.OccurredAt.UTC()}
 var id string
 err:=tx.QueryRow(ctx,`INSERT INTO memory_events(tenant_id,user_id,agent_id,robot_id,session_id,source,content,content_sha256,idempotency_key,occurred_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10)
 ON CONFLICT (tenant_id,user_id,idempotency_key) DO NOTHING RETURNING id::text`,
 p.Tenant,p.User,e.AgentID,e.RobotID,e.SessionID,e.Source,e.Content,digest(e.Content),key,when).Scan(&id)
 if errors.Is(err,pgx.ErrNoRows) {
  var existingHash string
  err=tx.QueryRow(ctx,`SELECT id::text,content_sha256 FROM memory_events WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3 AND deleted_at IS NULL`,p.Tenant,p.User,key).Scan(&id,&existingHash)
  if err!=nil{return "",false,err}
  if existingHash!=digest(e.Content){return "",false,ErrConflict}
  return id,false,nil
 }
 if err!=nil{return "",false,err}
 _,err=tx.Exec(ctx,`INSERT INTO memory_episodes(event_id,tenant_id,user_id,occurred_at,content) VALUES ($1,$2,$3,$4,$5)`,id,p.Tenant,p.User,when,e.Content)
 if err!=nil{return "",false,err}
 _,err=tx.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
 VALUES ('episode.index',$1,$2,$3)`,p.Tenant,p.User,id)
 if err!=nil{return "",false,err}
 if e.Source=="user"{
  _,err=tx.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)
  VALUES ('event.extract',$1,$2,$3)`,p.Tenant,p.User,id)
 }
 return id,true,err
}
func (s *Store) AddEvent(ctx context.Context,p auth.Claims,e EventInput,key string)(string,error){
 // Timestamp before the transaction, not in an async cache goroutine:
 // a cache insert racing a subsequent deletion must remain older than
 // the persistent deletion barrier.
 acceptedAt:=time.Now().UTC()
 if err:=ValidateEvent(&e);err!=nil{return "",err}
 tx,err:=s.scoped(ctx,p);if err!=nil{return "",err};defer tx.Rollback(ctx)
 if _,err=tx.Exec(ctx,`SELECT pg_advisory_xact_lock(hashtext($1),0)`,p.Tenant+":"+p.User);err!=nil{return "",err}
 id,isNew,err:=s.addEventTx(ctx,tx,p,e,key);if err!=nil{return "",err}
 if err=tx.Commit(ctx);err!=nil{return "",err}
 if isNew && s.Cache!=nil {
  go func(){
   c,cancel:=context.WithTimeout(context.Background(),60*time.Millisecond);defer cancel()
   item,_:=json.Marshal(struct{ID,Content string; At time.Time}{id,e.Content,acceptedAt})
   k:=fmt.Sprintf("wm:%s:%s:%s",p.Tenant,p.User,e.SessionID)
   pipe:=s.Cache.Pipeline()
   pipe.LPush(c,k,item);pipe.LTrim(c,k,0,9);pipe.Expire(c,k,15*time.Minute)
   pipe.SAdd(c,fmt.Sprintf("wm-index:%s:%s",p.Tenant,p.User),k)
   pipe.Expire(c,fmt.Sprintf("wm-index:%s:%s",p.Tenant,p.User),24*time.Hour)
   _,_=pipe.Exec(c)
  }()
 }
 return id,nil
}
func (s *Store) PutFact(ctx context.Context,p auth.Claims,f FactInput)(string,error){
 now:=time.Now().UTC()
 if err:=ValidateFact(&f,now);err!=nil{return "",err}
 when:=now;if f.ValidFrom!=nil{when=f.ValidFrom.UTC()}
 if f.SessionID==""{f.SessionID="explicit-facts"}
 tx,err:=s.scoped(ctx,p);if err!=nil{return "",err};defer tx.Rollback(ctx)
 if _,err=tx.Exec(ctx,`SELECT pg_advisory_xact_lock(hashtext($1),0)`,p.Tenant+":"+p.User);err!=nil{return "",err}
 _,err=tx.Exec(ctx,`SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))`,p.Tenant+":"+p.User+":"+f.Subject,f.Predicate)
 if err!=nil{return "",err}
 var version int
 err=tx.QueryRow(ctx,`SELECT COALESCE(MAX(version),0) FROM memory_facts
  WHERE tenant_id=$1 AND user_id=$2 AND subject=$3 AND predicate=$4`,
  p.Tenant,p.User,f.Subject,f.Predicate).Scan(&version)
 if err!=nil{return "",err}
 type prior struct{
  value string
  source string
  asserted string
  confidence float64
  version int
  from time.Time
  until *time.Time
 }
 retained:=[]prior{}
 rows,err:=tx.Query(ctx,`SELECT value::text,source_event_id::text,asserted_by,confidence,version,valid_from,valid_until
  FROM memory_facts WHERE tenant_id=$1 AND user_id=$2 AND subject=$3 AND predicate=$4
  AND recorded_until IS NULL ORDER BY valid_from FOR UPDATE`,
  p.Tenant,p.User,f.Subject,f.Predicate)
 if err!=nil{return "",err}
 for rows.Next(){
  var old prior
  if err=rows.Scan(&old.value,&old.source,&old.asserted,&old.confidence,&old.version,&old.from,&old.until);err!=nil{rows.Close();return "",err}
  if old.from.Before(when){retained=append(retained,old)}
 }
 err=rows.Err();rows.Close();if err!=nil{return "",err}
 evidence:=fmt.Sprintf("%s.%s = %s",f.Subject,f.Predicate,string(f.Value))
 event:=EventInput{SessionID:f.SessionID,AgentID:p.Agent,Source:"user",Content:evidence}
 eventID,_,err:=s.addEventTx(ctx,tx,p,event,"")
 if err!=nil{return "",err}
 _,err=tx.Exec(ctx,`UPDATE memory_facts SET recorded_until=$5 WHERE tenant_id=$1 AND user_id=$2
  AND subject=$3 AND predicate=$4 AND recorded_until IS NULL`,
  p.Tenant,p.User,f.Subject,f.Predicate,now)
 if err!=nil{return "",err}
 // Re-materialize older valid-time segments under current knowledge.
 // Without this, historical questions asked today would lose prior facts.
 for _,old:=range retained {
  until:=old.until
  if until==nil||until.After(when){t:=when;until=&t}
  if !until.After(old.from){continue}
  _,err=tx.Exec(ctx,`INSERT INTO memory_facts
   (tenant_id,user_id,subject,predicate,value,confidence,source_event_id,version,valid_from,valid_until,recorded_from,asserted_by)
   VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12)`,
   p.Tenant,p.User,f.Subject,f.Predicate,old.value,old.confidence,old.source,old.version,old.from,until,now,old.asserted)
  if err!=nil{return "",err}
 }
 var factID string
 err=tx.QueryRow(ctx,`INSERT INTO memory_facts(tenant_id,user_id,subject,predicate,value,confidence,source_event_id,version,valid_from,recorded_from,asserted_by)
 VALUES ($1,$2,$3,$4,$5::jsonb,1.0,$6,$7,$8,$9,'user_explicit') RETURNING id::text`,
 p.Tenant,p.User,f.Subject,f.Predicate,string(f.Value),eventID,version+1,when,now).Scan(&factID)
 if err!=nil{return "",err}
 _,err=tx.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id) VALUES ('fact.project',$1,$2,$3)`,p.Tenant,p.User,factID)
 if err!=nil{return "",err}
 if err=tx.Commit(ctx);err!=nil{return "",err}
 return factID,nil
}
func (s *Store) History(ctx context.Context,p auth.Claims,subject,predicate string)([]map[string]any,error){
 tx,err:=s.scoped(ctx,p);if err!=nil{return nil,err};defer tx.Rollback(ctx)
 rows,err:=tx.Query(ctx,`SELECT id::text,value,version,valid_from,valid_until,recorded_from,recorded_until,source_event_id::text
 FROM memory_facts WHERE tenant_id=$1 AND user_id=$2 AND subject=$3 AND predicate=$4 ORDER BY recorded_from DESC LIMIT 100`,
 p.Tenant,p.User,subject,predicate)
 if err!=nil{return nil,err};defer rows.Close()
 out:=[]map[string]any{}
 for rows.Next(){
  var id,event string;var val json.RawMessage;var version int
  var vf,rf time.Time;var vu,ru *time.Time
  if err=rows.Scan(&id,&val,&version,&vf,&vu,&rf,&ru,&event);err!=nil{return nil,err}
  out=append(out,map[string]any{"id":id,"value":val,"version":version,"valid_from":vf,"valid_until":vu,"recorded_from":rf,"recorded_until":ru,"source_event_id":event})
 }
 if err=rows.Err();err!=nil{return nil,err}
 return out,tx.Commit(ctx)
}
func (s *Store) Context(ctx context.Context,p auth.Claims,query,session string,asOf,knownAt time.Time)(Context,error){
 query=SearchTerms(query)
 term:=""
 for _,word:=range strings.Fields(strings.ToLower(query)) {
  if len(word)>3 && word!="what" && word!="where" && word!="when" && word!="about" && word!="your" && word!="have" {term=word;break}
 }
 result:=Context{Query:query,AsOf:asOf,KnownAt:knownAt,Items:[]MemoryItem{},EvidenceRequired:true}
 tx,err:=s.scoped(ctx,p);if err!=nil{return result,err};defer tx.Rollback(ctx)
 // All source IDs and current fact values come from the authoritative database.
 rows,err:=tx.Query(ctx,`SELECT f.id::text,f.subject,f.predicate,f.value::text,f.source_event_id::text,f.confidence,f.valid_from,f.recorded_from
 FROM memory_facts f JOIN memory_events e ON e.id=f.source_event_id
 WHERE f.tenant_id=$1 AND f.user_id=$2 AND f.valid_from <= $3
 AND (f.valid_until IS NULL OR f.valid_until > $3)
 AND f.recorded_from <= $4 AND (f.recorded_until IS NULL OR f.recorded_until > $4)
 AND e.deleted_at IS NULL
 AND ($6='' OR f.predicate ILIKE '%' || $6 || '%' OR f.value::text ILIKE '%' || $6 || '%')
 ORDER BY CASE WHEN f.predicate=$5 THEN 0 ELSE 1 END, f.recorded_from DESC LIMIT 12`,
 p.Tenant,p.User,asOf,knownAt,strings.ToLower(strings.ReplaceAll(query," ","_")),term)
 if err!=nil{return result,err}
 for rows.Next(){
  var id,subject,predicate,value,event string;var confidence float64;var valid,recorded time.Time
  if err=rows.Scan(&id,&subject,&predicate,&value,&event,&confidence,&valid,&recorded);err!=nil{rows.Close();return result,err}
  score:=0.68
  if strings.Contains(strings.ToLower(predicate),strings.ToLower(strings.ReplaceAll(query," ","_"))) {score=0.91}
  result.Items=append(result.Items,MemoryItem{ID:id,Type:"semantic",Content:subject+"."+predicate+" = "+value,SourceEventID:event,Confidence:confidence,Score:score,ValidFrom:&valid,RecordedFrom:&recorded})
 }
 err=rows.Err();rows.Close();if err!=nil{return result,err}
 if query!="" {
  rows,err=tx.Query(ctx,`SELECT ep.event_id::text,ep.content,ep.occurred_at,ts_rank_cd(ep.lexemes,websearch_to_tsquery('simple',$3)) AS rank
 FROM memory_episodes ep JOIN memory_events e ON e.id=ep.event_id
 WHERE ep.tenant_id=$1 AND ep.user_id=$2 AND ep.deleted_at IS NULL AND e.deleted_at IS NULL
 AND ep.occurred_at <= $4 AND e.recorded_at<=$5 AND (ep.expires_at IS NULL OR ep.expires_at>$4)
 AND ep.lexemes @@ websearch_to_tsquery('simple',$3)
 ORDER BY rank DESC,ep.occurred_at DESC LIMIT 12`,p.Tenant,p.User,query,asOf,knownAt)
  if err!=nil{return result,err}
  for rows.Next(){
   var id,content string;var occurred time.Time;var rank float64
   if err=rows.Scan(&id,&content,&occurred,&rank);err!=nil{rows.Close();return result,err}
   score:=0.48+0.3*rank/(1+rank)
   result.Items=append(result.Items,MemoryItem{ID:id,Type:"episodic",Content:content,SourceEventID:id,Confidence:1,Score:score,ValidFrom:&occurred})
  }
  err=rows.Err();rows.Close();if err!=nil{return result,err}
 }
 if term!="" {
  procedures,procErr:=tx.Query(ctx,`SELECT id::text,name,steps::text,required_permissions
   FROM memory_procedures WHERE tenant_id=$1 AND user_id=$2 AND state='approved'
   AND name ILIKE '%' || $3 || '%' ORDER BY approved_at DESC LIMIT 3`,p.Tenant,p.User,term)
  if procErr!=nil{return result,procErr}
  for procedures.Next(){
   var id,name,steps string;var required []string
   if err=procedures.Scan(&id,&name,&steps,&required);err!=nil{procedures.Close();return result,err}
   allowed:=true
   for _,scope:=range required{if !p.Has(scope){allowed=false;break}}
   if allowed {
    result.Items=append(result.Items,MemoryItem{ID:id,Type:"procedural",Content:name+" steps="+steps,SourceEventID:id,Confidence:1,Score:0.78})
   }
  }
  err=procedures.Err();procedures.Close();if err!=nil{return result,err}
 }
 // A deletion barrier also protects against stale Redis entries and late index writes.
 var deletion *time.Time
 if err=tx.QueryRow(ctx,`SELECT MAX(requested_at) FROM memory_deletions WHERE tenant_id=$1 AND user_id=$2`,p.Tenant,p.User).Scan(&deletion);err!=nil{return result,err}
 if err=tx.Commit(ctx);err!=nil{return result,err}
 if session!="" && s.Cache!=nil {
  c,cancel:=context.WithTimeout(ctx,20*time.Millisecond)
  vals,e:=s.Cache.LRange(c,fmt.Sprintf("wm:%s:%s:%s",p.Tenant,p.User,session),0,4).Result()
  cancel()
  if e==nil {
   for _,v:=range vals {
    var ev struct{ID,Content string;At time.Time}
    if json.Unmarshal([]byte(v),&ev)!=nil {continue}
    if deletion!=nil && !ev.At.After(*deletion){continue}
    // Cache entries are advisory. They are not supplied as factual evidence.
    result.Items=append(result.Items,MemoryItem{ID:ev.ID,Type:"working",Content:ev.Content,SourceEventID:ev.ID,Confidence:0.5,Score:0.55})
   }
  }else{result.RetrievalDegraded=true}
 }
 seen:=map[string]bool{};uniq:=make([]MemoryItem,0,len(result.Items))
 sort.SliceStable(result.Items,func(i,j int)bool{return result.Items[i].Score>result.Items[j].Score})
 for _,it:=range result.Items {
  if seen[it.SourceEventID] && it.Type!="semantic"{continue}
  seen[it.SourceEventID]=true;uniq=append(uniq,it)
  if len(uniq)>=12{break}
 }
 result.Items=uniq
 return result,nil
}
func (s *Store) ProposeProcedure(ctx context.Context,p auth.Claims,f ProcedureInput)(string,error){
 if strings.TrimSpace(f.Name)==""||len(f.Name)>128||!json.Valid(f.Steps)||len(f.Steps)>16000{return "",errors.New("invalid procedure")}
 if f.Version==0 {f.Version=1}
 tx,err:=s.scoped(ctx,p);if err!=nil{return "",err};defer tx.Rollback(ctx)
 var id string
 err=tx.QueryRow(ctx,`INSERT INTO memory_procedures(tenant_id,user_id,name,version,steps,required_permissions,state)
 VALUES($1,$2,$3,$4,$5,$6,'proposed') RETURNING id::text`,p.Tenant,p.User,f.Name,f.Version,[]byte(f.Steps),f.RequiredPermissions).Scan(&id)
 if err!=nil{return "",err}
 return id,tx.Commit(ctx)
}
func (s *Store) ApproveProcedure(ctx context.Context,p auth.Claims,id string)error {
 tx,err:=s.scoped(ctx,p);if err!=nil{return err};defer tx.Rollback(ctx)
 tag,err:=tx.Exec(ctx,`UPDATE memory_procedures SET state='approved',approved_at=now() WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND state='proposed'`,id,p.Tenant,p.User)
 if err!=nil{return err}
 if tag.RowsAffected()!=1{return pgx.ErrNoRows}
 return tx.Commit(ctx)
}
func (s *Store) DeleteAll(ctx context.Context,p auth.Claims)(string,error){
 tx,err:=s.scoped(ctx,p);if err!=nil{return "",err};defer tx.Rollback(ctx)
 // Serialize purges against ordinary fact writes by locking this user's advisory key.
 _,err=tx.Exec(ctx,`SELECT pg_advisory_xact_lock(hashtext($1),0)`,p.Tenant+":"+p.User)
 if err!=nil{return "",err}
 var purgeID string
 err=tx.QueryRow(ctx,`INSERT INTO memory_deletions(tenant_id,user_id)VALUES($1,$2) RETURNING id::text`,p.Tenant,p.User).Scan(&purgeID)
 if err!=nil{return "",err}
 for _,table:=range []string{"memory_facts","memory_episodes","memory_procedures","memory_events"}{
  _,err=tx.Exec(ctx,"DELETE FROM "+table+" WHERE tenant_id=$1 AND user_id=$2",p.Tenant,p.User)
  if err!=nil{return "",err}
 }
 _,err=tx.Exec(ctx,`DELETE FROM memory_outbox WHERE tenant_id=$1 AND user_id=$2`,p.Tenant,p.User)
 if err!=nil{return "",err}
 _,err=tx.Exec(ctx,`INSERT INTO memory_outbox(event_type,tenant_id,user_id,aggregate_id)VALUES('user.purge',$1,$2,$3)`,p.Tenant,p.User,purgeID)
 if err!=nil{return "",err}
 if err=tx.Commit(ctx);err!=nil{return "",err}
 if s.Cache!=nil {
  c,cancel:=context.WithTimeout(context.Background(),150*time.Millisecond);defer cancel()
  index:=fmt.Sprintf("wm-index:%s:%s",p.Tenant,p.User)
  keys,_:=s.Cache.SMembers(c,index).Result()
  keys=append(keys,index)
  _=s.Cache.Del(c,keys...).Err()
 }
 return purgeID,nil
}
