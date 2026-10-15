package memory

import (
 "context"
 "encoding/json"
 "math"
 "sort"
 "strings"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
)

func (v *Vectors) RelatedFactIDs(ctx context.Context,tenant,user,subject string)([]string,error){
 raw,err:=v.cypher(ctx,`MATCH (a:MemoryEntity {tenant:$tenant,user:$user,key:$subject})-[r:MEMORY_RELATION]->()
 RETURN r.fact_id LIMIT 12`,map[string]any{"tenant":tenant,"user":user,"subject":subject})
 if err!=nil{return nil,err}
 var result struct{Results []struct{Data []struct{Row []string `json:"row"`} `json:"data"`} `json:"results"`}
 if err=json.Unmarshal(raw,&result);err!=nil{return nil,err}
 out:=[]string{}
 for _,res:=range result.Results{for _,entry:=range res.Data{if len(entry.Row)>0{out=append(out,entry.Row[0])}}}
 return out,nil
}
func (s *Store) Enrich(ctx context.Context,p auth.Claims,base Context) Context {
 // Optional derived indexes cannot make retrieval fail. They only nominate IDs.
 if !s.Config.VectorEnabled && !s.Config.GraphEnabled {return base}
 var episodeHits []VectorHit
 var factIDs []string
 if s.Config.VectorEnabled && base.Query!="" {
  c,cancel:=context.WithTimeout(ctx,45*time.Millisecond)
  vector,err:=s.Vector.Embed(c,base.Query)
  if err==nil{episodeHits,err=s.Vector.SearchEpisodes(c,p.Tenant,p.User,vector)}
  cancel()
  if err!=nil{base.RetrievalDegraded=true}
 }
 if s.Config.GraphEnabled && strings.HasPrefix(base.Query,"related:"){
  subject:=strings.TrimSpace(strings.TrimPrefix(base.Query,"related:"))
  if subject!=""{
   c,cancel:=context.WithTimeout(ctx,30*time.Millisecond)
   ids,err:=s.Vector.RelatedFactIDs(c,p.Tenant,p.User,subject)
   cancel()
   if err!=nil{base.RetrievalDegraded=true}else{factIDs=ids}
  }
 }
 if len(episodeHits)==0 && len(factIDs)==0{return base}
 tx,err:=s.scoped(ctx,p)
 if err!=nil{base.RetrievalDegraded=true;return base}
 defer tx.Rollback(ctx)
 for _,h:=range episodeHits {
  var id,content string
  var occurred time.Time
  err=tx.QueryRow(ctx,`SELECT ep.event_id::text,ep.content,ep.occurred_at
   FROM memory_episodes ep JOIN memory_events e ON e.id=ep.event_id
   WHERE ep.event_id::text=$1 AND ep.tenant_id=$2 AND ep.user_id=$3 AND ep.deleted_at IS NULL
   AND e.deleted_at IS NULL AND ep.occurred_at<=$4 AND e.recorded_at<=$5
   AND (ep.expires_at IS NULL OR ep.expires_at>$4)`,
   h.ID,p.Tenant,p.User,base.AsOf,base.KnownAt).Scan(&id,&content,&occurred)
  if err!=nil{continue}
  score:=0.55+0.36*math.Max(0,math.Min(1,h.Score))
  base.Items=append(base.Items,MemoryItem{ID:id,Type:"episodic",Content:content,SourceEventID:id,Confidence:1,Score:score,ValidFrom:&occurred})
 }
 for _,id:=range factIDs{
  var factID,subject,predicate,value,event string
  var confidence float64
  var valid,recorded time.Time
  err=tx.QueryRow(ctx,`SELECT f.id::text,f.subject,f.predicate,f.value::text,f.source_event_id::text,
   f.confidence,f.valid_from,f.recorded_from FROM memory_facts f
   JOIN memory_events e ON e.id=f.source_event_id
   WHERE f.id::text=$1 AND f.tenant_id=$2 AND f.user_id=$3
   AND f.valid_from<=$4 AND (f.valid_until IS NULL OR f.valid_until>$4)
   AND f.recorded_from<=$5 AND (f.recorded_until IS NULL OR f.recorded_until>$5)
   AND e.deleted_at IS NULL`,
   id,p.Tenant,p.User,base.AsOf,base.KnownAt).Scan(&factID,&subject,&predicate,&value,&event,&confidence,&valid,&recorded)
  if err!=nil{continue}
  base.Items=append(base.Items,MemoryItem{ID:factID,Type:"semantic",Content:subject+"."+predicate+" = "+value,SourceEventID:event,Confidence:confidence,Score:0.86,ValidFrom:&valid,RecordedFrom:&recorded})
 }
 if err=tx.Commit(ctx);err!=nil{base.RetrievalDegraded=true;return base}
 seen:=map[string]bool{}
 compact:=make([]MemoryItem,0,len(base.Items))
 sort.SliceStable(base.Items,func(i,j int)bool{return base.Items[i].Score>base.Items[j].Score})
 for _,item:=range base.Items{
  key:=item.Type+":"+item.ID
  if seen[key]{continue}
  seen[key]=true
  compact=append(compact,item)
  if len(compact)==12{break}
 }
 base.Items=compact
 return base
}
