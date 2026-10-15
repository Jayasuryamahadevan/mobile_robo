package memory

import (
 "bytes"
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "net/http"
 "strconv"
 "strings"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)

type Vectors struct { Config config.Config; HTTP *http.Client }
func NewVectors(c config.Config)*Vectors {return &Vectors{Config:c,HTTP:&http.Client{Timeout:5*time.Second}}}
func (v *Vectors) request(ctx context.Context,method,url string,body any,basicAuth bool) ([]byte,error) {
 var r io.Reader
 if body!=nil {b,e:=json.Marshal(body);if e!=nil{return nil,e};r=bytes.NewReader(b)}
 req,e:=http.NewRequestWithContext(ctx,method,url,r);if e!=nil{return nil,e}
 req.Header.Set("Content-Type","application/json")
 if basicAuth {req.SetBasicAuth(v.Config.Neo4jUser,v.Config.Neo4jPassword)}
 res,e:=v.HTTP.Do(req);if e!=nil{return nil,e};defer res.Body.Close()
 data,e:=io.ReadAll(io.LimitReader(res.Body,2<<20));if e!=nil{return nil,e}
 if res.StatusCode<200||res.StatusCode>=300{return nil,fmt.Errorf("projection backend HTTP %d: %s",res.StatusCode,string(data))}
 return data,nil
}
func (v *Vectors) Embed(ctx context.Context,input string)([]float32,error){
 b,e:=v.request(ctx,"POST",strings.TrimRight(v.Config.OllamaURL,"/")+"/api/embed",map[string]any{"model":v.Config.EmbeddingModel,"input":input},false)
 if e!=nil{return nil,e}
 var out struct{Embeddings [][]float32 `json:"embeddings"`}
 if e=json.Unmarshal(b,&out);e!=nil{return nil,e}
 if len(out.Embeddings)==0||len(out.Embeddings[0])!=768{return nil,errors.New("embedding dimension mismatch: expected 768")}
 return out.Embeddings[0],nil
}
func (v *Vectors) EnsureQdrant(ctx context.Context) error {
 _,e:=v.request(ctx,"PUT",strings.TrimRight(v.Config.QdrantURL,"/")+"/collections/episodes",
  map[string]any{"vectors":map[string]any{"size":768,"distance":"Cosine"}},false)
 return e
}
func (v *Vectors) UpsertEpisode(ctx context.Context,id,tenant,user string,vector []float32)error{
 _,e:=v.request(ctx,"PUT",strings.TrimRight(v.Config.QdrantURL,"/")+"/collections/episodes/points?wait=true",
  map[string]any{"points":[]any{map[string]any{"id":id,"vector":vector,"payload":map[string]any{"tenant_id":tenant,"user_id":user}}}},false)
 return e
}
type VectorHit struct {ID string;Score float64}
func (v *Vectors) SearchEpisodes(ctx context.Context,tenant,user string,vec []float32)([]VectorHit,error){
 filters:=[]any{
  map[string]any{"key":"tenant_id","match":map[string]any{"value":tenant}},
  map[string]any{"key":"user_id","match":map[string]any{"value":user}},
 }
 b,e:=v.request(ctx,"POST",strings.TrimRight(v.Config.QdrantURL,"/")+"/collections/episodes/points/query",
  map[string]any{"query":vec,"filter":map[string]any{"must":filters},"limit":10,"with_payload":false},false)
 if e!=nil{return nil,e}
 var out struct{Result struct{Points []struct{ID any `json:"id"`;Score float64 `json:"score"`} `json:"points"`}}`json:"result"`}
 if e=json.Unmarshal(b,&out);e!=nil{return nil,e}
 hits:=make([]VectorHit,0,len(out.Result.Points))
 for _,h:=range out.Result.Points{if id,ok:=h.ID.(string);ok{hits=append(hits,VectorHit{ID:id,Score:h.Score})}}
 return hits,nil
}
func (v *Vectors) PurgeQdrant(ctx context.Context,tenant,user string)error{
 filters:=[]any{
  map[string]any{"key":"tenant_id","match":map[string]any{"value":tenant}},
  map[string]any{"key":"user_id","match":map[string]any{"value":user}},
 }
 _,e:=v.request(ctx,"POST",strings.TrimRight(v.Config.QdrantURL,"/")+"/collections/episodes/points/delete?wait=true",
  map[string]any{"filter":map[string]any{"must":filters}},false)
 return e
}
func (v *Vectors) cypher(ctx context.Context,statement string,params map[string]any)([]byte,error){
 b,e:=v.request(ctx,"POST",strings.TrimRight(v.Config.Neo4jURL,"/")+"/db/neo4j/tx/commit",
  map[string]any{"statements":[]any{map[string]any{"statement":statement,"parameters":params}}},true)
 if e!=nil{return nil,e}
 var out struct{Errors []struct{Code string `json:"code"`;Message string `json:"message"`} `json:"errors"`}
 if e=json.Unmarshal(b,&out);e!=nil{return nil,e}
 if len(out.Errors)>0{return nil,fmt.Errorf("Neo4j: %s: %s",out.Errors[0].Code,out.Errors[0].Message)}
 return b,nil
}
func (v *Vectors) ProjectFact(ctx context.Context,id,tenant,user,subject,predicate,target string,from time.Time)error {
 query:=`MERGE (a:MemoryEntity {tenant:$tenant, user:$user, key:$subject})
 MERGE (b:MemoryEntity {tenant:$tenant, user:$user, key:$target})
 WITH a,b
 OPTIONAL MATCH (a)-[old:MEMORY_RELATION]->()
 WHERE old.predicate=$predicate AND old.fact_id<>$fact
 DELETE old
 WITH a,b
 MERGE (a)-[r:MEMORY_RELATION {fact_id:$fact}]->(b)
 SET r.predicate=$predicate,r.valid_from=$valid_from`
 _,err:=v.cypher(ctx,query,map[string]any{"tenant":tenant,"user":user,"subject":subject,"target":target,"predicate":predicate,"fact":id,"valid_from":from.UTC().Format(time.RFC3339Nano)})
 return err
}
func (v *Vectors) PurgeNeo4j(ctx context.Context,tenant,user string)error{
 _,e:=v.cypher(ctx,`MATCH (n:MemoryEntity {tenant:$tenant,user:$user}) DETACH DELETE n`,
  map[string]any{"tenant":tenant,"user":user})
 return e
}
func VectorLiteral(vec []float32)string{
 b:=strings.Builder{};b.WriteByte('[')
 for i,f:=range vec {if i>0{b.WriteByte(',')};b.WriteString(strconv.FormatFloat(float64(f),'g',-1,32))}
 b.WriteByte(']');return b.String()
}
