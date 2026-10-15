package httpapi

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "log/slog"
 "io"
 "net/http"
 "strings"
 "time"

 "github.com/jackc/pgx/v5"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)

type API struct{ Store *memory.Store }
func jsonResponse(w http.ResponseWriter,status int,v any){
 w.Header().Set("Content-Type","application/json");w.Header().Set("Cache-Control","no-store");w.WriteHeader(status);_ =json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter,err error){
 status:=http.StatusBadRequest
 if errors.Is(err,pgx.ErrNoRows){status=http.StatusNotFound
 }else if errors.Is(err,memory.ErrConflict){status=http.StatusConflict
 }else if errors.Is(err,context.DeadlineExceeded){status=http.StatusGatewayTimeout
 }else if !isInputError(err){status=http.StatusInternalServerError;slog.Error("memory error","error",err);err=errors.New("internal memory error")}
 jsonResponse(w,status,map[string]any{"error":err.Error()})
}
func isInputError(err error)bool {
 msg:=err.Error()
 for _,prefix:=range []string{"invalid ","session_id","value must","future-effective","idempotency key is too long","invalid procedure","query is required","unknown field","json:","unexpected EOF"} {
  if strings.HasPrefix(msg,prefix){return true}
 }
 return false
}
func parseBody(w http.ResponseWriter,r *http.Request,v any)error{
 r.Body=http.MaxBytesReader(w,r.Body,32<<10)
 dec:=json.NewDecoder(r.Body);dec.DisallowUnknownFields()
 if err:=dec.Decode(v);err!=nil{return err}
 var remainder any
 if err:=dec.Decode(&remainder);!errors.Is(err,io.EOF){return errors.New("invalid JSON: unexpected trailing content")}
 return nil
}
func require(w http.ResponseWriter,p auth.Claims,scope string)bool{
 if p.Has(scope){return true}
 jsonResponse(w,http.StatusForbidden,map[string]string{"error":"missing scope: "+scope});return false
}
func parseTime(input string,def time.Time)(time.Time,error){
 if input==""{return def,nil}
 t,e:=time.Parse(time.RFC3339Nano,input)
 if e!=nil{return t,errors.New("invalid timestamp (expected RFC3339)")}
 return t.UTC(),nil
}
func (a *API) Handler()http.Handler{
 priv:=http.NewServeMux()
 priv.HandleFunc("POST /v1/events",a.events)
 priv.HandleFunc("PUT /v1/facts",a.facts)
 priv.HandleFunc("GET /v1/context",a.context)
 priv.HandleFunc("GET /v1/facts/history",a.history)
 priv.HandleFunc("POST /v1/procedures",a.procedures)
 priv.HandleFunc("POST /v1/procedures/{id}/approve",a.approve)
 priv.HandleFunc("DELETE /v1/memories",a.delete)
 mux:=http.NewServeMux()
 mux.HandleFunc("GET /healthz",func(w http.ResponseWriter,r *http.Request){jsonResponse(w,200,map[string]string{"status":"alive"})})
 mux.HandleFunc("GET /readyz",func(w http.ResponseWriter,r *http.Request){
  c,cancel:=context.WithTimeout(r.Context(),time.Second);defer cancel()
  if err:=a.Store.Ready(c);err!=nil{jsonResponse(w,503,map[string]string{"status":"not_ready"});return}
  jsonResponse(w,200,map[string]string{"status":"ready"})
 })
 mux.Handle("/",auth.Middleware([]byte(a.Store.Config.JWTSecret),a.Store.Config.Issuer,a.Store.Config.Audience,priv))
 return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  w.Header().Set("X-Content-Type-Options","nosniff")
  w.Header().Set("Referrer-Policy","no-referrer")
  mux.ServeHTTP(w,r)
 })
}
func (a *API) events(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:write"){return}
 var body memory.EventInput
 if err:=parseBody(w,r,&body);err!=nil{fail(w,fmt.Errorf("invalid JSON: %w",err));return}
 if body.Source==""{body.Source="user"}
 if body.Source!="user" && !p.Has("memory:source:any"){jsonResponse(w,403,map[string]string{"error":"source requires memory:source:any"});return}
 key:=r.Header.Get("Idempotency-Key")
 if len(key)>128{fail(w,errors.New("idempotency key is too long"));return}
 c,cancel:=context.WithTimeout(r.Context(),2*time.Second);defer cancel()
 id,err:=a.Store.AddEvent(c,p,body,key)
 if err!=nil{fail(w,err);return}
 jsonResponse(w,201,map[string]any{"event_id":id,"durable":true})
}
func (a *API) facts(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:write"){return}
 var body memory.FactInput
 if err:=parseBody(w,r,&body);err!=nil{fail(w,fmt.Errorf("invalid JSON: %w",err));return}
 c,cancel:=context.WithTimeout(r.Context(),2*time.Second);defer cancel()
 id,err:=a.Store.PutFact(c,p,body)
 if err!=nil{fail(w,err);return}
 jsonResponse(w,201,map[string]string{"fact_id":id,"state":"current"})
}
func (a *API) context(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:read"){return}
 q:=r.URL.Query();query:=memory.SearchTerms(q.Get("query"))
 if query==""{fail(w,errors.New("query is required"));return}
 now:=time.Now().UTC()
 asOf,err:=parseTime(q.Get("as_of"),now);if err!=nil{fail(w,err);return}
 knownAt,err:=parseTime(q.Get("known_at"),now);if err!=nil{fail(w,err);return}
 c,cancel:=context.WithTimeout(r.Context(),95*time.Millisecond);defer cancel()
 result,err:=a.Store.Context(c,p,query,q.Get("session_id"),asOf,knownAt)
 if err!=nil{fail(w,err);return}
 result=a.Store.Enrich(c,p,result)
 jsonResponse(w,200,result)
}
func (a *API) history(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:read"){return}
 q:=r.URL.Query();if q.Get("subject")==""||q.Get("predicate")==""{fail(w,errors.New("invalid subject/predicate"));return}
 c,cancel:=context.WithTimeout(r.Context(),2*time.Second);defer cancel()
 history,err:=a.Store.History(c,p,q.Get("subject"),q.Get("predicate"))
 if err!=nil{fail(w,err);return}
 jsonResponse(w,200,map[string]any{"history":history})
}
func (a *API) procedures(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:write"){return}
 var body memory.ProcedureInput
 if err:=parseBody(w,r,&body);err!=nil{fail(w,fmt.Errorf("invalid JSON: %w",err));return}
 c,cancel:=context.WithTimeout(r.Context(),2*time.Second);defer cancel()
 id,err:=a.Store.ProposeProcedure(c,p,body)
 if err!=nil{fail(w,err);return}
 jsonResponse(w,201,map[string]string{"procedure_id":id,"state":"proposed"})
}
func (a *API) approve(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"procedure:approve"){return}
 c,cancel:=context.WithTimeout(r.Context(),2*time.Second);defer cancel()
 if err:=a.Store.ApproveProcedure(c,p,r.PathValue("id"));err!=nil{fail(w,err);return}
 jsonResponse(w,200,map[string]string{"state":"approved"})
}
func (a *API) delete(w http.ResponseWriter,r *http.Request){
 p:=auth.Principal(r.Context());if !require(w,p,"memory:delete"){return}
 c,cancel:=context.WithTimeout(r.Context(),5*time.Second);defer cancel()
 id,err:=a.Store.DeleteAll(c,p)
 if err!=nil{fail(w,err);return}
 jsonResponse(w,202,map[string]any{"deletion_id":id,"authoritative_deleted":true,"projections":"pending"})
}
