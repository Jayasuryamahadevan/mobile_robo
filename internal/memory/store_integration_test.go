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
func TestIntegrationTemporalCorrectionAndDeletion(t *testing.T){
 dsn:=os.Getenv("MEMORY_TEST_DATABASE_URL")
 if dsn==""{t.Skip("set MEMORY_TEST_DATABASE_URL pointing to an initialized disposable PostgreSQL database")}
 c:=config.Config{DBURL:dsn}
 ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel()
 store,err:=NewStore(ctx,c);if err!=nil{t.Fatal(err)};defer store.Close()
 p:=auth.Claims{Tenant:"memory-test-"+time.Now().Format("150405.000000000"),User:"alice",Agent:"phone_robot"}
 defer store.DeleteAll(context.Background(),p)
 id,err:=store.AddEvent(ctx,p,EventInput{SessionID:"s1",Source:"user",Content:"Hello memory"},"same-key")
 if err!=nil{t.Fatal(err)}
 id2,err:=store.AddEvent(ctx,p,EventInput{SessionID:"s1",Source:"user",Content:"Hello memory"},"same-key")
 if err!=nil||id2!=id{t.Fatalf("idempotency failed %q %q %v",id,id2,err)}
 if _,err:=store.AddEvent(ctx,p,EventInput{SessionID:"s1",Content:"Different"},"same-key");err!=ErrConflict{t.Fatalf("expected conflict; got %v",err)}
 _,err=store.PutFact(ctx,p,FactInput{Subject:"self",Predicate:"favorite_drink",Value:json.RawMessage(`"tea"`)})
 if err!=nil{t.Fatal(err)}
 knownBefore:=time.Now().UTC()
 time.Sleep(10*time.Millisecond)
 _,err=store.PutFact(ctx,p,FactInput{Subject:"self",Predicate:"favorite_drink",Value:json.RawMessage(`"coffee"`)})
 if err!=nil{t.Fatal(err)}
 now:=time.Now().UTC()
 current,err:=store.Context(ctx,p,"favorite_drink","",now,now)
 if err!=nil{t.Fatal(err)}
 if len(current.Items)==0||current.Items[0].Type!="semantic"{t.Fatalf("missing current semantic fact %+v",current.Items)}
 earlier,err:=store.Context(ctx,p,"favorite_drink","",knownBefore,knownBefore)
 if err!=nil{t.Fatal(err)}
 if len(earlier.Items)==0||earlier.Items[0].Type!="semantic"{t.Fatalf("missing recorded-time fact %+v",earlier.Items)}
 pastToday,err:=store.Context(ctx,p,"favorite_drink","",knownBefore,now)
 if err!=nil{t.Fatal(err)}
 if len(pastToday.Items)==0||pastToday.Items[0].Type!="semantic" {t.Fatalf("current knowledge lost past validity %+v",pastToday.Items)}
  history,err:=store.History(ctx,p,"self","favorite_drink")
 if err!=nil||len(history)<3{t.Fatalf("history %v %#v",err,history)}
 if _,err:=store.DeleteAll(ctx,p);err!=nil{t.Fatal(err)}
 after,err:=store.Context(ctx,p,"favorite_drink","",now,now)
 if err!=nil{t.Fatal(err)}
 if len(after.Items)!=0{t.Fatalf("deleted facts resurfaced: %+v",after.Items)}
}
