package memory

import (
 "context"
 "os"
 "testing"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)

// DB user must NOT own the tables. The test ensures an unscoped read is
// denied by RLS even if the application accidentally omits WHERE clauses.
func TestRLSNonOwnerCannotReadUnscopedMemory(t *testing.T){
 url:=os.Getenv("MEMORY_RLS_TEST_DATABASE_URL")
 if url==""{t.Skip("MEMORY_RLS_TEST_DATABASE_URL not configured")}
 ctx,cancel:=context.WithTimeout(context.Background(),8*time.Second);defer cancel()
 s,err:=NewStore(ctx,config.Config{DBURL:url})
 if err!=nil{t.Fatal(err)}
 defer s.Close()
 p:=auth.Claims{Tenant:"tenant-"+time.Now().UTC().Format("150405.000000000"),User:"scoped",Agent:"phone_robot"}
 if _,err:=s.AddEvent(ctx,p,EventInput{SessionID:"s",Content:"RLS protected user data"},"");err!=nil{t.Fatal(err)}
 defer s.DeleteAll(context.Background(),p)
 var count int64
 if err:=s.DB.QueryRow(ctx,`SELECT count(*) FROM memory_events`).Scan(&count);err!=nil{t.Fatal(err)}
 if count!=0{t.Fatalf("RLS bypass: unscoped API role read %d events",count)}
 var outboxCount int64
 if err:=s.DB.QueryRow(ctx,`SELECT count(*) FROM memory_outbox`).Scan(&outboxCount);err!=nil{t.Fatal(err)}
 if outboxCount!=0{t.Fatalf("RLS bypass: unscoped API role read %d outbox rows",outboxCount)}
 history,err:=s.Context(ctx,p,"RLS protected user data","s",time.Now(),time.Now())
 if err!=nil{t.Fatal(err)}
 if len(history.Items)==0{t.Fatal("scoped API cannot retrieve own memory")}
}
