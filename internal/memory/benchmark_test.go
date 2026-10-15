package memory
import (
 "context"
 "os"
 "testing"
 "time"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
)
func BenchmarkContextPostgres(b *testing.B){
 dsn:=os.Getenv("MEMORY_TEST_DATABASE_URL")
 if dsn==""{b.Skip("set MEMORY_TEST_DATABASE_URL")}
 s,err:=NewStore(context.Background(),config.Config{DBURL:dsn});if err!=nil{b.Fatal(err)};defer s.Close()
 p:=auth.Claims{Tenant:"bench",User:"bench",Agent:"phone_robot"}
 now:=time.Now()
 b.ResetTimer()
 for i:=0;i<b.N;i++{
  if _,err:=s.Context(context.Background(),p,"preferred_language","",now,now);err!=nil{b.Fatal(err)}
 }
}
