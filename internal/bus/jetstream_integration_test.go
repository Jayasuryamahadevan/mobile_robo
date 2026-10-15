package bus

import (
 "context"
 "encoding/json"
 "os"
 "testing"
 "time"

 "github.com/nats-io/nats.go/jetstream"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)

func TestJetStreamPublishAckAndDurablePull(t *testing.T){
 url:=os.Getenv("NATS_TEST_URL")
 if url==""{t.Skip("NATS_TEST_URL not configured")}
 b,err:=Connect(url);if err!=nil{t.Fatal(err)}
 defer b.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
 defer cancel()
 if err=b.Ensure(ctx);err!=nil{t.Fatal(err)}
 id:=time.Now().UnixNano()
 notice:=memory.OutboxNotice{SchemaVersion:1,OutboxID:id}
 if err=b.Publish(ctx,notice);err!=nil{t.Fatal(err)}
 consumer,err:=b.js.CreateOrUpdateConsumer(ctx,StreamName,jetstream.ConsumerConfig{
  Durable:"ci-projection-pull",FilterSubject:Subject,AckPolicy:jetstream.AckExplicitPolicy,
 })
 if err!=nil{t.Fatal(err)}
 batch,err:=consumer.Fetch(64,jetstream.FetchMaxWait(3*time.Second))
 if err!=nil{t.Fatal(err)}
 found:=false
 for msg:=range batch.Messages(){
  var incoming memory.OutboxNotice
  if err:=json.Unmarshal(msg.Data(),&incoming);err==nil&&incoming.OutboxID==id{found=true}
  if err:=msg.DoubleAck(ctx);err!=nil{t.Fatal(err)}
 }
 if err=batch.Error();err!=nil{t.Fatal(err)}
 if !found{t.Fatal("JetStream durable consumer did not receive published notification")}
}
