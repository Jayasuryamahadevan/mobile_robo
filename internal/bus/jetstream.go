package bus

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "log/slog"
 "time"

 "github.com/nats-io/nats.go"
 "github.com/nats-io/nats.go/jetstream"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)

const (
 StreamName="MEMORY_PROJECTIONS"
 Subject="memory.projections.v1"
 ConsumerName="memory-projector-v1"
)
type Broker struct{
 connection *nats.Conn
 js jetstream.JetStream
}
func Connect(url string)(*Broker,error){
 if url==""{return nil,errors.New("NATS_URL is required")}
 nc,err:=nats.Connect(url,nats.Name("phone-robot-memory"),nats.Timeout(5*time.Second),
  nats.MaxReconnects(-1),nats.ReconnectWait(time.Second))
 if err!=nil{return nil,err}
 js,err:=jetstream.New(nc)
 if err!=nil{nc.Close();return nil,err}
 return &Broker{connection:nc,js:js},nil
}
func (b *Broker)Close(){_ =b.connection.Drain();b.connection.Close()}
func (b *Broker)Ensure(ctx context.Context)error{
 _,err:=b.js.CreateOrUpdateStream(ctx,jetstream.StreamConfig{
  Name:StreamName,Subjects:[]string{Subject},
  Storage:jetstream.FileStorage,Retention:jetstream.LimitsPolicy,
  MaxAge:7*24*time.Hour,MaxMsgs:2000000,
 })
 return err
}
func (b *Broker)Publish(ctx context.Context,notice memory.OutboxNotice)error{
 bytes,err:=json.Marshal(notice);if err!=nil{return err}
 _,err=b.js.Publish(ctx,Subject,bytes,jetstream.WithMsgID(fmt.Sprintf("outbox:%d",notice.OutboxID)))
 return err
}
func (b *Broker) RunProjector(ctx context.Context,s *memory.Store)error{
 consumer,err:=b.js.CreateOrUpdateConsumer(ctx,StreamName,jetstream.ConsumerConfig{
  Durable:ConsumerName,FilterSubject:Subject,AckPolicy:jetstream.AckExplicitPolicy,
  AckWait:90*time.Second,MaxDeliver:30,MaxAckPending:64,
 })
 if err!=nil{return err}
 for ctx.Err()==nil{
  batch,err:=consumer.Fetch(8,jetstream.FetchMaxWait(2*time.Second))
  if err!=nil{
   if ctx.Err()!=nil{return nil}
   slog.Warn("JetStream fetch","error",err);time.Sleep(time.Second);continue
  }
  for msg:=range batch.Messages(){
   var notice memory.OutboxNotice
   if err=json.Unmarshal(msg.Data(),&notice);err!=nil{
    slog.Error("invalid projection envelope","error",err);_ =msg.Term();continue
   }
   workCtx,cancel:=context.WithTimeout(ctx,65*time.Second)
   processErr:=s.ProcessNotice(workCtx,notice)
   cancel()
   switch{
   case processErr==nil || errors.Is(processErr,memory.ErrProjectionComplete):
    if err=msg.DoubleAck(context.Background());err!=nil{slog.Error("projection ack","error",err)}
   case errors.Is(processErr,memory.ErrProjectionDead):
    slog.Error("dead-letter projection","outbox_id",notice.OutboxID)
    _ =msg.Term()
   case errors.Is(processErr,memory.ErrProjectionBusy):
    _ =msg.NakWithDelay(3*time.Second)
   default:
    slog.Warn("projection retry","outbox_id",notice.OutboxID,"error",processErr)
    _ =msg.NakWithDelay(5*time.Second)
   }
  }
  if err:=batch.Error();err!=nil{slog.Warn("JetStream batch","error",err)}
 }
 return nil
}
