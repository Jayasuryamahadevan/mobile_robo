package main

import (
 "context"
 "log/slog"
 "os"
 "os/signal"
 "syscall"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/bus"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)
func main(){
 slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout,nil)))
 cfg,err:=config.Load()
 if err!=nil{slog.Error("config","error",err);os.Exit(1)}
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM)
 defer stop()
 store,err:=memory.NewStore(ctx,cfg)
 if err!=nil{slog.Error("database","error",err);os.Exit(1)}
 defer store.Close()
 broker,err:=bus.Connect(cfg.NATSURL)
 if err!=nil{slog.Error("broker","error",err);os.Exit(1)}
 defer broker.Close()
 startup,cancel:=context.WithTimeout(ctx,15*time.Second)
 err=broker.Ensure(startup);cancel()
 if err!=nil{slog.Error("JetStream stream","error",err);os.Exit(1)}
 slog.Info("CQRS outbox relay started")
 if err=store.RunRelay(ctx,broker.Publish);err!=nil{slog.Error("relay","error",err);os.Exit(1)}
}
