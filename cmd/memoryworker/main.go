package main

import (
 "context"
 "log/slog"
 "os"
 "os/signal"
 "syscall"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)
func main(){
 slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout,nil)))
 c,err:=config.Load();if err!=nil{slog.Error("config","error",err);os.Exit(1)}
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
 s,err:=memory.NewStore(ctx,c);if err!=nil{slog.Error("database","error",err);os.Exit(1)};defer s.Close()
 slog.Info("outbox worker started")
 if err=s.RunWorker(ctx);err!=nil{slog.Error("worker","error",err);os.Exit(1)}
}
