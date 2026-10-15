package main

import (
 "context"
 "errors"
 "log/slog"
 "net/http"
 "os"
 "os/signal"
 "syscall"
 "time"

 "github.com/Jayasuryamahadevan/mobile_robo/internal/config"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/httpapi"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/memory"
)
func main(){
 slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout,nil)))
 c,err:=config.Load();if err!=nil{slog.Error("config","error",err);os.Exit(1)}
 ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
 s,err:=memory.NewStore(ctx,c);if err!=nil{slog.Error("database","error",err);os.Exit(1)};defer s.Close()
 server:=&http.Server{Addr:":"+c.Port,Handler:(&httpapi.API{Store:s}).Handler(),ReadHeaderTimeout:3*time.Second,ReadTimeout:7*time.Second,WriteTimeout:8*time.Second,IdleTimeout:30*time.Second}
 go func(){
  <-ctx.Done()
  shutdown,done:=context.WithTimeout(context.Background(),5*time.Second);defer done()
  if e:=server.Shutdown(shutdown);e!=nil{slog.Warn("shutdown","error",e)}
 }()
 slog.Info("memory API listening","port",c.Port)
 if err=server.ListenAndServe();err!=nil&&!errors.Is(err,http.ErrServerClosed){slog.Error("server","error",err);os.Exit(1)}
}
