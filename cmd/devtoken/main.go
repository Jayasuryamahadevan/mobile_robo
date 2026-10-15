package main

import (
 "flag"
 "fmt"
 "os"
 "strings"
 "time"
 "github.com/Jayasuryamahadevan/mobile_robo/internal/auth"
)
func main(){
 tenant:=flag.String("tenant","demo","tenant ID")
 user:=flag.String("user","alice","user ID")
 scopes:=flag.String("scopes","memory:read,memory:write,memory:delete","comma-separated scopes")
 minutes:=flag.Int("minutes",60,"token lifetime (<= 1440)")
 flag.Parse()
 secret:=os.Getenv("JWT_SECRET")
 if len(secret)<32||*minutes<1||*minutes>1440 {fmt.Fprintln(os.Stderr,"set JWT_SECRET >=32 characters and minutes 1..1440");os.Exit(1)}
 now:=time.Now()
 c:=auth.Claims{Tenant:*tenant,User:*user,Agent:"phone_robot",Scope:strings.Join(strings.Split(*scopes,",")," "),Issuer:env("JWT_ISSUER","phone-robot"),Audience:env("JWT_AUDIENCE","memory-api"),Expires:now.Add(time.Duration(*minutes)*time.Minute).Unix()}
 token,err:=auth.Sign(c,[]byte(secret));if err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)}
 fmt.Println(token)
}
func env(k,fallback string)string{if v:=os.Getenv(k);v!=""{return v};return fallback}
