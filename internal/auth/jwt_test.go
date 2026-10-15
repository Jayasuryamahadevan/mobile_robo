package auth

import (
 "strings"
 "testing"
 "time"
)

func TestTokenRoundTrip(t *testing.T) {
 secret:=[]byte(strings.Repeat("a",32))
 now:=time.Now()
 c:=Claims{Tenant:"one",User:"alice",Agent:"robot",Scope:"memory:read memory:write",Issuer:"issuer",Audience:"api",Expires:now.Add(time.Hour).Unix()}
 token,err:=Sign(c,secret); if err!=nil {t.Fatal(err)}
 got,err:=Verify(token,secret,"issuer","api",now); if err!=nil {t.Fatal(err)}
 if !got.Has("memory:read")||got.Has("memory:delete") {t.Fatalf("bad scopes: %+v",got)}
 _,err=Verify(token,[]byte(strings.Repeat("b",32)),"issuer","api",now)
 if err==nil {t.Fatal("accepted wrong signature")}
}
func TestExpiredAndWrongAudience(t *testing.T) {
 secret:=[]byte(strings.Repeat("c",32))
 now:=time.Now()
 c:=Claims{Tenant:"one",User:"alice",Issuer:"i",Audience:"a",Expires:now.Add(-time.Minute).Unix()}
 tok,_:=Sign(c,secret)
 if _,err:=Verify(tok,secret,"i","a",now);err==nil {t.Fatal("accepted expired token")}
 c.Expires=now.Add(time.Hour).Unix();tok,_=Sign(c,secret)
 if _,err:=Verify(tok,secret,"i","different",now);err==nil {t.Fatal("accepted wrong audience")}
}
