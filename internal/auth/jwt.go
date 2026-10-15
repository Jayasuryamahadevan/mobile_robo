package auth

import (
 "context"
 "crypto/hmac"
 "crypto/sha256"
 "encoding/base64"
 "encoding/json"
 "errors"
 "fmt"
 "net/http"
 "strings"
 "time"
)

type Claims struct {
 Tenant string `json:"tenant_id"`
 User string `json:"sub"`
 Agent string `json:"agent_id"`
 Scope string `json:"scope"`
 Issuer string `json:"iss"`
 Audience string `json:"aud"`
 Expires int64 `json:"exp"`
 NotBefore int64 `json:"nbf,omitempty"`
}

func (c Claims) Has(scope string) bool {
 for _, s := range strings.Fields(c.Scope) { if s == scope { return true } }
 return false
}

func (c Claims) valid(now time.Time, issuer, audience string) error {
 if c.Tenant == "" || c.User == "" || len(c.Tenant)>128 || len(c.User)>128 { return errors.New("invalid principal") }
 if c.Issuer != issuer || c.Audience != audience { return errors.New("invalid token issuer or audience") }
 if c.Expires <= now.Unix() || c.NotBefore > now.Unix() { return errors.New("expired or premature token") }
 if c.Expires > now.Add(24*time.Hour).Unix() { return errors.New("token lifetime too long") }
 return nil
}

func Sign(c Claims, secret []byte) (string,error) {
 header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
 body, err := json.Marshal(c)
 if err != nil { return "",err }
 payload := header + "." + base64.RawURLEncoding.EncodeToString(body)
 m := hmac.New(sha256.New,secret); _,_ = m.Write([]byte(payload))
 return payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)),nil
}

func Verify(token string, secret []byte, issuer, audience string, now time.Time) (Claims,error) {
 var c Claims
 parts := strings.Split(token,".")
 if len(parts)!=3 { return c,errors.New("malformed token") }
 header,err:=base64.RawURLEncoding.DecodeString(parts[0])
 if err!=nil { return c,errors.New("invalid header") }
 var h struct { Alg string `json:"alg"`; Typ string `json:"typ"` }
 if json.Unmarshal(header,&h)!=nil || h.Alg!="HS256" || h.Typ!="JWT" { return c,errors.New("unsupported token algorithm") }
 signature,err:=base64.RawURLEncoding.DecodeString(parts[2])
 if err!=nil { return c,errors.New("invalid signature") }
 mac:=hmac.New(sha256.New,secret); _,_=mac.Write([]byte(parts[0]+"."+parts[1]))
 if !hmac.Equal(mac.Sum(nil),signature) { return c,errors.New("invalid signature") }
 body,err:=base64.RawURLEncoding.DecodeString(parts[1])
 if err!=nil || json.Unmarshal(body,&c)!=nil { return c,errors.New("invalid claims") }
 return c,c.valid(now,issuer,audience)
}

type contextKey struct{}
func Principal(ctx context.Context) Claims {
 c,_:=ctx.Value(contextKey{}).(Claims); return c
}
func Middleware(secret []byte, issuer,audience string,next http.Handler) http.Handler {
 return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  h:=r.Header.Get("Authorization")
  if !strings.HasPrefix(h,"Bearer ") { http.Error(w,"bearer token required",http.StatusUnauthorized); return }
  c,err:=Verify(strings.TrimPrefix(h,"Bearer "),secret,issuer,audience,time.Now())
  if err!=nil { http.Error(w,fmt.Sprintf("unauthorized: %v",err),http.StatusUnauthorized); return }
  next.ServeHTTP(w,r.WithContext(context.WithValue(r.Context(),contextKey{},c)))
 })
}
