package memory
import (
 "encoding/json"
 "testing"
 "time"
)
func TestFactValidation(t *testing.T){
 f:=FactInput{Predicate:"preferred_language",Value:json.RawMessage(`"Tamil"`)}
 if err:=ValidateFact(&f,time.Now());err!=nil {t.Fatal(err)}
 if f.Subject!="self" {t.Fatalf("got subject %q",f.Subject)}
 f.Predicate="foo; DROP TABLE memory_facts"
 if err:=ValidateFact(&f,time.Now());err==nil {t.Fatal("accepted invalid predicate")}
}
func TestNoFutureFacts(t *testing.T) {
 tt:=time.Now().Add(time.Hour)
 f:=FactInput{Predicate:"location",Value:json.RawMessage(`"office"`),ValidFrom:&tt}
 if ValidateFact(&f,time.Now())==nil {t.Fatal("accepted future-effective fact")}
}
