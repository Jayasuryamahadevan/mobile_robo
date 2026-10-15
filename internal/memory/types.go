package memory

import (
 "encoding/json"
 "errors"
 "regexp"
 "strings"
 "time"
)

type EventInput struct {
 SessionID string `json:"session_id"`
 AgentID string `json:"agent_id"`
 RobotID string `json:"robot_id"`
 Source string `json:"source"`
 Content string `json:"content"`
 OccurredAt *time.Time `json:"occurred_at,omitempty"`
}
type FactInput struct {
 Subject string `json:"subject"`
 Predicate string `json:"predicate"`
 Value json.RawMessage `json:"value"`
 SessionID string `json:"session_id"`
 ValidFrom *time.Time `json:"valid_from,omitempty"`
}
type MemoryItem struct {
 ID string `json:"id"`
 Type string `json:"type"`
 Content string `json:"content"`
 SourceEventID string `json:"source_event_id"`
 Confidence float64 `json:"confidence"`
 Score float64 `json:"score"`
 ValidFrom *time.Time `json:"valid_from,omitempty"`
 RecordedFrom *time.Time `json:"recorded_from,omitempty"`
}
type Context struct {
 Query string `json:"query"`
 AsOf time.Time `json:"as_of"`
 KnownAt time.Time `json:"known_at"`
 Items []MemoryItem `json:"items"`
 EvidenceRequired bool `json:"evidence_required"`
 RetrievalDegraded bool `json:"retrieval_degraded"`
}
type ProcedureInput struct {
 Name string `json:"name"`
 Version int `json:"version"`
 Steps json.RawMessage `json:"steps"`
 RequiredPermissions []string `json:"required_permissions"`
}
var predicateRE=regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
func ValidateEvent(e *EventInput) error {
 if e.SessionID==""||len(e.SessionID)>128 || len(e.Content)==0||len(e.Content)>16000 {return errors.New("session_id and content (1..16000 bytes) required")}
 if e.AgentID=="" {e.AgentID="phone_robot"}
 if len(e.AgentID)>128||len(e.RobotID)>128 {return errors.New("agent or robot ID too long")}
 if e.Source=="" {e.Source="user"}
 switch e.Source {case "user","agent","tool","sensor","system":default:return errors.New("invalid source")}
 return nil
}
func ValidateFact(f *FactInput,now time.Time) error {
 if f.Subject=="" {f.Subject="self"}
 if len(f.Subject)>128 || !predicateRE.MatchString(f.Predicate) {return errors.New("invalid subject/predicate")}
 if !json.Valid(f.Value) || len(f.Value)>4096 || len(f.Value)==0 || string(f.Value)=="null" {return errors.New("value must be non-null JSON (<=4096 bytes)")}
 if f.ValidFrom!=nil && f.ValidFrom.After(now.Add(time.Second)) {return errors.New("future-effective facts are not supported by this endpoint")}
 if len(f.SessionID)>128 {return errors.New("invalid session ID")}
 return nil
}
func SearchTerms(q string) string {
 q=strings.TrimSpace(q)
 if len(q)>512 { q=q[:512] }
 return q
}
