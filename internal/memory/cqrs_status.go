package memory

import (
 "context"
 "time"
)

// ProjectionHealth is intentionally tenant scoped and contains no memory body.
type ProjectionHealth struct{
 Pending int64 `json:"pending"`
 Running int64 `json:"running"`
 Done int64 `json:"done"`
 Dead int64 `json:"dead"`
 Unpublished int64 `json:"unpublished"`
 OldestPendingAt *time.Time `json:"oldest_pending_at,omitempty"`
}

func (s *Store) ProjectionStatus(ctx context.Context,tenant,user string)(ProjectionHealth,error){
 var h ProjectionHealth
 err:=s.DB.QueryRow(ctx,`
 SELECT COUNT(*) FILTER(WHERE status='pending'),
        COUNT(*) FILTER(WHERE status='running'),
        COUNT(*) FILTER(WHERE status='done'),
        COUNT(*) FILTER(WHERE status='dead'),
        COUNT(*) FILTER(WHERE published_at IS NULL AND status IN ('pending','running')),
        MIN(created_at) FILTER(WHERE status IN ('pending','running'))
 FROM memory_outbox WHERE tenant_id=$1 AND user_id=$2`,tenant,user).
 Scan(&h.Pending,&h.Running,&h.Done,&h.Dead,&h.Unpublished,&h.OldestPendingAt)
 return h,err
}
