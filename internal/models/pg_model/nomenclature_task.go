package pg_model

import (
	"time"
)

const (
	TaskNMButch = 15000
)

const (
	NomenclaturePushPriorityTooLow    = 500
	NomenclaturePushPriorityLow       = 400
	NomenclaturePushPriorityMedium    = 300
	NomenclaturePushPriorityNormal    = 200
	NomenclaturePushPriorityHigh      = 100
	NomenclaturePushPriorityImmediate = 50
	NomenclaturePushPriorityCritical  = 1
	NomenclatureDefaultPushPriority   = NomenclaturePushPriorityNormal

	StreamTaskType            = "stream_task"
	RecalculateByTtlTask      = "recalculate_by_ttl_task"
	RecalculateByOfficeIDTask = "recalculate_by_office_id_task"
	CriticalWarming           = "critical_warming_task"

	DefaultQueueNumber = 1

	UpsertReason = "upsert"
	DeleteReason = "delete"
)

type PushParams struct {
	QueueNumber int64
	Priority    int64
	TaskType    string
}

type Bucket struct {
	OfficeID OfficeID
	Nms      []int64
	Push     *PushParams
}
type OfficeID int64
type SeparatedNmsMap map[OfficeID][]*Bucket

type Task struct {
	ID          int64
	TaskType    string
	Reason      string
	OfficeID    int64
	QueueNumber int
	Priority    int
	CreatedAt   *time.Time
	LastUpdate  *time.Time
}

type TaskNomenclatureItem struct {
	TaskID int64
	NmID   int64
}
