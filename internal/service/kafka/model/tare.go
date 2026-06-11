package models

const TareMoveEntity = "tare_move"

type WhTare struct {
	TareSticker        string  `json:"tare_sticker"`
	Tare               int     `json:"tare"`
	TareType           string  `json:"tare_type"`
	Dt                 string  `json:"dt"`
	OfficeId           int     `json:"office_id"`
	EmployeeId         int     `json:"employee_id"`
	WhId               int     `json:"wh_id"`
	PlaceId            int     `json:"place_id"`
	Lifecycle          string  `json:"lifecycle"`
	LifecycleDt        string  `json:"lifecycle_dt"`
	LifecycleInit      string  `json:"lifecycle_init"`
	LifecycleDstType   string  `json:"lifecycle_dst_type"`
	LifecycleDst       int     `json:"lifecycle_dst"`
	WhTareEntry        string  `json:"wh_tare_entry"`
	ReportedDt         string  `json:"reported_dt"`
	IsTareContentEmpty bool    `json:"is_tare_content_empty"`
	StateID            *string `json:"state_id,omitempty"` // unused
}
