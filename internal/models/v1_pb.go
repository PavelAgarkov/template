package models

import "github.com/PavelAgarkov/template/internal/models/pg_model"

type GetTurnoverWithOffset struct {
	OfficeID pg_model.OfficeID `json:"office_id"`
	LastNmID int64             `json:"last_nm_id"`
	Offset   int64             `json:"offset"`
}

type GetTurnoverWithOffsetResponse struct {
	OfficeID pg_model.OfficeID
	LastNmID int64
}
