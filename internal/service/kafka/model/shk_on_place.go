package models

const ShkOnPlaceEntity = "shk_on_place"

type Shk struct {
	ShkID            int64  `json:"shk_id"`
	WbstickerID      int64  `json:"wbsticker_id"`
	Barcode          string `json:"barcode"`
	ChrtID           int64  `json:"chrt_id"`
	NmID             int64  `json:"nm_id"`
	Dt               string `json:"dt"`
	EmployeeID       int64  `json:"employee_id"`
	PlaceID          int64  `json:"place_id"`
	StateID          string `json:"state_id"`
	OfficeID         int64  `json:"office_id"`
	WhID             int64  `json:"wh_id"`
	IsStock          bool   `json:"is_stock"`
	IsPodsort        bool   `json:"is_podsort"`
	BoxID            int64  `json:"box_id"`
	ContainerID      int64  `json:"container_id"`
	TransferBoxID    int64  `json:"transfer_box_id"`
	PalletID         int64  `json:"pallet_id"`
	PlaceTypeId      int64  `json:"place_type_id"`
	PlaceName        string `json:"place_name"`
	Stage            int64  `json:"stage"`
	Street           int64  `json:"street"`
	Section          int64  `json:"section"`
	Rack             int64  `json:"rack"`
	Field            int64  `json:"field"`
	ExtBarcode       string `json:"ext_barcode"`
	NoWBSticker      bool   `json:"no_wb_sticker"`
	GoodsSticker     string `json:"goods_sticker"`
	GoodsStickerType string `json:"goods_sticker_type"`
	AddedExtIDs      []struct {
		ExtID     string `json:"ext_id"`
		ExtTypeID string `json:"ext_type_id"`
		IsCorrect bool   `json:"is_correct"`
	} `json:"added_ext_ids"`
	Tare         int64  `json:"tare"`
	TareType     string `json:"tare_type"`
	CorrectionDt string `json:"correction_dt"`
}
