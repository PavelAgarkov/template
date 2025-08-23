package v1

var (
	OfficeIDValidationError        = "office_id must be greater than 0, passed: %d"
	NmIDsContainValidationError    = "nm_ids must contain 1-15000 items, passed: %d items"
	CurrentNmIDValidationError     = "current_nm_id must be greater than 0, passed: %d"
	OffsetValidationError          = "offset must be greater than or equal to 0 and less than 15000, passed: %d"
	FailedToWarmNomenclaturesError = "failed warming for passed office: %d"
)
