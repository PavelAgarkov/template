package badger

import (
	sdk "github.com/PavelAgarkov/badger-wrapper"
)

type (
	EngineRepositoryInterface interface {
		GetEngine() sdk.BadgerStorageEngine
	}
)
