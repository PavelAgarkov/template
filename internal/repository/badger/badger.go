package badger

import (
	sdk "github.com/PavelAgarkov/badger-wrapper"
)

type Repository struct {
	badgerStorageEngine sdk.BadgerStorageEngine
}

func NewRepository(badgerStorageEngine sdk.BadgerStorageEngine) *Repository {
	return &Repository{
		badgerStorageEngine: badgerStorageEngine,
	}
}

func (r *Repository) GetEngine() sdk.BadgerStorageEngine {
	return r.badgerStorageEngine
}
