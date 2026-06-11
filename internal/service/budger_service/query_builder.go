package budger_service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	badger2 "github.com/PavelAgarkov/template/internal/repository/badger"
	pb "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"

	//pb "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/service"

	sdk "github.com/PavelAgarkov/badger-wrapper"
	v1model "github.com/PavelAgarkov/template/protobuf/badger_interface/v1/core"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type QueryService struct {
	engineRepository badger2.EngineRepositoryInterface
}

func NewQueryService(engineRepository badger2.EngineRepositoryInterface) *QueryService {
	return &QueryService{
		engineRepository: engineRepository,
	}
}

func protoJSON(m proto.Message) (string, error) {
	b, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type EntityDescriptor struct {
	New func() proto.Message
}

var entityRegistry = map[string]EntityDescriptor{
	"user": {
		New: func() proto.Message { return &v1model.User{} },
	},
}

func resolveEntity(table string) (EntityDescriptor, bool) {
	ed, ok := entityRegistry[table]
	return ed, ok
}

func (r *QueryService) BuildQuery(ctx context.Context, whereIs []*pb.Where, dbase, version, table string) ([]string, error) {
	ed, ok := resolveEntity(table)
	if !ok {
		return nil, fmt.Errorf("unknown table %q: no entity registered", table)
	}

	containID := func(where []*pb.Where) bool {
		for _, w := range where {
			if strings.TrimSpace(w.GetKey()) == "id" {
				return true
			}
		}
		return false
	}

	switch {
	// 1) Без фильтров — просто читаем все записи по PK-префиксу
	case len(whereIs) == 0:
		pkPrefix := sdk.BuildPKPrefix(dbase, version, table)
		out := make([]string, 0)
		var cursor []byte

		f := sdk.OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
			for _, v := range vals {
				msg := ed.New()
				if err := r.engineRepository.GetEngine().Unmarshal(v, msg); err != nil {
					return err
				}
				j, err := protoJSON(msg)
				if err != nil {
					return err
				}
				out = append(out, j)
			}
			return nil
		})
		if _, err := r.engineRepository.GetEngine().IterationByPkPrefix(ctx, pkPrefix, f, cursor, 0); err != nil {
			return nil, fmt.Errorf("error building query: %w", err)
		}

		return out, nil
	// 2) Один фильтр — id → прямой Get по PK
	case len(whereIs) == 1 && containID(whereIs):
		idStr := strings.TrimSpace(whereIs[0].GetValue())
		numID, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad id: %v", err)
		}
		pk := sdk.BuildPKKey(dbase, version, table, sdk.Uint64ToFixedWidthBytes(uint64(numID), 12))

		val, err := r.engineRepository.GetEngine().GetOneByPKRaw(ctx, pk)
		if err != nil {
			if errors.Is(err, sdk.ErrNotFound) || errors.Is(err, badger.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, fmt.Errorf("error building query: %w", err)
		}

		msg := ed.New()
		if err := r.engineRepository.GetEngine().Unmarshal(val, msg); err != nil {
			return nil, fmt.Errorf("error unmarshalling message: %w", err)
		}
		j, err := protoJSON(msg)
		if err != nil {
			return nil, fmt.Errorf("error converting to JSON: %w", err)
		}
		return []string{j}, nil
	// 3) Несколько фильтров — скан индексного префикса (id НЕ в левой части)
	case len(whereIs) > 0 && !containID(whereIs):
		parts := make([]sdk.IndexPart, 0, len(whereIs))
		for _, w := range whereIs {
			k := strings.TrimSpace(w.GetKey())
			if k == "" || k == "id" {
				continue
			}
			parts = append(parts, sdk.IndexPart{
				Field: k,
				Value: strings.TrimSpace(w.GetValue()),
			})
		}
		if len(parts) == 0 {
			return nil, fmt.Errorf("no valid index parts found in whereIs clause")
		}

		indexPrefix := sdk.BuildCompositeIndexPrefix(dbase, version, table, parts)

		var cursor []byte
		out := make([]string, 0)

		f := sdk.OnLanesFunc(func(ctx context.Context, pks [][]byte, vals [][]byte) error {
			for _, v := range vals {
				msg := ed.New()
				if err := r.engineRepository.GetEngine().Unmarshal(v, msg); err != nil {
					return err
				}
				j, err := protoJSON(msg)
				if err != nil {
					return err
				}
				out = append(out, j)
			}
			return nil
		})
		if _, err := r.engineRepository.GetEngine().IterationByIndexPrefix(ctx, indexPrefix, f, cursor, 0); err != nil {
			return nil, fmt.Errorf("error iterating by index prefix: %w", err)
		}

		return out, nil
	default:
		parts := make([]sdk.IndexPart, 0, len(whereIs))
		for _, w := range whereIs {
			k := strings.TrimSpace(w.GetKey())
			if k == "" || k == "id" {
				continue
			}
			parts = append(parts, sdk.IndexPart{
				Field: k,
				Value: strings.TrimSpace(w.GetValue()),
			})
		}
		if len(parts) == 0 {
			return nil, fmt.Errorf("no valid index parts found in whereIs clause")
		}

		indexPrefix := sdk.BuildCompositeIndexPrefix(dbase, version, table, parts)

		return nil, fmt.Errorf("invalid query/builder: bad index combination in whereIs on result index %s doesn't be executed", string(indexPrefix)+"#id=="+"1234567890123")
	}

	return nil, fmt.Errorf("unreachable code reached in BuildQuery")
}
