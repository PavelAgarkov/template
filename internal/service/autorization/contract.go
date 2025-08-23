package autorization

import (
	"context"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
)

type ServiceInterface interface {
	Generate(ctx context.Context, name string) (*pg_model.Authorized, error)
	CheckToken(token string, client string) (bool, error)
}
