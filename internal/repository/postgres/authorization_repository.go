package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/PavelAgarkov/template/internal/models/pg_model"

	"github.com/jackc/pgx/v5"
)

type AuthorizationRepository struct {
	repository *Repository
}

func NewAuthorizationRepository(repository *Repository) *AuthorizationRepository {
	return &AuthorizationRepository{
		repository: repository,
	}
}

func (authorizationRepository *AuthorizationRepository) Generate(ctx context.Context, name string, token string) (*pg_model.Authorized, error) {
	query := `INSERT INTO cloud_template.authorized_client (client, token, created_at) VALUES ($1, $2, NOW()) ON CONFLICT do nothing returning id, client, created_at, token;`
	raw := authorizationRepository.repository.PoolMaster.QueryRow(ctx, query, name, token)

	au := &pg_model.Authorized{}
	if err := raw.Scan(&au.ID, &au.Client, &au.CreatedAt, &au.Token); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("authorization for client %s already exists", name)
		}
		return nil, fmt.Errorf("failed to generate authorization: %w", err)
	}
	return au, nil
}

func (authorizationRepository *AuthorizationRepository) GetAllAuthorizedUsers(ctx context.Context) ([]*pg_model.Authorized, error) {
	query := `SELECT id, client, created_at, token FROM cloud_template.authorized_client;`
	rows, err := authorizationRepository.repository.PoolMaster.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all authorized users: %w", err)
	}
	defer rows.Close()

	var users []*pg_model.Authorized
	for rows.Next() {
		user := &pg_model.Authorized{}
		if err := rows.Scan(&user.ID, &user.Client, &user.CreatedAt, &user.Token); err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", err)
	}

	return users, nil
}
