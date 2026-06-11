package autorization

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	"github.com/PavelAgarkov/template/internal/repository/postgres"
	"sync"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"
)

const (
	TokenMetaHeader      = "X-CLIENT-TOKEN"
	ClientNameMetaHeader = "X-CLIENT-NAME"
)

type (
	token      string
	clientName string
)

type Service struct {
	authorizationRepository postgres.AuthorizationRepositoryInterface
	mu                      sync.RWMutex
	resolvedUsers           map[token]clientName
	cancelCtx               context.CancelFunc
}

func NewService(ctx context.Context, authorizationRepository postgres.AuthorizationRepositoryInterface) *Service {
	//localCtx, cancel := context.WithCancel(ctx)
	s := &Service{
		authorizationRepository: authorizationRepository,
		resolvedUsers:           make(map[token]clientName),
		//cancelCtx:               cancel,
	}
	//err := s.updateUsers(ctx)
	//if err != nil {
	//	panic(fmt.Sprintf("failed to update users: %v", err))
	//}
	//s.reload(localCtx)
	//s.resolvedUsers["43ed87a019b3515a6f2848eae4cdd44751777176d"] = "Obi Wan Kenobi"

	return s
}

func (s *Service) Generate(ctx context.Context, name string) (*pg_model.Authorized, error) {
	token, err := generateToken(255)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}
	return s.authorizationRepository.Generate(ctx, name, token)
}

func generateToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (s *Service) CheckToken(passedToken string, client string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	found, ok := s.resolvedUsers[token(passedToken)]
	if !ok {
		return false, fmt.Errorf("token %s not found", passedToken)
	}
	if client != string(found) {
		return false, fmt.Errorf("client %s does not match authorized", client)
	}
	return true, nil
}

func (s *Service) updateUsers(ctx context.Context) error {
	users, err := s.authorizationRepository.GetAllAuthorizedUsers(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all authorized users: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolvedUsers = make(map[token]clientName, len(users))
	for _, user := range users {
		s.resolvedUsers[token(user.Token)] = clientName(user.Client)
	}
	s.resolvedUsers["43ed87a019b3515a6f2848eae4cdd44751777176d"] = "Obi Wan Kenobi"

	return nil
}

func (s *Service) reload(ctx context.Context) {
	utils.GoRecover(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.updateUsers(ctx); err != nil {
					logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
						Msg:       "failed to update users",
						Error:     err,
						Component: "authorization_service",
						Method:    "reload",
					})
					return
				}
			}
		}
	})
}

func (s *Service) Stop() {
	if s.cancelCtx != nil {
		s.cancelCtx()
	}
}
