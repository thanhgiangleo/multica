package taskchannel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const BindingTokenTTL = 15 * time.Minute

var (
	ErrBindingTokenInvalid       = errors.New("task channel binding token invalid or expired")
	ErrBindingAlreadyAssigned    = errors.New("external user is already bound to a different Multica user")
	ErrBindingNotWorkspaceMember = errors.New("binding user is not a workspace member")
)

type BindingToken struct {
	Raw       string
	ExpiresAt time.Time
}

type RedeemedBindingToken struct {
	WorkspaceID    pgtype.UUID
	InstallationID pgtype.UUID
	ChannelUserID  string
}

type BindingTokenService struct {
	q  *db.Queries
	tx engine.TxStarter
}

func NewBindingTokenService(q *db.Queries, tx engine.TxStarter) *BindingTokenService {
	return &BindingTokenService{q: q, tx: tx}
}

func (s *BindingTokenService) Mint(ctx context.Context, workspaceID, installationID pgtype.UUID, channelUserID string) (BindingToken, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return BindingToken{}, fmt.Errorf("generate task channel binding token: %w", err)
	}
	raw := base64.RawURLEncoding.EncodeToString(bytes)
	expiresAt := time.Now().Add(BindingTokenTTL)
	if _, err := s.q.CreateChannelBindingToken(ctx, db.CreateChannelBindingTokenParams{
		TokenHash: hashBindingToken(raw), WorkspaceID: workspaceID, InstallationID: installationID,
		ChannelType: string(TypeExternal), ChannelUserID: channelUserID,
		ExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
	}); err != nil {
		return BindingToken{}, fmt.Errorf("persist task channel binding token: %w", err)
	}
	return BindingToken{Raw: raw, ExpiresAt: expiresAt}, nil
}

func (s *BindingTokenService) RedeemAndBind(ctx context.Context, raw string, multicaUserID pgtype.UUID) (RedeemedBindingToken, error) {
	if s.tx == nil {
		return RedeemedBindingToken{}, errors.New("task channel binding service missing transaction starter")
	}
	tx, err := s.tx.Begin(ctx)
	if err != nil {
		return RedeemedBindingToken{}, fmt.Errorf("begin task channel bind: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)
	row, err := qtx.ConsumeChannelBindingToken(ctx, hashBindingToken(raw))
	if errors.Is(err, pgx.ErrNoRows) {
		return RedeemedBindingToken{}, ErrBindingTokenInvalid
	}
	if err != nil {
		return RedeemedBindingToken{}, fmt.Errorf("consume task channel binding token: %w", err)
	}
	if row.ChannelType != string(TypeExternal) {
		return RedeemedBindingToken{}, ErrBindingTokenInvalid
	}
	if _, err := qtx.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: multicaUserID, WorkspaceID: row.WorkspaceID,
	}); errors.Is(err, pgx.ErrNoRows) {
		return RedeemedBindingToken{}, ErrBindingNotWorkspaceMember
	} else if err != nil {
		return RedeemedBindingToken{}, fmt.Errorf("check task channel binding membership: %w", err)
	}
	if _, err := qtx.CreateChannelUserBinding(ctx, db.CreateChannelUserBindingParams{
		WorkspaceID: row.WorkspaceID, MulticaUserID: multicaUserID,
		InstallationID: row.InstallationID, ChannelType: string(TypeExternal),
		ChannelUserID: row.ChannelUserID, Config: []byte(`{}`),
	}); errors.Is(err, pgx.ErrNoRows) {
		return RedeemedBindingToken{}, ErrBindingAlreadyAssigned
	} else if err != nil {
		return RedeemedBindingToken{}, fmt.Errorf("create task channel user binding: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RedeemedBindingToken{}, fmt.Errorf("commit task channel bind: %w", err)
	}
	return RedeemedBindingToken{
		WorkspaceID: row.WorkspaceID, InstallationID: row.InstallationID, ChannelUserID: row.ChannelUserID,
	}, nil
}

func hashBindingToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
