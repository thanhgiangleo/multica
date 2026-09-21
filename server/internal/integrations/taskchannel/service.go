package taskchannel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	ErrInstallationNotFound = errors.New("task channel installation not found")
	ErrInstallationConflict = errors.New("this Plugin is already connected to another agent")
	ErrOutboxClaimLost      = errors.New("task channel outbox claim was lost")
)

type RegisterParams struct {
	PluginInstallationID pgtype.UUID
	WorkspaceID          pgtype.UUID
	AgentID              pgtype.UUID
	InstallerUserID      pgtype.UUID
	Provider             string
	AccountID            string
	DisplayName          string
}

type InboundRequest struct {
	EventID        string `json:"event_id"`
	MessageID      string `json:"message_id"`
	ChatID         string `json:"chat_id"`
	ChatType       string `json:"chat_type"`
	SenderID       string `json:"sender_id"`
	SenderStableID string `json:"sender_stable_id,omitempty"`
	SenderName     string `json:"sender_name,omitempty"`
	Text           string `json:"text"`
	CommandText    string `json:"command_text,omitempty"`
	AddressedToBot bool   `json:"addressed_to_bot"`
	ForceFresh     bool   `json:"force_fresh,omitempty"`
}

type Installation struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Provider    string `json:"provider"`
	AccountID   string `json:"account_id"`
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
}

type OutboxMessage struct {
	ID         string `json:"id"`
	ChatID     string `json:"chat_id"`
	Text       string `json:"text"`
	ClaimToken string `json:"claim_token"`
	Attempts   int32  `json:"attempts"`
}

type Service struct {
	queries *db.Queries
	router  *engine.Router
	binding *BindingTokenService
}

func NewService(queries *db.Queries, tx engine.TxStarter, router *engine.Router) *Service {
	return &Service{queries: queries, router: router, binding: NewBindingTokenService(queries, tx)}
}

func (s *Service) BindingTokens() *BindingTokenService { return s.binding }

// ListByWorkspace exposes only the provider-neutral metadata needed by the
// host's Integrations directory. Provider credentials and wire-format details
// continue to live exclusively in the out-of-process extension.
func (s *Service) ListByWorkspace(ctx context.Context, workspaceID pgtype.UUID) ([]Installation, error) {
	if !workspaceID.Valid {
		return nil, errors.New("workspace id must be valid")
	}
	rows, err := s.queries.ListChannelInstallationsByWorkspace(ctx, db.ListChannelInstallationsByWorkspaceParams{
		WorkspaceID: workspaceID,
		ChannelType: string(TypeExternal),
	})
	if err != nil {
		return nil, fmt.Errorf("list task channel installations: %w", err)
	}
	installations := make([]Installation, 0, len(rows))
	for _, row := range rows {
		installations = append(installations, installationResponse(row))
	}
	return installations, nil
}

func (s *Service) Register(ctx context.Context, params RegisterParams) (Installation, error) {
	if !params.PluginInstallationID.Valid || !params.WorkspaceID.Valid || !params.AgentID.Valid || !params.InstallerUserID.Valid {
		return Installation{}, errors.New("task channel registration ids must be valid")
	}
	if _, err := s.queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
		ID: params.AgentID, WorkspaceID: params.WorkspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Installation{}, ErrInstallationNotFound
		}
		return Installation{}, fmt.Errorf("load task channel agent: %w", err)
	}
	config, err := encodeInstallationConfig(
		uuidString(params.PluginInstallationID), params.Provider, params.AccountID, params.DisplayName,
	)
	if err != nil {
		return Installation{}, err
	}

	existing, err := s.queries.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{
		ChannelType: string(TypeExternal), AppID: uuidString(params.PluginInstallationID),
	})
	if err == nil && (existing.WorkspaceID != params.WorkspaceID || existing.AgentID != params.AgentID) {
		return Installation{}, ErrInstallationConflict
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, fmt.Errorf("load task channel installation: %w", err)
	}

	row, err := s.queries.UpsertChannelInstallation(ctx, db.UpsertChannelInstallationParams{
		WorkspaceID: params.WorkspaceID, AgentID: params.AgentID,
		ChannelType: string(TypeExternal), Config: config, InstallerUserID: params.InstallerUserID,
	})
	if err != nil {
		return Installation{}, fmt.Errorf("upsert task channel installation: %w", err)
	}
	return installationResponse(row), nil
}

func (s *Service) Ingest(ctx context.Context, pluginInstallationID pgtype.UUID, request InboundRequest) error {
	if s.router == nil {
		return errors.New("task channel router is unavailable")
	}
	request.EventID = strings.TrimSpace(request.EventID)
	request.MessageID = strings.TrimSpace(request.MessageID)
	request.ChatID = strings.TrimSpace(request.ChatID)
	request.SenderID = strings.TrimSpace(request.SenderID)
	request.Text = strings.TrimSpace(request.Text)
	if request.EventID == "" || request.MessageID == "" || request.ChatID == "" || request.SenderID == "" || request.Text == "" {
		return errors.New("event_id, message_id, chat_id, sender_id and text are required")
	}
	chatType := channel.ChatType(request.ChatType)
	if chatType != channel.ChatTypeP2P && chatType != channel.ChatTypeGroup {
		return errors.New("chat_type must be p2p or group")
	}
	commandText := strings.TrimSpace(request.CommandText)
	if commandText == "" {
		commandText = request.Text
	}
	raw, err := jsonMarshal(map[string]string{
		"plugin_installation_id": uuidString(pluginInstallationID),
		"sender_name":            request.SenderName,
	})
	if err != nil {
		return err
	}
	return s.router.Handle(ctx, channel.InboundMessage{
		EventID: request.EventID, MessageID: request.MessageID,
		Type: channel.MsgTypeText, Text: request.Text, CommandText: commandText,
		AddressedToBot: request.AddressedToBot, ForceFresh: request.ForceFresh,
		Source: channel.Source{
			ChannelType: TypeExternal, ChatID: request.ChatID, ChatType: chatType,
			SenderID: request.SenderID, SenderStableID: strings.TrimSpace(request.SenderStableID),
		},
		Raw: raw,
	})
}

func (s *Service) ClaimOutbox(ctx context.Context, pluginInstallationID pgtype.UUID, limit int32) ([]OutboxMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.queries.ClaimChannelExtensionOutbound(ctx, db.ClaimChannelExtensionOutboundParams{
		PluginInstallationID: uuidString(pluginInstallationID), ClaimLimit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("claim task channel outbox: %w", err)
	}
	out := make([]OutboxMessage, 0, len(rows))
	for _, row := range rows {
		out = append(out, OutboxMessage{
			ID: uuidString(row.ID), ChatID: row.ChannelChatID, Text: row.Text,
			ClaimToken: uuidString(row.ClaimToken), Attempts: row.Attempts,
		})
	}
	return out, nil
}

func (s *Service) AckOutbox(ctx context.Context, pluginInstallationID, id, claimToken pgtype.UUID) error {
	_, err := s.queries.AckChannelExtensionOutbound(ctx, db.AckChannelExtensionOutboundParams{
		ID: id, ClaimToken: claimToken, PluginInstallationID: uuidString(pluginInstallationID),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOutboxClaimLost
	}
	return err
}

func (s *Service) RetryOutbox(ctx context.Context, pluginInstallationID, id, claimToken pgtype.UUID, message string) error {
	_, err := s.queries.RetryChannelExtensionOutbound(ctx, db.RetryChannelExtensionOutboundParams{
		ID: id, ClaimToken: claimToken, LastError: strings.TrimSpace(message),
		PluginInstallationID: uuidString(pluginInstallationID),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOutboxClaimLost
	}
	return err
}

func installationResponse(row db.ChannelInstallation) Installation {
	config, _ := decodeInstallationConfig(row.Config)
	return Installation{
		ID: uuidString(row.ID), WorkspaceID: uuidString(row.WorkspaceID), AgentID: uuidString(row.AgentID),
		Provider: config.Provider, AccountID: config.AccountID, DisplayName: config.DisplayName, Status: row.Status,
	}
}

func uuidString(value pgtype.UUID) string {
	if !value.Valid {
		return ""
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", value.Bytes[0:4], value.Bytes[4:6], value.Bytes[6:8], value.Bytes[8:10], value.Bytes[10:16])
}
