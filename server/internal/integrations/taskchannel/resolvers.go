package taskchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

type inboundRaw struct {
	PluginInstallationID string `json:"plugin_installation_id"`
	SenderName           string `json:"sender_name,omitempty"`
}

func NewResolverSet(q *db.Queries, tx engine.TxStarter, replier engine.OutboundReplier) engine.ResolverSet {
	return engine.ResolverSet{
		Installation: &installationResolver{q: q}, Identity: &identityResolver{q: q},
		AgentRouter: &intentAgentRouter{q: q},
		Dedup:       &deduper{q: q},
		Session: &sessionBinder{session: engine.NewChatSession(q, tx, TypeExternal, engine.SessionTitles{
			Group: "External group", Direct: "External direct message", Fallback: "External conversation",
		})},
		Audit: &auditor{q: q}, Replier: replier, OriginType: OriginExternalChat,
	}
}

func decodeInboundRaw(message channel.InboundMessage) (inboundRaw, error) {
	var raw inboundRaw
	if err := json.Unmarshal(message.Raw, &raw); err != nil {
		return raw, fmt.Errorf("decode task channel inbound metadata: %w", err)
	}
	if raw.PluginInstallationID == "" {
		return raw, errors.New("task channel inbound metadata has no Plugin installation id")
	}
	return raw, nil
}

type installationResolver struct{ q *db.Queries }

func (r *installationResolver) ResolveInstallation(ctx context.Context, message channel.InboundMessage) (engine.ResolvedInstallation, error) {
	raw, err := decodeInboundRaw(message)
	if err != nil {
		return engine.ResolvedInstallation{}, err
	}
	row, err := r.q.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{
		ChannelType: string(TypeExternal), AppID: raw.PluginInstallationID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.ResolvedInstallation{}, engine.ErrInstallationNotFound
	}
	if err != nil {
		return engine.ResolvedInstallation{}, err
	}
	return engine.ResolvedInstallation{
		ID: row.ID, WorkspaceID: row.WorkspaceID, AgentID: row.AgentID,
		InstallerUserID: row.InstallerUserID, Active: row.Status == "active", Platform: row,
	}, nil
}

type identityResolver struct{ q *db.Queries }

func (r *identityResolver) ResolveSender(ctx context.Context, installation engine.ResolvedInstallation, message channel.InboundMessage) (engine.ResolvedIdentity, error) {
	binding, err := r.q.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: installation.ID, ChannelUserID: message.Source.SenderID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.ResolvedIdentity{}, engine.ErrSenderUnbound
	}
	if err != nil {
		return engine.ResolvedIdentity{}, err
	}
	if _, err := r.q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: binding.MulticaUserID, WorkspaceID: installation.WorkspaceID,
	}); errors.Is(err, pgx.ErrNoRows) {
		return engine.ResolvedIdentity{}, engine.ErrSenderNotMember
	} else if err != nil {
		return engine.ResolvedIdentity{}, err
	}
	return engine.ResolvedIdentity{UserID: binding.MulticaUserID}, nil
}

// intentAgentRouter keeps Zalo's one-way task transport on the normal agent
// path, except for key provisioning requests which must execute only under the
// dedicated agent holding the required secrets.
type intentAgentRouter struct{ q *db.Queries }

func (r *intentAgentRouter) RouteAgent(ctx context.Context, installation engine.ResolvedInstallation, sender engine.ResolvedIdentity, message channel.InboundMessage) (pgtype.UUID, error) {
	row, ok := installation.Platform.(db.ChannelInstallation)
	if !ok || !InstallationIsReportOnly(row.Config) || !isLiteLLMKeyProvisionRequest(message.Text) {
		return installation.AgentID, nil
	}

	agents, err := r.q.ListAgents(ctx, installation.WorkspaceID)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("list LiteLLM provisioner candidates: %w", err)
	}
	var provisioner db.Agent
	for _, agent := range agents {
		if agent.Name == liteLLMKeyProvisionerName {
			provisioner = agent
			break
		}
	}
	if !provisioner.ID.Valid {
		return pgtype.UUID{}, errors.New("LiteLLM key provisioner is not configured")
	}
	if !r.senderCanInvoke(ctx, installation.WorkspaceID, provisioner, sender.UserID) {
		return pgtype.UUID{}, errors.New("sender cannot invoke the LiteLLM key provisioner")
	}
	return provisioner.ID, nil
}

func (r *intentAgentRouter) senderCanInvoke(ctx context.Context, workspaceID pgtype.UUID, agent db.Agent, senderID pgtype.UUID) bool {
	if agent.OwnerID == senderID {
		return true
	}
	if agent.PermissionMode != "public_to" {
		return false
	}
	targets, err := r.q.ListAgentInvocationTargets(ctx, agent.ID)
	if err != nil {
		return false
	}
	for _, target := range targets {
		switch target.TargetType {
		case "workspace":
			if _, err := r.q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
				UserID: senderID, WorkspaceID: workspaceID,
			}); err == nil {
				return true
			}
		case "member":
			if target.TargetID == senderID {
				return true
			}
		}
	}
	return false
}

type deduper struct{ q *db.Queries }

func (r *deduper) Claim(ctx context.Context, installationID pgtype.UUID, messageID string) (pgtype.UUID, error) {
	claim, err := r.q.ClaimChannelInboundDedup(ctx, db.ClaimChannelInboundDedupParams{InstallationID: installationID, MessageID: messageID})
	if errors.Is(err, pgx.ErrNoRows) {
		return pgtype.UUID{}, engine.ErrDuplicate
	}
	return claim.ClaimToken, err
}

func (r *deduper) Mark(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := r.q.MarkChannelInboundDedupProcessed(ctx, db.MarkChannelInboundDedupProcessedParams{
		InstallationID: installationID, MessageID: messageID, ClaimToken: claimToken,
	})
	return err
}

func (r *deduper) Release(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := r.q.ReleaseChannelInboundDedup(ctx, db.ReleaseChannelInboundDedupParams{
		InstallationID: installationID, MessageID: messageID, ClaimToken: claimToken,
	})
	return err
}

type chatSession interface {
	EnsureSession(context.Context, engine.EnsureSessionInput) (pgtype.UUID, error)
	StartSession(context.Context, engine.StartSessionInput) (engine.StartSessionResult, error)
	MarkPendingFresh(context.Context, pgtype.UUID, string) error
	AppendUserMessage(context.Context, engine.AppendInput) (engine.AppendResult, error)
	BindMediaRefs(context.Context, engine.BindMediaInput) error
}

type sessionBinder struct{ session chatSession }

func sessionRouting(installation engine.ResolvedInstallation, message channel.InboundMessage) (string, []byte) {
	config, _ := json.Marshal(map[string]string{"chat_id": message.Source.ChatID})
	bindingKey := message.Source.ChatID
	if row, ok := installation.Platform.(db.ChannelInstallation); ok && row.AgentID != installation.AgentID &&
		InstallationIsReportOnly(row.Config) && isLiteLLMKeyProvisionRequest(message.Text) {
		bindingKey += liteLLMKeyRouteSuffix
	}
	return bindingKey, config
}

func sessionCommandText(installation engine.ResolvedInstallation, message channel.InboundMessage) string {
	commandText := message.CommandText
	if commandText == "" {
		commandText = message.Text
	}
	row, ok := installation.Platform.(db.ChannelInstallation)
	if !ok || !InstallationIsReportOnly(row.Config) || !isLiteLLMKeyProvisionRequest(message.Text) {
		return commandText
	}
	if _, explicit := engine.ParseIssueCommand(commandText); explicit {
		return commandText
	}
	return "/issue " + strings.TrimSpace(message.Text)
}

func (r *sessionBinder) EnsureSession(ctx context.Context, params engine.EnsureSessionParams) (pgtype.UUID, error) {
	bindingKey, config := sessionRouting(params.Installation, params.Message)
	return r.session.EnsureSession(ctx, engine.EnsureSessionInput{
		WorkspaceID: params.Installation.WorkspaceID, AgentID: params.Installation.AgentID,
		InstallationID: params.Installation.ID, Sender: params.Sender,
		BindingKey: bindingKey, BindingConfig: config, ChatType: params.Message.Source.ChatType,
	})
}

func (r *sessionBinder) StartSession(ctx context.Context, params engine.StartSessionParams) (engine.StartSessionResult, error) {
	bindingKey, config := sessionRouting(params.Installation, params.Message)
	commandText := sessionCommandText(params.Installation, params.Message)
	return r.session.StartSession(ctx, engine.StartSessionInput{
		EnsureSessionInput: engine.EnsureSessionInput{
			WorkspaceID: params.Installation.WorkspaceID, AgentID: params.Installation.AgentID,
			InstallationID: params.Installation.ID, Sender: params.Creator,
			BindingKey: bindingKey, BindingConfig: config, ChatType: params.Message.Source.ChatType,
		},
		Initiator: params.Sender, Body: params.Message.Text, CommandText: commandText,
		MessageID: params.Message.MessageID, ClaimToken: params.ClaimToken,
		MediaPendingSeconds: params.MediaPendingSeconds, PersistMessage: params.PersistMessage,
		HistoryBoundaryPending: params.HistoryBoundaryPending, BeforeCommit: params.BeforeCommit,
	})
}

func (r *sessionBinder) MarkPendingFresh(ctx context.Context, sessionID pgtype.UUID, messageID string) error {
	return r.session.MarkPendingFresh(ctx, sessionID, messageID)
}

func (r *sessionBinder) AppendMessage(ctx context.Context, params engine.AppendParams) (engine.AppendResult, error) {
	commandText := sessionCommandText(params.Installation, params.Message)
	return r.session.AppendUserMessage(ctx, engine.AppendInput{
		SessionID: params.SessionID, Sender: params.Sender, InstallationID: params.InstallationID,
		Body: params.Message.Text, CommandText: commandText, MessageID: params.Message.MessageID,
		ClaimToken: params.ClaimToken, MediaPendingSeconds: params.MediaPendingSeconds,
		ForceFresh: params.Message.ForceFresh,
	})
}

func (r *sessionBinder) BindMedia(ctx context.Context, params engine.BindMediaParams) (engine.BindMediaResult, error) {
	return engine.BindMediaResult{}, r.session.BindMediaRefs(ctx, engine.BindMediaInput{
		MessageID: params.MessageID, SessionID: params.SessionID, WorkspaceID: params.WorkspaceID,
		Sender: params.Sender, MediaRefs: params.MediaRefs,
	})
}

type auditor struct{ q *db.Queries }

func (r *auditor) RecordDrop(ctx context.Context, installationID pgtype.UUID, message channel.InboundMessage, reason engine.DropReason) error {
	return r.q.RecordChannelInboundDrop(ctx, db.RecordChannelInboundDropParams{
		ID: dbid.NewV7(), ChannelType: string(TypeExternal), EventType: "external.message",
		DropReason: string(reason), InstallationID: installationID,
		ChannelChatID: optionalText(message.Source.ChatID), ChannelEventID: optionalText(message.EventID),
		ChannelMessageID: optionalText(message.MessageID),
	})
}

func optionalText(value string) pgtype.Text { return pgtype.Text{String: value, Valid: value != ""} }
