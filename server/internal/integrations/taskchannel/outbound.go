package taskchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type outboundQueries interface {
	GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error)
	GetIssue(context.Context, pgtype.UUID) (db.Issue, error)
	GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error)
	TaskHasChannelIngestedMessages(context.Context, pgtype.UUID) (bool, error)
	GetChannelTaskDelivery(context.Context, pgtype.UUID) (db.ChannelTaskDelivery, error)
	GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error)
	EnqueueChannelExtensionOutbound(context.Context, db.EnqueueChannelExtensionOutboundParams) (db.ChannelExtensionOutbox, error)
}

type Outbound struct {
	q      outboundQueries
	appURL string
	logger *slog.Logger
}

func NewOutbound(q outboundQueries, appURL string, logger *slog.Logger) *Outbound {
	if logger == nil {
		logger = slog.Default()
	}
	return &Outbound{q: q, appURL: strings.TrimRight(strings.TrimSpace(appURL), "/"), logger: logger}
}

func (o *Outbound) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventChatDone, o.handleEvent)
	bus.Subscribe(protocol.EventTaskCompleted, o.handleEvent)
	bus.Subscribe(protocol.EventTaskFailed, o.handleEvent)
}

func (o *Outbound) handleEvent(event events.Event) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := o.processEvent(ctx, event); err != nil {
			o.logger.WarnContext(ctx, "task channel: terminal reply enqueue failed", "error", err, "task_id", event.TaskID)
		}
	}()
}

func (o *Outbound) processEvent(ctx context.Context, event events.Event) error {
	taskID, ok := eventTaskID(event)
	if !ok {
		return nil
	}
	text := ""
	if event.Type != protocol.EventTaskCompleted {
		text = eventText(event)
		if text == "" {
			return nil
		}
	}
	delivery, err := o.q.GetChannelTaskDelivery(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup task channel delivery: %w", err)
	}
	if delivery.ChannelType != string(TypeExternal) {
		return nil
	}
	task, err := o.q.GetAgentTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("load task channel task: %w", err)
	}
	if event.Type == protocol.EventTaskCompleted {
		// Every channel Chat task also publishes task:completed after chat:done.
		// Only Issue tasks use this event, otherwise one Chat turn would report
		// twice. The delivery snapshot is the authoritative proof that the Issue
		// originated from this external channel.
		if !task.IssueID.Valid {
			return nil
		}
		text = completedIssueTaskText(task)
		if text == "" {
			return nil
		}
	}
	if task.IssueID.Valid {
		text = appendIssueLink(text, o.issueLink(ctx, task.IssueID))
	}
	deliver, err := engine.TaskInputIsChannelIngested(ctx, o.q, task)
	if err != nil || !deliver {
		return err
	}
	installation, err := o.q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID: delivery.InstallationID, ChannelType: string(TypeExternal),
	})
	if errors.Is(err, pgx.ErrNoRows) || installation.Status != "active" {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = o.q.EnqueueChannelExtensionOutbound(ctx, db.EnqueueChannelExtensionOutboundParams{
		InstallationID: installation.ID, ChannelChatID: externalDeliveryChatID(delivery), Text: text,
		DedupeKey: "task:" + uuidString(taskID) + ":" + event.Type,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}

func externalDeliveryChatID(delivery db.ChannelTaskDelivery) string {
	var route struct {
		ChatID string `json:"chat_id"`
	}
	if err := json.Unmarshal(delivery.Config, &route); err == nil && strings.TrimSpace(route.ChatID) != "" {
		return route.ChatID
	}
	return delivery.ChannelChatID
}

func (o *Outbound) issueLink(ctx context.Context, issueID pgtype.UUID) string {
	identifier := uuidString(issueID)
	if issue, err := o.q.GetIssue(ctx, issueID); err == nil {
		if workspace, workspaceErr := o.q.GetWorkspace(ctx, issue.WorkspaceID); workspaceErr == nil {
			if resolved := service.IssueIdentifier(workspace.IssuePrefix, issue.Number); resolved != "" {
				identifier = resolved
			}
		}
	}
	return issueWebURL(o.appURL, identifier)
}

func completedIssueTaskText(task db.AgentTaskQueue) string {
	var payload protocol.TaskCompletedPayload
	if err := json.Unmarshal(task.Result, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(util.UnescapeBackslashEscapes(payload.Output))
}

func eventTaskID(event events.Event) (pgtype.UUID, bool) {
	var id pgtype.UUID
	if event.TaskID != "" {
		_ = id.Scan(event.TaskID)
	}
	if !id.Valid {
		switch payload := event.Payload.(type) {
		case protocol.ChatDonePayload:
			_ = id.Scan(payload.TaskID)
		case map[string]any:
			if value, _ := payload["task_id"].(string); value != "" {
				_ = id.Scan(value)
			}
		}
	}
	return id, id.Valid
}

func eventText(event events.Event) string {
	switch payload := event.Payload.(type) {
	case protocol.ChatDonePayload:
		return payload.Content
	case map[string]any:
		if event.Type == protocol.EventTaskFailed {
			if pending, _ := payload["retry_pending"].(bool); pending {
				return ""
			}
			if message, _ := payload["error"].(string); message != "" {
				return "⚠️ Agent xử lý thất bại: " + message
			}
			return "⚠️ Agent xử lý thất bại. Vui lòng thử lại."
		}
		message, _ := payload["content"].(string)
		return message
	}
	return ""
}
