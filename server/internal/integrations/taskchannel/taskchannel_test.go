package taskchannel

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func mustUUID(t *testing.T, raw string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	if err := id.Scan(raw); err != nil {
		t.Fatalf("parse UUID %q: %v", raw, err)
	}
	return id
}

func TestInstallationConfigIsProviderNeutral(t *testing.T) {
	raw, err := encodeInstallationConfig(
		"11111111-1111-1111-1111-111111111111", "  messaging-provider ", " account-1 ", " Support Bot ",
	)
	if err != nil {
		t.Fatalf("encodeInstallationConfig: %v", err)
	}
	config, err := decodeInstallationConfig(raw)
	if err != nil {
		t.Fatalf("decodeInstallationConfig: %v", err)
	}
	if config.Provider != "messaging-provider" || config.AccountID != "account-1" || config.DisplayName != "Support Bot" {
		t.Fatalf("unexpected config: %#v", config)
	}
	if _, err := encodeInstallationConfig("", "provider", "account", "bot"); err == nil {
		t.Fatal("missing Plugin installation id must be rejected")
	}
}

func TestInstallationIsReportOnlyIsScopedToZalo(t *testing.T) {
	zalo, err := encodeInstallationConfig(
		"11111111-1111-1111-1111-111111111111", " ZALO ", "account", "bot",
	)
	if err != nil {
		t.Fatalf("encode Zalo config: %v", err)
	}
	other, err := encodeInstallationConfig(
		"22222222-2222-2222-2222-222222222222", "mattermost", "account", "bot",
	)
	if err != nil {
		t.Fatalf("encode other config: %v", err)
	}

	if !InstallationIsReportOnly(zalo) {
		t.Fatal("Zalo installation must use report-only task handling")
	}
	if InstallationIsReportOnly(other) {
		t.Fatal("non-Zalo task channels must retain their existing interaction policy")
	}
	if InstallationIsReportOnly([]byte(`not-json`)) {
		t.Fatal("invalid installation config must fail closed to the default policy")
	}
}

func TestLiteLLMKeyProvisionIntent(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{text: "tạo key LLM cho phucnh18@vng.com.vn", want: true},
		{text: "cấp LiteLLM key", want: true},
		{text: "generate API key LLM", want: true},
		{text: "key LLM này dùng ở đâu", want: false},
		{text: "tạo API key GitHub", want: false},
	} {
		if got := isLiteLLMKeyProvisionRequest(tc.text); got != tc.want {
			t.Errorf("isLiteLLMKeyProvisionRequest(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestSessionRoutingSeparatesLiteLLMProvisioner(t *testing.T) {
	installationID := mustUUID(t, "11111111-1111-1111-1111-111111111111")
	defaultAgentID := mustUUID(t, "22222222-2222-2222-2222-222222222222")
	provisionerID := mustUUID(t, "33333333-3333-3333-3333-333333333333")
	config, err := encodeInstallationConfig(installationID.String(), "zalo", "bot-42", "Support Bot")
	if err != nil {
		t.Fatalf("encode installation config: %v", err)
	}
	installation := engine.ResolvedInstallation{
		ID: installationID, AgentID: provisionerID,
		Platform: db.ChannelInstallation{ID: installationID, AgentID: defaultAgentID, Config: config},
	}
	message := channel.InboundMessage{Text: "tạo key LLM", Source: channel.Source{ChatID: "zalo-chat"}}
	bindingKey, rawConfig := sessionRouting(installation, message)
	if bindingKey != "zalo-chat"+liteLLMKeyRouteSuffix {
		t.Fatalf("binding key = %q", bindingKey)
	}
	var routeConfig map[string]string
	if err := json.Unmarshal(rawConfig, &routeConfig); err != nil {
		t.Fatalf("decode routing config: %v", err)
	}
	if routeConfig["chat_id"] != "zalo-chat" {
		t.Fatalf("outbound chat_id = %q, want zalo-chat", routeConfig["chat_id"])
	}
}

func TestLiteLLMProvisionRouteCreatesIssueCommand(t *testing.T) {
	installationID := mustUUID(t, "11111111-1111-1111-1111-111111111111")
	config, err := encodeInstallationConfig(installationID.String(), "zalo", "bot-42", "Support Bot")
	if err != nil {
		t.Fatalf("encode installation config: %v", err)
	}
	installation := engine.ResolvedInstallation{
		ID:       installationID,
		AgentID:  mustUUID(t, "33333333-3333-3333-3333-333333333333"),
		Platform: db.ChannelInstallation{ID: installationID, Config: config},
	}

	message := channel.InboundMessage{Text: "tạo key litellm cho manvn@vng.com.vn"}
	if got, want := sessionCommandText(installation, message), "/issue tạo key litellm cho manvn@vng.com.vn"; got != want {
		t.Fatalf("sessionCommandText() = %q, want %q", got, want)
	}

	explicit := channel.InboundMessage{
		Text:        "tạo key litellm cho manvn@vng.com.vn",
		CommandText: "/issue cấp key theo yêu cầu",
	}
	if got := sessionCommandText(installation, explicit); got != explicit.CommandText {
		t.Fatalf("explicit issue command changed to %q", got)
	}

	ordinary := channel.InboundMessage{Text: "kiểm tra trạng thái hệ thống"}
	if got := sessionCommandText(installation, ordinary); got != ordinary.Text {
		t.Fatalf("ordinary command text = %q, want %q", got, ordinary.Text)
	}
}

func TestExternalDeliveryChatIDUsesRouteConfig(t *testing.T) {
	if got := externalDeliveryChatID(db.ChannelTaskDelivery{
		ChannelChatID: "zalo-chat" + liteLLMKeyRouteSuffix,
		Config:        []byte(`{"chat_id":"zalo-chat"}`),
	}); got != "zalo-chat" {
		t.Fatalf("external delivery chat ID = %q, want zalo-chat", got)
	}
	if got := externalDeliveryChatID(db.ChannelTaskDelivery{ChannelChatID: "fallback", Config: []byte(`{}`)}); got != "fallback" {
		t.Fatalf("fallback delivery chat ID = %q", got)
	}
}

func TestInstallationResponseExposesOnlyProviderNeutralMetadata(t *testing.T) {
	config, err := encodeInstallationConfig(
		"22222222-2222-2222-2222-222222222222", "zalo", "bot-42", "Support Bot",
	)
	if err != nil {
		t.Fatalf("encodeInstallationConfig: %v", err)
	}
	row := db.ChannelInstallation{
		ID:          mustUUID(t, "11111111-1111-1111-1111-111111111111"),
		WorkspaceID: mustUUID(t, "33333333-3333-3333-3333-333333333333"),
		AgentID:     mustUUID(t, "44444444-4444-4444-4444-444444444444"),
		Config:      config,
		Status:      "active",
	}
	got := installationResponse(row)
	if got.Provider != "zalo" || got.AccountID != "bot-42" || got.DisplayName != "Support Bot" || got.Status != "active" {
		t.Fatalf("unexpected installation response: %#v", got)
	}
}

func TestEventText(t *testing.T) {
	tests := []struct {
		name  string
		event events.Event
		want  string
	}{
		{"chat done", events.Event{Type: protocol.EventChatDone, Payload: protocol.ChatDonePayload{Content: "done"}}, "done"},
		{"serialized chat done", events.Event{Type: protocol.EventChatDone, Payload: map[string]any{"content": "done from map"}}, "done from map"},
		{"failed", events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"error": "timeout", "retry_pending": false}}, "⚠️ Agent xử lý thất bại: timeout"},
		{"failed without detail", events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"retry_pending": false}}, "⚠️ Agent xử lý thất bại. Vui lòng thử lại."},
		{"retry pending", events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"error": "timeout", "retry_pending": true}}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := eventText(test.event); got != test.want {
				t.Fatalf("eventText() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestCompletedIssueTaskText(t *testing.T) {
	task := db.AgentTaskQueue{Result: []byte(`{"output":"done\\nwith detail"}`)}
	if got := completedIssueTaskText(task); got != "done\nwith detail" {
		t.Fatalf("completedIssueTaskText() = %q", got)
	}
	if got := completedIssueTaskText(db.AgentTaskQueue{Result: []byte(`{"output":"   "}`)}); got != "" {
		t.Fatalf("blank completed output = %q, want empty", got)
	}
	if got := completedIssueTaskText(db.AgentTaskQueue{Result: []byte(`not-json`)}); got != "" {
		t.Fatalf("invalid completed output = %q, want empty", got)
	}
}

type noDeliveryQueries struct{ lookedUp bool }

func (q *noDeliveryQueries) GetChannelTaskDelivery(context.Context, pgtype.UUID) (db.ChannelTaskDelivery, error) {
	q.lookedUp = true
	return db.ChannelTaskDelivery{}, pgx.ErrNoRows
}
func (*noDeliveryQueries) GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error) {
	panic("GetAgentTask must not run without a delivery snapshot")
}
func (*noDeliveryQueries) GetIssue(context.Context, pgtype.UUID) (db.Issue, error) {
	panic("GetIssue must not run without a delivery snapshot")
}
func (*noDeliveryQueries) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	panic("GetWorkspace must not run without a delivery snapshot")
}
func (*noDeliveryQueries) TaskHasChannelIngestedMessages(context.Context, pgtype.UUID) (bool, error) {
	panic("TaskHasChannelIngestedMessages must not run without a delivery snapshot")
}
func (*noDeliveryQueries) GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	panic("GetChannelInstallation must not run without a delivery snapshot")
}
func (*noDeliveryQueries) EnqueueChannelExtensionOutbound(context.Context, db.EnqueueChannelExtensionOutboundParams) (db.ChannelExtensionOutbox, error) {
	panic("EnqueueChannelExtensionOutbound must not run without a delivery snapshot")
}

func TestTaskFailureCanResolveDeliveryWithoutEnvelopeSessionID(t *testing.T) {
	queries := &noDeliveryQueries{}
	outbound := NewOutbound(queries, "https://multica.test", nil)
	event := events.Event{
		Type: protocol.EventTaskFailed,
		Payload: map[string]any{
			"task_id":       "11111111-1111-1111-1111-111111111111",
			"error":         "runtime stopped",
			"retry_pending": false,
		},
	}
	if err := outbound.processEvent(context.Background(), event); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if !queries.lookedUp {
		t.Fatal("task delivery was not resolved from task_id when ChatSessionID was absent")
	}
}

type recordingOutbox struct {
	params db.EnqueueChannelExtensionOutboundParams
	err    error
}

func (q *recordingOutbox) EnqueueChannelExtensionOutbound(_ context.Context, params db.EnqueueChannelExtensionOutboundParams) (db.ChannelExtensionOutbox, error) {
	q.params = params
	return db.ChannelExtensionOutbox{}, q.err
}

func TestReplierQueuesImmediateAgentAcknowledgement(t *testing.T) {
	queue := &recordingOutbox{}
	replier := NewReplier(queue, nil, "")
	installationID := mustUUID(t, "11111111-1111-1111-1111-111111111111")
	replier.Reply(context.Background(), engine.ResolvedInstallation{
		ID: installationID,
		Platform: db.ChannelInstallation{
			ID: installationID,
		},
	}, channel.InboundMessage{
		MessageID: "room-1:message-1",
		Source:    channel.Source{ChatID: "room-1"},
	}, engine.Result{Outcome: engine.OutcomeIngested})

	if queue.params.Text != messageAccepted {
		t.Fatalf("queued text = %q, want %q", queue.params.Text, messageAccepted)
	}
	if queue.params.ChannelChatID != "room-1" || queue.params.DedupeKey != "inbound:room-1:message-1:ingested" {
		t.Fatalf("unexpected outbox routing: %#v", queue.params)
	}
}

func TestReplierIncludesIssueLinkInAcknowledgement(t *testing.T) {
	queue := &recordingOutbox{}
	replier := NewReplier(queue, nil, "https://multica.test/")
	installationID := mustUUID(t, "11111111-1111-1111-1111-111111111111")
	replier.Reply(context.Background(), engine.ResolvedInstallation{
		ID: installationID,
		Platform: db.ChannelInstallation{
			ID: installationID,
		},
	}, channel.InboundMessage{
		MessageID: "room-1:message-1",
		Source:    channel.Source{ChatID: "room-1"},
	}, engine.Result{
		Outcome:         engine.OutcomeIngested,
		IssueID:         mustUUID(t, "22222222-2222-2222-2222-222222222222"),
		IssueNumber:     42,
		IssueIdentifier: "ZBSI-42",
	})

	want := "✅ Đã tạo ZBSI-42. Agent đang xử lý.\n🔗 Issue: https://multica.test/issues/ZBSI-42"
	if queue.params.Text != want {
		t.Fatalf("queued text = %q, want %q", queue.params.Text, want)
	}
}

func TestReplierIgnoresIdempotentEnqueueConflict(t *testing.T) {
	queue := &recordingOutbox{err: pgx.ErrNoRows}
	replier := NewReplier(queue, nil, "")
	id := mustUUID(t, "11111111-1111-1111-1111-111111111111")
	replier.Reply(context.Background(), engine.ResolvedInstallation{
		Platform: db.ChannelInstallation{ID: id},
	}, channel.InboundMessage{MessageID: "m", Source: channel.Source{ChatID: "c"}}, engine.Result{Outcome: engine.OutcomeIngested})
	if !errors.Is(queue.err, pgx.ErrNoRows) {
		t.Fatal("test setup error")
	}
}
