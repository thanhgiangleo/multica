package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/realtime"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Project-scoped members never join the workspace room, so every event they
// receive is routed here on purpose: to the rooms of the projects they were
// granted, or to the shared room for catalogs every member may read. An event
// that is in neither list reaches them through nothing, which is the safe
// default for event types added later.

// issueProjectResolver maps an issue to its project. ok is false when the
// issue is gone, has no project, or the lookup failed; the event then reaches
// no project room.
type issueProjectResolver func(ctx context.Context, workspaceID, issueID string) (projectID string, ok bool)

type listenerOptions struct {
	resolveIssueProject issueProjectResolver
}

type listenerOption func(*listenerOptions)

// withIssueProjectResolver enables project routing of issue-scoped events.
func withIssueProjectResolver(r issueProjectResolver) listenerOption {
	return func(o *listenerOptions) { o.resolveIssueProject = r }
}

func newIssueProjectResolver(q *db.Queries) issueProjectResolver {
	return func(ctx context.Context, workspaceID, issueID string) (string, bool) {
		wsUUID, err := util.ParseUUID(workspaceID)
		if err != nil {
			return "", false
		}
		idUUID, err := util.ParseUUID(issueID)
		if err != nil {
			return "", false
		}
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		projectID, err := q.GetIssueProjectID(ctx, db.GetIssueProjectIDParams{ID: idUUID, WorkspaceID: wsUUID})
		if err != nil || !projectID.Valid {
			return "", false
		}
		return util.UUIDToString(projectID), true
	}
}

// restrictedSharedEvents carry catalogs and lifecycle every member may read,
// whatever their project. They mirror the read routes the middleware allows.
var restrictedSharedEvents = map[string]bool{
	protocol.EventIssueStatusChanged:   true,
	protocol.EventIssueWorkflowChanged: true,
	protocol.EventLabelCreated:         true,
	protocol.EventLabelUpdated:         true,
	protocol.EventLabelDeleted:         true,
	protocol.EventPropertyCreated:      true,
	protocol.EventPropertyUpdated:      true,
	protocol.EventWorkspaceUpdated:     true,
	protocol.EventWorkspaceDeleted:     true,
	// The hub reads this frame to close the removed member's sockets.
	protocol.EventMemberRemoved: true,
}

// restrictedIssueEvents are routed to the project of the issue they belong to.
var restrictedIssueEvents = map[string]bool{
	protocol.EventIssueCreated:            true,
	protocol.EventIssueUpdated:            true,
	protocol.EventIssueDeleted:            true,
	protocol.EventIssueMetadataChanged:    true,
	protocol.EventIssueAttachmentsChanged: true,
	protocol.EventCommentCreated:          true,
	protocol.EventCommentUpdated:          true,
	protocol.EventCommentDeleted:          true,
	protocol.EventCommentResolved:         true,
	protocol.EventCommentUnresolved:       true,
	protocol.EventReactionAdded:           true,
	protocol.EventReactionRemoved:         true,
	protocol.EventIssueReactionAdded:      true,
	protocol.EventIssueReactionRemoved:    true,
	protocol.EventSubscriberAdded:         true,
	protocol.EventSubscriberRemoved:       true,
	protocol.EventActivityCreated:         true,
	protocol.EventIssueLabelsChanged:      true,
	protocol.EventIssuePropertiesChanged:  true,
	protocol.EventPullRequestLinked:       true,
	protocol.EventPullRequestUpdated:      true,
	protocol.EventPullRequestUnlinked:     true,
	// Task lifecycle of an issue run; chat-task events have no issue and
	// stay off these rooms.
	protocol.EventTaskQueued:                true,
	protocol.EventTaskCoalesced:             true,
	protocol.EventTaskDispatch:              true,
	protocol.EventTaskRunning:               true,
	protocol.EventTaskWaitingLocalDirectory: true,
	protocol.EventTaskProgress:              true,
	protocol.EventTaskCompleted:             true,
	protocol.EventTaskFailed:                true,
	protocol.EventTaskCancelled:             true,
	protocol.EventTaskMessage:               true,
}

// restrictedProjectEvents carry their project id directly.
var restrictedProjectEvents = map[string]bool{
	protocol.EventProjectCreated:         true,
	protocol.EventProjectUpdated:         true,
	protocol.EventProjectDeleted:         true,
	protocol.EventProjectResourceCreated: true,
	protocol.EventProjectResourceUpdated: true,
	protocol.EventProjectResourceDeleted: true,
}

// routeRestricted delivers e to the rooms project-scoped members sit in. data
// is the frame already marshaled for workspace fanout.
func routeRestricted(b realtime.Broadcaster, e events.Event, data []byte, opts listenerOptions) {
	if e.WorkspaceID == "" {
		return
	}
	if e.Type == protocol.EventMemberAccessChanged {
		return
	}
	if restrictedSharedEvents[e.Type] {
		b.BroadcastToScope(realtime.ScopeWorkspaceShared, e.WorkspaceID, data)
		return
	}
	switch {
	case restrictedProjectEvents[e.Type]:
		frame := decodeFramePayload(data)
		if id := projectIDOf(frame); id != "" {
			b.BroadcastToScope(realtime.ScopeProject, id, data)
		}
	case restrictedIssueEvents[e.Type]:
		if e.Type == protocol.EventTaskMessage && e.ChatSessionID != "" {
			return
		}
		routeIssueEvent(b, e, data, opts)
	}
}

func routeIssueEvent(b realtime.Broadcaster, e events.Event, data []byte, opts listenerOptions) {
	payload := decodeFramePayload(data)
	if payload == nil {
		return
	}
	targets := map[string]bool{}
	if id := projectIDOf(payload); id != "" {
		targets[id] = true
	} else if opts.resolveIssueProject != nil {
		for _, issueID := range issueIDsOf(payload) {
			if projectID, ok := opts.resolveIssueProject(context.Background(), e.WorkspaceID, issueID); ok {
				targets[projectID] = true
			}
		}
	}
	for projectID := range targets {
		b.BroadcastToScope(realtime.ScopeProject, projectID, data)
	}
	// An issue that left a project must disappear from that project's members
	// without them ever seeing where it went.
	if e.Type == protocol.EventIssueUpdated {
		var prev string
		if raw, ok := e.Payload.(map[string]any); ok {
			switch v := raw["prev_project_id"].(type) {
			case string:
				prev = v
			case *string:
				if v != nil {
					prev = *v
				}
			}
		}
		issue, _ := payload["issue"].(map[string]any)
		issueID, _ := issue["id"].(string)
		if prev != "" && issueID != "" && !targets[prev] {
			removed, err := json.Marshal(map[string]any{
				"type":       protocol.EventIssueDeleted,
				"payload":    map[string]any{"issue_id": issueID, "project_id": prev},
				"actor_id":   e.ActorID,
				"actor_type": e.ActorType,
			})
			if err != nil {
				slog.Error("failed to marshal project-leave frame", "error", err)
				return
			}
			b.BroadcastToScope(realtime.ScopeProject, prev, removed)
		}
	}
}

func decodeFramePayload(data []byte) map[string]any {
	var frame struct {
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil
	}
	return frame.Payload
}

// projectIDOf reads a project id the producer put on the payload: directly,
// on the issue snapshot, or on the project itself.
func projectIDOf(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if id, ok := payload["project_id"].(string); ok && id != "" {
		return id
	}
	if issue, ok := payload["issue"].(map[string]any); ok {
		if id, ok := issue["project_id"].(string); ok && id != "" {
			return id
		}
		// An issue snapshot with no project belongs to no project room.
		return ""
	}
	if project, ok := payload["project"].(map[string]any); ok {
		if id, ok := project["id"].(string); ok {
			return id
		}
	}
	return ""
}

// issueIDsOf lists the issues an event is about, for events that name the
// issue rather than carrying its project.
func issueIDsOf(payload map[string]any) []string {
	var ids []string
	add := func(v any) {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			ids = append(ids, s)
		}
	}
	add(payload["issue_id"])
	if comment, ok := payload["comment"].(map[string]any); ok {
		add(comment["issue_id"])
	}
	if issue, ok := payload["issue"].(map[string]any); ok {
		add(issue["id"])
	}
	if linked, ok := payload["linked_issue_ids"].([]any); ok {
		for _, v := range linked {
			add(v)
		}
	}
	return ids
}
