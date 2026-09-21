package taskchannel

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	messageAgentOffline  = "Agent đang offline. Hãy khởi động runtime rồi thử lại."
	messageAgentArchived = "Agent này đã được lưu trữ và không thể nhận công việc mới."
	messageFreshPending  = "✅ Đã xóa ngữ cảnh. Tin nhắn tiếp theo sẽ bắt đầu phiên mới."
	messageChatStarted   = "✅ Đã bắt đầu cuộc trò chuyện Multica mới."
	messageIssueUsage    = "Vui lòng thêm tiêu đề:\n\n/issue <tiêu đề>\n[mô tả] (không bắt buộc)"
	messageAccepted      = "⏳ Đã nhận việc. Agent đang xử lý và sẽ phản hồi khi hoàn tất."
)

type bindingMinter interface {
	Mint(context.Context, pgtype.UUID, pgtype.UUID, string) (BindingToken, error)
}

type outboundEnqueuer interface {
	EnqueueChannelExtensionOutbound(context.Context, db.EnqueueChannelExtensionOutboundParams) (db.ChannelExtensionOutbox, error)
}

type Replier struct {
	q       outboundEnqueuer
	binding bindingMinter
	appURL  string
}

func NewReplier(q outboundEnqueuer, binding bindingMinter, appURL string) *Replier {
	return &Replier{q: q, binding: binding, appURL: strings.TrimRight(appURL, "/")}
}

func issueWebURL(appURL, identifier string) string {
	base := strings.TrimRight(strings.TrimSpace(appURL), "/")
	identifier = strings.TrimSpace(identifier)
	if base == "" || identifier == "" {
		return ""
	}
	return base + "/issues/" + url.PathEscape(identifier)
}

func appendIssueLink(text, link string) string {
	if link == "" {
		return text
	}
	return text + "\n🔗 Issue: " + link
}

func (r *Replier) Reply(ctx context.Context, installation engine.ResolvedInstallation, message channel.InboundMessage, result engine.Result) {
	var text string
	switch result.Outcome {
	case engine.OutcomeNeedsBinding:
		if r.binding == nil || r.appURL == "" {
			return
		}
		token, err := r.binding.Mint(ctx, installation.WorkspaceID, installation.ID, message.Source.SenderID)
		if err != nil {
			return
		}
		text = "👋 Để giao việc cho agent, hãy liên kết tài khoản với Multica:\n" +
			r.appURL + "/channel/bind?token=" + url.QueryEscape(token.Raw) +
			"\n(Link hết hạn sau 15 phút.)"
	case engine.OutcomeAgentOffline:
		text = messageAgentOffline
	case engine.OutcomeAgentArchived:
		text = messageAgentArchived
	case engine.OutcomeFreshPending:
		text = messageFreshPending
	case engine.OutcomeChatStarted:
		text = messageChatStarted
	case engine.OutcomeIssueUsage:
		text = messageIssueUsage
	case engine.OutcomeIngested:
		if result.IssueID.Valid {
			identifier := result.IssueIdentifier
			if identifier == "" {
				identifier = fmt.Sprintf("#%d", result.IssueNumber)
			}
			if result.IssueDuplicate {
				text = "⚠️ Không tạo mới — task " + identifier + " đang tồn tại."
			} else {
				text = "✅ Đã tạo " + identifier + ". Agent đang xử lý."
			}
			text = appendIssueLink(text, issueWebURL(r.appURL, identifier))
		} else {
			text = messageAccepted
		}
	case engine.OutcomeDropped:
		if result.DropReason == engine.DropReasonNonWorkspaceMember {
			text = "Tài khoản Multica đã liên kết không còn là thành viên workspace này."
		}
	}
	if text == "" {
		return
	}
	row, ok := installation.Platform.(db.ChannelInstallation)
	if !ok {
		return
	}
	dedupeKey := "inbound:" + message.MessageID + ":" + string(result.Outcome)
	_, err := r.q.EnqueueChannelExtensionOutbound(ctx, db.EnqueueChannelExtensionOutboundParams{
		InstallationID: row.ID, ChannelChatID: message.Source.ChatID, Text: text, DedupeKey: dedupeKey,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return
	}
}

var _ engine.OutboundReplier = (*Replier)(nil)
