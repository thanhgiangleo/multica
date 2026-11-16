package telegram

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// otpCommandName is the zbs admin command that discloses the newest still-valid
// verification code. Telegram bot commands cannot contain '-', so the
// hyphenated form the operator asked for is spelled with an underscore.
const otpCommandName = "/get_multica_otp"

// otpAdminEnv lists the Telegram user ids allowed to run the command, comma
// separated. Unset or empty disables the command entirely: an admin-only
// disclosure must be opt-in per deployment, never on by default.
const otpAdminEnv = "MULTICA_OTP_ADMIN_TELEGRAM_IDS"

// otpAdminIDs parses otpAdminEnv. Malformed entries are skipped rather than
// widening access.
func otpAdminIDs() map[int64]bool {
	raw := strings.TrimSpace(os.Getenv(otpAdminEnv))
	if raw == "" {
		return nil
	}
	ids := make(map[int64]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			continue
		}
		ids[id] = true
	}
	return ids
}

// isOTPCommand reports whether the message body is the admin OTP command. The
// command must be the first token; a "/cmd@BotName" routing suffix is ignored.
func isOTPCommand(text, botUsername string) bool {
	fields := strings.Fields(normalizeText(text, botUsername))
	if len(fields) == 0 {
		return false
	}
	first := fields[0]
	if at := strings.IndexByte(first, '@'); at >= 0 {
		first = first[:at]
	}
	return strings.EqualFold(first, otpCommandName)
}

// interceptOTPCommand handles /get_multica_otp. It returns true when the update
// was consumed, so the message never reaches the agent pipeline.
//
// The command is admin-only and private-chat-only: a verification code is a
// login credential, so it is never posted into a group and never answered for
// a sender outside the allowlist. Every disclosure is logged for audit.
func (c *telegramChannel) interceptOTPCommand(ctx context.Context, m *Message) bool {
	if m == nil || m.From == nil || m.From.IsBot || m.From.ID == c.botID {
		return false
	}
	text := m.Text
	if text == "" {
		text = m.Caption
	}
	if !isOTPCommand(text, c.botUsername) {
		return false
	}

	if m.Chat.Type != "private" {
		c.replyOTP(ctx, m, "⚠️ Lệnh này chỉ dùng trong chat riêng với bot.")
		return true
	}
	if !otpAdminIDs()[m.From.ID] {
		c.replyOTP(ctx, m, "⛔ Bạn không có quyền dùng lệnh này.")
		return true
	}
	if c.pool == nil {
		c.replyOTP(ctx, m, "⚠️ Lệnh chưa được cấu hình (thiếu kết nối DB).")
		return true
	}

	email, code, expiresAt, ok, err := c.latestOTP(ctx)
	if err != nil {
		c.logger.WarnContext(ctx, "telegram: otp lookup failed", "bot_id", c.botID, "error", err)
		c.replyOTP(ctx, m, "⚠️ Không truy vấn được OTP. Thử lại sau.")
		return true
	}
	if !ok {
		c.replyOTP(ctx, m, "Không có OTP còn hiệu lực.")
		return true
	}

	c.logger.InfoContext(ctx, "telegram: otp disclosed",
		"bot_id", c.botID,
		"admin_telegram_id", m.From.ID,
		"email", email,
	)
	c.replyOTP(ctx, m, fmt.Sprintf("📧 %s\n🔑 %s\n⏰ Hết hạn: %s (giờ VN)",
		email, code, expiresAt.In(vietnamLocation()).Format("2006-01-02 15:04:05")))
	return true
}

// latestOTP returns the newest unused, unexpired verification code across all
// accounts. ok=false means there is none.
func (c *telegramChannel) latestOTP(ctx context.Context) (email, code string, expiresAt time.Time, ok bool, err error) {
	row := c.pool.QueryRow(ctx, `
		SELECT email, code, expires_at
		FROM verification_code
		WHERE used = false AND expires_at > now()
		ORDER BY created_at DESC
		LIMIT 1`)
	err = row.Scan(&email, &code, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", time.Time{}, false, nil
	}
	if err != nil {
		return "", "", time.Time{}, false, err
	}
	return email, code, expiresAt, true, nil
}

// vietnamLocation falls back to a fixed +07 offset when the tz database is
// unavailable, so the reply never fails on a missing zone.
func vietnamLocation() *time.Location {
	if loc, err := time.LoadLocation("Asia/Ho_Chi_Minh"); err == nil {
		return loc
	}
	return time.FixedZone("ICT", 7*60*60)
}

// replyOTP posts a plain-text reply, quoting the command message so the answer
// stays unambiguous in a busy chat. Best-effort: failures are logged only.
func (c *telegramChannel) replyOTP(ctx context.Context, m *Message, text string) {
	if _, err := c.api.SendMessage(ctx, sendMessageParams{
		ChatID:          m.Chat.ID,
		Text:            text,
		MessageThreadID: m.MessageThreadID,
		ReplyParameters: &replyParameters{
			MessageID:                m.MessageID,
			AllowSendingWithoutReply: true,
		},
	}); err != nil {
		c.logger.WarnContext(ctx, "telegram: otp reply failed", "bot_id", c.botID, "error", err)
	}
}
