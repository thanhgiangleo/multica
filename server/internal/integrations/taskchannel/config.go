// Package taskchannel connects out-of-process messaging extensions to the
// generic Multica channel engine. Provider SDKs, credentials and wire formats
// intentionally do not belong in this package.
package taskchannel

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

const TypeExternal channel.Type = "external"

const OriginExternalChat = "external_chat"

const (
	liteLLMKeyProvisionerName = "LiteLLM Key Provisioner"
	liteLLMKeyRouteSuffix     = ":intent:litellm-key-provisioner"
)

type installationConfig struct {
	AppID       string `json:"app_id"`
	Provider    string `json:"provider"`
	AccountID   string `json:"account_id"`
	DisplayName string `json:"display_name"`
}

// InstallationIsReportOnly identifies transports that can accept a task and
// later deliver its terminal report, but cannot sustain an interactive
// clarification loop. Keep this provider policy at the task-channel boundary:
// the shared channel engine and first-party chat integrations must retain their
// conversational behaviour.
func InstallationIsReportOnly(raw json.RawMessage) bool {
	config, err := decodeInstallationConfig(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(config.Provider), "zalo")
}

func isLiteLLMKeyProvisionRequest(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	hasAction := strings.Contains(text, "tạo") || strings.Contains(text, "tao") ||
		strings.Contains(text, "cấp") || strings.Contains(text, "cap") ||
		strings.Contains(text, "create") || strings.Contains(text, "generate") ||
		strings.Contains(text, "provision")
	hasLLM := strings.Contains(text, "litellm") || strings.Contains(text, "llm")
	return hasAction && hasLLM && strings.Contains(text, "key")
}

func encodeInstallationConfig(pluginInstallationID, provider, accountID, displayName string) ([]byte, error) {
	provider = strings.TrimSpace(provider)
	accountID = strings.TrimSpace(accountID)
	displayName = strings.TrimSpace(displayName)
	if pluginInstallationID == "" || provider == "" || accountID == "" || displayName == "" {
		return nil, errors.New("task channel installation fields must not be empty")
	}
	encoded, err := json.Marshal(installationConfig{
		AppID: pluginInstallationID, Provider: provider, AccountID: accountID, DisplayName: displayName,
	})
	if err != nil {
		return nil, fmt.Errorf("encode task channel installation: %w", err)
	}
	return encoded, nil
}

func decodeInstallationConfig(raw json.RawMessage) (installationConfig, error) {
	var config installationConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return config, fmt.Errorf("decode task channel installation: %w", err)
	}
	if config.AppID == "" || config.Provider == "" {
		return config, errors.New("task channel installation is incomplete")
	}
	return config, nil
}
