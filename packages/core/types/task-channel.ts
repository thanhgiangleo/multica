export interface TaskChannelInstallation {
  id: string;
  workspace_id: string;
  agent_id: string;
  provider: string;
  account_id: string;
  display_name: string;
  status: string;
}

export interface ListTaskChannelInstallationsResponse {
  installations: TaskChannelInstallation[];
}

export interface RedeemTaskChannelBindingTokenResponse {
  workspace_id: string;
  installation_id: string;
  channel_user_id: string;
}
