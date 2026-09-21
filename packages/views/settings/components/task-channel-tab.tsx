"use client";

import { MessageSquareMore } from "lucide-react";
import type { TaskChannelInstallation } from "@multica/core/types";
import { useActorName } from "@multica/core/workspace/hooks";
import { cn } from "@multica/ui/lib/utils";
import { ActorAvatar } from "../../common/actor-avatar";
import { useT } from "../../i18n";
import { SettingsCard, SettingsSection } from "./settings-layout";

export function taskChannelProviderLabel(provider: string): string {
  return provider
    .trim()
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

export function taskChannelIsReportOnly(provider: string): boolean {
  return provider.trim().toLowerCase() === "zalo";
}

export function TaskChannelTab({
  provider,
  installations,
  isLoading,
  isError,
}: {
  provider: string;
  installations: TaskChannelInstallation[];
  isLoading: boolean;
  isError: boolean;
}) {
  const { t } = useT("settings");
  const { getAgentName } = useActorName();
  const reportOnly = taskChannelIsReportOnly(provider);

  if (isError) {
    return (
      <p className="text-body text-muted-foreground">
        {t(($) => $.task_channel.load_failed)}
      </p>
    );
  }
  if (isLoading) {
    return (
      <p className="text-body text-muted-foreground">
        {t(($) => $.task_channel.loading)}
      </p>
    );
  }

  return (
    <div className="space-y-6">
      <p className="max-w-2xl text-caption leading-5 text-muted-foreground">
        {reportOnly
          ? t(($) => $.task_channel.managed_by_extension_report_only)
          : t(($) => $.task_channel.managed_by_extension)}
      </p>
      <SettingsSection title={t(($) => $.task_channel.connected_accounts)}>
        <SettingsCard>
          {installations.map((installation) => {
            const active = installation.status === "active";
            return (
              <div
                key={installation.id}
                className="flex min-h-20 items-center gap-4 px-4 py-4"
              >
                <span
                  aria-hidden="true"
                  className="flex size-11 shrink-0 items-center justify-center rounded-xl border border-surface-border bg-background text-muted-foreground"
                >
                  <MessageSquareMore className="size-5" />
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                    <span className="truncate text-body font-medium text-foreground">
                      {installation.display_name || taskChannelProviderLabel(provider)}
                    </span>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1.5 text-caption",
                        active ? "text-success" : "text-muted-foreground",
                      )}
                    >
                      {active ? (
                        <span aria-hidden="true" className="size-1.5 rounded-full bg-success" />
                      ) : null}
                      {active
                        ? t(($) => $.task_channel.active_badge)
                        : t(($) => $.task_channel.inactive_badge)}
                    </span>
                  </div>
                  <p className="mt-1 text-caption text-muted-foreground">
                    {t(($) => $.task_channel.account_id, {
                      id: installation.account_id,
                    })}
                  </p>
                  <div className="mt-2 flex items-center gap-2 text-caption text-muted-foreground">
                    <ActorAvatar
                      actorType="agent"
                      actorId={installation.agent_id}
                      size="xs"
                      enableHoverCard
                      profileLink
                    />
                    <span>
                      {t(($) => $.task_channel.assigned_agent, {
                        agent: getAgentName(installation.agent_id),
                      })}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </SettingsCard>
      </SettingsSection>
    </div>
  );
}
