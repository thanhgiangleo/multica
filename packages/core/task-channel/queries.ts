import { queryOptions } from "@tanstack/react-query";
import { api } from "../api";

export const taskChannelKeys = {
  all: (wsId: string) => ["task-channel", wsId] as const,
  installations: (wsId: string) =>
    [...taskChannelKeys.all(wsId), "installations"] as const,
};

export const taskChannelInstallationsOptions = (wsId: string) =>
  queryOptions({
    queryKey: taskChannelKeys.installations(wsId),
    queryFn: () => api.listTaskChannelInstallations(wsId),
    enabled: !!wsId,
  });
