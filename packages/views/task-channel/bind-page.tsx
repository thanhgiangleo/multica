"use client";

import { useEffect, useState } from "react";
import { api } from "@multica/core/api";
import { useAuthStore } from "@multica/core/auth";
import { Button } from "@multica/ui/components/ui/button";
import { Card, CardContent } from "@multica/ui/components/ui/card";
import { useNavigation } from "../navigation";
import { useT } from "../i18n";

type State = "idle" | "redeeming" | "needs-auth" | "done" | "expired" | "error";

export function TaskChannelBindPage({ token }: { token: string | null }) {
  const { t } = useT("common");
  const user = useAuthStore((state) => state.user);
  const authLoading = useAuthStore((state) => state.isLoading);
  const navigation = useNavigation();
  const [state, setState] = useState<State>("idle");

  useEffect(() => {
    if (!token) {
      setState("expired");
      return;
    }
    if (authLoading) return;
    if (!user) {
      setState("needs-auth");
      return;
    }
    if (state !== "idle" && state !== "needs-auth") return;
    setState("redeeming");
    void api.redeemTaskChannelBindingToken(token).then((response) => {
      setState(response.channel_user_id ? "done" : "error");
    }).catch((error: unknown) => {
      const message = error instanceof Error ? error.message.toLowerCase() : "";
      setState(message.includes("expired") || message.includes("410") ? "expired" : "error");
    });
  }, [authLoading, state, token, user]);

  const retryPath = `/channel/bind?token=${encodeURIComponent(token ?? "")}`;
  return (
    <div className="mx-auto flex min-h-screen max-w-md items-center p-6">
      <Card className="w-full">
        <CardContent className="space-y-4" aria-busy={state === "redeeming"}>
          <h1 className="text-title font-semibold">{t(($) => $.channel_bind.page_title)}</h1>
          {state === "idle" || state === "redeeming" ? (
            <p className="text-body text-muted-foreground">{t(($) => $.channel_bind.redeeming)}</p>
          ) : state === "needs-auth" ? (
            <>
              <p className="text-body text-muted-foreground">{t(($) => $.channel_bind.needs_auth)}</p>
              <Button size="sm" onClick={() => navigation.push(`/login?next=${encodeURIComponent(retryPath)}`)}>
                {t(($) => $.channel_bind.sign_in)}
              </Button>
            </>
          ) : state === "done" ? (
            <p className="text-body font-medium">{t(($) => $.channel_bind.done)}</p>
          ) : (
            <>
              <p className="text-body font-medium">{t(($) => $.channel_bind.error_title)}</p>
              <p className="text-caption text-muted-foreground">
                {state === "expired" ? t(($) => $.channel_bind.expired) : t(($) => $.channel_bind.error)}
              </p>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
