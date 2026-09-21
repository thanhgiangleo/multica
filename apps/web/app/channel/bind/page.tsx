import { TaskChannelBindPage } from "@multica/views/task-channel";

export default async function ChannelBindRoute({ searchParams }: { searchParams: Promise<{ token?: string }> }) {
  const params = await searchParams;
  return <TaskChannelBindPage token={params.token ?? null} />;
}
