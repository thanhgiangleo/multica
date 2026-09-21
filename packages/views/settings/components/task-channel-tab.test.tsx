import { describe, expect, it } from "vitest";
import {
  taskChannelIsReportOnly,
  taskChannelProviderLabel,
} from "./task-channel-tab";

describe("taskChannelProviderLabel", () => {
  it("turns provider identifiers into integration labels", () => {
    expect(taskChannelProviderLabel("zalo")).toBe("Zalo");
    expect(taskChannelProviderLabel("mattermost_cloud")).toBe("Mattermost Cloud");
  });
});

describe("taskChannelIsReportOnly", () => {
  it("limits one-shot handling to Zalo", () => {
    expect(taskChannelIsReportOnly(" ZALO ")).toBe(true);
    expect(taskChannelIsReportOnly("mattermost_cloud")).toBe(false);
  });
});
