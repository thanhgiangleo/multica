// @vitest-environment node

import { describe, expect, it } from "vitest";
import { isPrivateWorkspaceUploadURL } from "./private-upload-url";

describe("isPrivateWorkspaceUploadURL", () => {
  it("matches site-relative workspace upload keys", () => {
    expect(
      isPrivateWorkspaceUploadURL("/uploads/workspaces/ws-1/abc.png"),
    ).toBe(true);
    expect(
      isPrivateWorkspaceUploadURL("/uploads/workspaces/ws-1/abc.png?exp=1&sig=x"),
    ).toBe(true);
  });

  it("matches absolute workspace upload URLs but not signed download URLs", () => {
    expect(
      isPrivateWorkspaceUploadURL(
        "https://api.example.test/uploads/workspaces/ws-1/abc.png",
      ),
    ).toBe(true);
    expect(
      isPrivateWorkspaceUploadURL(
        "https://cdn.example.test/uploads/workspaces/ws-1/abc.png?Signature=fresh",
      ),
    ).toBe(false);
  });

  it("ignores uploads outside the workspace namespace", () => {
    expect(isPrivateWorkspaceUploadURL("/uploads/downloads/installer.exe")).toBe(false);
    expect(isPrivateWorkspaceUploadURL("/uploads/ws-1/abc.png")).toBe(false);
    expect(isPrivateWorkspaceUploadURL("/api/attachments/att-1/download")).toBe(false);
    expect(isPrivateWorkspaceUploadURL("https://cdn.example.test/photo.png")).toBe(false);
    expect(isPrivateWorkspaceUploadURL("")).toBe(false);
  });
});
