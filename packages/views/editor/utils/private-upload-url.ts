/**
 * Workspace uploads (`/uploads/workspaces/<ws>/<file>`) are private: the
 * server answers them with a 302 to the auth-gated
 * `/api/attachments/<id>/download` endpoint instead of streaming the object.
 * That redirect is useless to a native `<img>`/`<video>` fetch, which cannot
 * carry the API client's Authorization header, so renderers must prefer the
 * durable attachment endpoint (and let the re-sign hook upgrade it) rather
 * than emitting the storage path.
 */
export function isPrivateWorkspaceUploadURL(rawURL: string): boolean {
  if (!rawURL) return false;
  // The download_url can be a signed CDN URL with this same path; its
  // signature already authorizes the native media request.
  if (/[?&](Signature|X-Amz-Signature|Key-Pair-Id|Expires|X-Amz-Expires)=/i.test(rawURL)) {
    return false;
  }
  const path = (() => {
    if (!/^https?:\/\//i.test(rawURL)) return rawURL.split(/[?#]/, 1)[0] ?? "";
    try {
      return new URL(rawURL).pathname;
    } catch {
      return "";
    }
  })();
  return path.startsWith("/uploads/workspaces/");
}
