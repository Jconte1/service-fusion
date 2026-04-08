import { clearTokenCache, getValidAccessToken } from "./token-manager";

const SERVICE_FUSION_API_BASE_URL = "https://api.servicefusion.com/v1";

function buildUrl(path: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }

  if (path.startsWith("/")) {
    return `${SERVICE_FUSION_API_BASE_URL}${path}`;
  }

  return `${SERVICE_FUSION_API_BASE_URL}/${path}`;
}

function withBearerHeader(headers: HeadersInit | undefined, accessToken: string): Headers {
  const merged = new Headers(headers);
  merged.set("Authorization", `Bearer ${accessToken}`);
  return merged;
}

export async function serviceFusionFetch(path: string, init?: RequestInit): Promise<Response> {
  const url = buildUrl(path);

  const token = await getValidAccessToken();
  const firstResponse = await fetch(url, {
    ...init,
    headers: withBearerHeader(init?.headers, token),
    cache: init?.cache ?? "no-store",
  });

  if (firstResponse.status !== 401) {
    return firstResponse;
  }

  clearTokenCache();
  const retriedToken = await getValidAccessToken();
  return fetch(url, {
    ...init,
    headers: withBearerHeader(init?.headers, retriedToken),
    cache: init?.cache ?? "no-store",
  });
}

export async function serviceFusionJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await serviceFusionFetch(path, init);

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `Service Fusion API request failed: HTTP ${response.status} ${response.statusText}. ${body}`,
    );
  }

  return (await response.json()) as T;
}
