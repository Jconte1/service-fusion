import {
  fetchClientCredentialsToken,
  refreshAccessToken,
  type ServiceFusionTokenResponse,
} from "./oauth";

const TOKEN_REFRESH_BUFFER_MS = 60_000;

let cachedToken: ServiceFusionTokenResponse | null = null;
let inFlightTokenPromise: Promise<ServiceFusionTokenResponse> | null = null;

function willExpireSoon(token: ServiceFusionTokenResponse): boolean {
  const expiryMs = Date.parse(token.expires_at);
  if (Number.isNaN(expiryMs)) {
    return true;
  }
  return expiryMs - Date.now() <= TOKEN_REFRESH_BUFFER_MS;
}

async function issueFreshToken(): Promise<ServiceFusionTokenResponse> {
  if (cachedToken?.refresh_token) {
    try {
      const refreshed = await refreshAccessToken(cachedToken.refresh_token);
      cachedToken = refreshed;
      return refreshed;
    } catch {
      // Fall through to a new client credentials token if refresh fails.
    }
  }

  const token = await fetchClientCredentialsToken();
  cachedToken = token;
  return token;
}

export async function getValidAccessToken(): Promise<string> {
  if (cachedToken && !willExpireSoon(cachedToken)) {
    return cachedToken.access_token;
  }

  if (!inFlightTokenPromise) {
    inFlightTokenPromise = issueFreshToken().finally(() => {
      inFlightTokenPromise = null;
    });
  }

  const token = await inFlightTokenPromise;
  return token.access_token;
}

export function clearTokenCache(): void {
  cachedToken = null;
}
