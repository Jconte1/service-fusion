import { getServiceFusionEnv } from "./env";

const SERVICE_FUSION_TOKEN_URL = "https://api.servicefusion.com/oauth/access_token";

export type ServiceFusionTokenResponse = {
  access_token: string;
  token_type: string;
  expires_in: number;
  refresh_token?: string;
  expires_at: string;
};

type ServiceFusionTokenWireResponse = {
  access_token?: string;
  token_type?: string;
  expires_in?: number | string;
  refresh_token?: string;
  error?: string;
  error_description?: string;
};

async function requestToken(
  payload: Record<string, string>,
): Promise<ServiceFusionTokenResponse> {
  const response = await fetch(SERVICE_FUSION_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    cache: "no-store",
  });

  let data: ServiceFusionTokenWireResponse;
  try {
    data = (await response.json()) as ServiceFusionTokenWireResponse;
  } catch {
    throw new Error("Service Fusion token response was not valid JSON.");
  }

  if (!response.ok) {
    const reason =
      data.error_description || data.error || `HTTP ${response.status} ${response.statusText}`;
    throw new Error(`Service Fusion token request failed: ${reason}`);
  }

  const expiresIn =
    typeof data.expires_in === "number" ? data.expires_in : Number.parseInt(data.expires_in ?? "", 10);

  if (!data.access_token || !data.token_type || Number.isNaN(expiresIn)) {
    throw new Error("Service Fusion token response did not include expected token fields.");
  }

  return {
    access_token: data.access_token,
    token_type: data.token_type,
    expires_in: expiresIn,
    refresh_token: data.refresh_token,
    expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
  };
}

export async function fetchClientCredentialsToken(): Promise<ServiceFusionTokenResponse> {
  const { clientId, clientSecret } = getServiceFusionEnv();

  return requestToken({
    grant_type: "client_credentials",
    client_id: clientId,
    client_secret: clientSecret,
  });
}

export async function refreshAccessToken(refreshToken: string): Promise<ServiceFusionTokenResponse> {
  return requestToken({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
  });
}
