type TokenResponse = {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
  error?: string;
  error_description?: string;
};

type TokenCache = {
  accessToken: string;
  expiresAtMs: number;
};

let tokenCache: TokenCache | null = null;

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function cleanBaseUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

export class AcumaticaClient {
  private readonly baseUrl: string;
  private readonly clientId: string;
  private readonly clientSecret: string;
  private readonly username: string;
  private readonly password: string;
  private readonly company: string;
  private readonly invoiceEndpoint: string;

  constructor() {
    this.baseUrl = cleanBaseUrl(requiredEnv("ACUMATICA_BASE_URL"));
    this.clientId = requiredEnv("ACUMATICA_CLIENT_ID");
    this.clientSecret = requiredEnv("ACUMATICA_CLIENT_SECRET");
    this.username = requiredEnv("ACUMATICA_USERNAME");
    this.password = requiredEnv("ACUMATICA_PASSWORD");
    this.company = requiredEnv("ACUMATICA_COMPANY");
    this.invoiceEndpoint = process.env.ACUMATICA_INVOICE_ENDPOINT ?? "SalesInvoice";
  }

  private async getAccessToken(): Promise<string> {
    if (tokenCache && tokenCache.expiresAtMs > Date.now()) {
      return tokenCache.accessToken;
    }

    const body = new URLSearchParams({
      grant_type: "password",
      client_id: this.clientId,
      client_secret: this.clientSecret,
      username: this.username,
      password: this.password,
      scope: "api offline_access",
    });

    const response = await fetch(`${this.baseUrl}/identity/connect/token`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: body.toString(),
    });

    const data = (await response.json()) as TokenResponse;
    if (!response.ok || !data.access_token || !data.expires_in) {
      const reason = data.error_description ?? data.error ?? "Unknown token error.";
      throw new Error(`Acumatica token request failed: ${reason}`);
    }

    tokenCache = {
      accessToken: data.access_token,
      expiresAtMs: Date.now() + Math.max(0, data.expires_in - 30) * 1000,
    };

    return tokenCache.accessToken;
  }

  async putInvoice(payload: unknown): Promise<{ ok: boolean; status: number; body: unknown }> {
    const token = await this.getAccessToken();
    const endpoint = `${this.baseUrl}/entity/ServiceFusion/24.200.001/${this.invoiceEndpoint}`;

    const response = await fetch(endpoint, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
        // Reserved for future tenant-specific routing; currently validated-only.
        "X-Acumatica-Company": this.company,
      },
      body: JSON.stringify(payload),
    });

    const raw = await response.text();
    let body: unknown = raw;
    try {
      body = raw ? (JSON.parse(raw) as unknown) : null;
    } catch {
      body = raw;
    }

    return {
      ok: response.ok,
      status: response.status,
      body,
    };
  }
}
