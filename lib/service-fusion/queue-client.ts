type QueueJobStatus = "queued" | "processing" | "succeeded" | "failed";

type QueueJobResponse = {
  jobId: string;
  type: string;
  status: QueueJobStatus;
  result: unknown;
  error: string | null;
};

export type QueueInvoiceSendResponse = {
  ok: boolean;
  status: number | null;
  body: unknown;
};

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getQueueBaseUrl(): string {
  return requireEnv("MLD_QUEUE_BASE_URL").replace(/\/+$/, "");
}

function getQueueToken(): string {
  return requireEnv("MLD_QUEUE_TOKEN");
}

function getPollIntervalMs(): number {
  const raw = Number(process.env.MLD_QUEUE_POLL_INTERVAL_MS ?? 1500);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 1500;
}

function getPollTimeoutMs(): number {
  const raw = Number(process.env.MLD_QUEUE_POLL_TIMEOUT_MS ?? 600000);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 600000;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestQueueJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${getQueueBaseUrl()}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${getQueueToken()}`,
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });

  const raw = await response.text();
  let parsed: unknown = null;
  try {
    parsed = raw ? (JSON.parse(raw) as unknown) : null;
  } catch {
    parsed = raw;
  }

  if (!response.ok) {
    const message =
      parsed && typeof parsed === "object" && "error" in parsed
        ? String((parsed as { error?: unknown }).error ?? "")
        : response.statusText;
    throw new Error(`Queue request failed (${response.status}): ${message || raw || "Unknown error"}`);
  }

  return parsed as T;
}

function normalizeQueueResult(result: unknown): QueueInvoiceSendResponse {
  if (!result || typeof result !== "object") {
    return { ok: true, status: 200, body: result };
  }

  const resultObject = result as Record<string, unknown>;
  const status =
    typeof resultObject.status === "number" && Number.isFinite(resultObject.status)
      ? resultObject.status
      : 200;
  const body = "body" in resultObject ? resultObject.body : result;
  return { ok: status >= 200 && status < 300, status, body };
}

export function shouldUseQueueForInvoiceSend(): boolean {
  return (process.env.SERVICE_FUSION_USE_QUEUE ?? "true").toLowerCase() === "true";
}

export async function sendSalesInvoiceViaQueue(
  payload: Record<string, unknown>,
): Promise<QueueInvoiceSendResponse> {
  const enqueue = await requestQueueJson<{ jobId: string }>("/api/erp/jobs/sales-invoices", {
    method: "POST",
    body: JSON.stringify({ payload }),
  });

  const deadline = Date.now() + getPollTimeoutMs();
  const pollIntervalMs = getPollIntervalMs();

  while (Date.now() < deadline) {
    const job = await requestQueueJson<QueueJobResponse>(`/api/erp/jobs/${enqueue.jobId}`, {
      method: "GET",
    });

    if (job.status === "succeeded") {
      return normalizeQueueResult(job.result);
    }

    if (job.status === "failed") {
      return {
        ok: false,
        status: null,
        body: {
          error: job.error ?? "Queue job failed.",
          jobId: job.jobId,
          type: job.type,
        },
      };
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(
    `Queue polling timed out before invoice job completed. queueJobId=${enqueue.jobId}`,
  );
}
