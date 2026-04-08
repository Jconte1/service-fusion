import { Prisma, SfJobSyncStatus, type SfJob } from "@prisma/client";

import { AcumaticaClient } from "@/lib/acumatica/client";
import {
  mapSfJobToAcumaticaInvoicePayload,
  type SendableSfJob,
} from "@/lib/acumatica/map-invoice-payload";
import { prisma } from "@/lib/prisma";
import {
  sendSalesInvoiceViaQueue,
  shouldUseQueueForInvoiceSend,
} from "@/lib/service-fusion/queue-client";

type CandidateSource = "CURRENT_RUN_READY" | "RETRY_FAILED_SEND";

type JobCandidate = {
  source: CandidateSource;
  job: SendableSfJob;
};

export type SendReadyInvoicesResult = {
  runId: string;
  stats: {
    currentRunReady: number;
    retryFailedSend: number;
    attempted: number;
    sent: number;
    failed: number;
  };
  sentJobs: Array<{
    jobId: string;
    serviceFusionJobId: string;
    serviceFusionJobNumber: string | null;
    source: CandidateSource;
    acumaticaRef: string | null;
  }>;
  failedJobs: Array<{
    jobId: string;
    serviceFusionJobId: string;
    serviceFusionJobNumber: string | null;
    source: CandidateSource;
    reason: string;
    acumatica: {
      status: number | null;
      message: string | null;
      fieldErrors: Array<{
        path: string;
        message: string;
      }>;
      response: unknown;
    };
  }>;
};

function jobIdentity(job: SfJob): string {
  return `${job.serviceFusionJobId.toString()}::${job.serviceFusionUpdatedAt?.toISOString() ?? "null"}`;
}

function extractAcumaticaRef(responseBody: unknown): string | null {
  if (!responseBody || typeof responseBody !== "object") {
    return null;
  }
  const body = responseBody as Record<string, unknown>;
  const candidates = [
    body.RefNbr,
    body.ReferenceNbr,
    body.InvoiceNbr,
    body.InvoiceNumber,
  ];

  for (const candidate of candidates) {
    if (candidate && typeof candidate === "object" && "value" in candidate) {
      const value = (candidate as { value?: unknown }).value;
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
  }

  return null;
}

function toJsonValue(value: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(value ?? null)) as Prisma.InputJsonValue;
}

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function extractAcumaticaErrorMessage(responseBody: unknown): string | null {
  if (!responseBody || typeof responseBody !== "object") {
    return asNonEmptyString(responseBody);
  }

  const body = responseBody as Record<string, unknown>;
  const directCandidates = [
    body.message,
    body.error,
    body.error_description,
    body.exceptionMessage,
    body.title,
    body.detail,
  ];
  for (const candidate of directCandidates) {
    const text = asNonEmptyString(candidate);
    if (text) {
      return text;
    }
  }

  const modelState = body.ModelState;
  if (modelState && typeof modelState === "object") {
    const values = Object.values(modelState as Record<string, unknown>);
    for (const entry of values) {
      if (Array.isArray(entry)) {
        for (const item of entry) {
          const text = asNonEmptyString(item);
          if (text) {
            return text;
          }
        }
      } else {
        const text = asNonEmptyString(entry);
        if (text) {
          return text;
        }
      }
    }
  }

  return null;
}

function collectAcumaticaFieldErrors(
  node: unknown,
  path = "$",
  acc: Array<{ path: string; message: string }> = [],
): Array<{ path: string; message: string }> {
  if (!node || typeof node !== "object") {
    return acc;
  }

  if (Array.isArray(node)) {
    for (let i = 0; i < node.length; i += 1) {
      collectAcumaticaFieldErrors(node[i], `${path}[${i}]`, acc);
    }
    return acc;
  }

  const obj = node as Record<string, unknown>;
  const maybeError = asNonEmptyString(obj.error);
  if (maybeError) {
    acc.push({ path, message: maybeError });
  }

  for (const [key, value] of Object.entries(obj)) {
    collectAcumaticaFieldErrors(value, `${path}.${key}`, acc);
  }

  return acc;
}

function buildAcumaticaFailureLog(status: number | null, responseBody: unknown): {
  status: number | null;
  message: string | null;
  fieldErrors: Array<{ path: string; message: string }>;
  response: unknown;
} {
  const message = extractAcumaticaErrorMessage(responseBody);
  const fieldErrors = collectAcumaticaFieldErrors(responseBody);
  return {
    status,
    message,
    fieldErrors,
    response: responseBody,
  };
}

async function loadCandidates(runId: string): Promise<{
  currentRunReady: JobCandidate[];
  retryFailed: JobCandidate[];
}> {
  const currentRunReadyJobs = await prisma.sfJob.findMany({
    where: {
      runId,
      syncStatus: SfJobSyncStatus.READY,
    },
    include: {
      lines: true,
      taxDetails: true,
    },
    orderBy: [{ serviceFusionJobId: "asc" }],
  });

  const retryFailedJobs = await prisma.sfJob.findMany({
    where: {
      syncStatus: SfJobSyncStatus.FAILED,
      acumaticaRef: null,
      runId: { not: runId },
      events: {
        some: {
          eventType: "ACUMATICA_SEND_FAILED",
        },
      },
    },
    include: {
      lines: true,
      taxDetails: true,
    },
    orderBy: [{ updatedAt: "asc" }],
    take: 500,
  });

  return {
    currentRunReady: currentRunReadyJobs.map((job) => ({
      source: "CURRENT_RUN_READY" as const,
      job,
    })),
    retryFailed: retryFailedJobs.map((job) => ({
      source: "RETRY_FAILED_SEND" as const,
      job,
    })),
  };
}

async function markSent(
  jobId: string,
  acumaticaRef: string | null,
  responseBody: unknown,
  requestPayload: unknown,
): Promise<void> {
  await prisma.sfJob.update({
    where: { id: jobId },
    data: {
      syncStatus: SfJobSyncStatus.SENT,
      failureReason: null,
      acumaticaRef,
      updatedAt: new Date(),
    },
  });

  await prisma.sfJobEvent.create({
    data: {
      jobId,
      eventType: "ACUMATICA_SEND_SUCCESS",
      message: acumaticaRef
        ? `Invoice sent to Acumatica successfully. Ref: ${acumaticaRef}`
        : "Invoice sent to Acumatica successfully.",
      detailsJson: toJsonValue({
        requestPayload,
        acumaticaRef,
        response: responseBody,
      }),
    },
  });
}

async function markFailed(
  jobId: string,
  reason: string,
  details: unknown,
  requestPayload: unknown,
): Promise<void> {
  const detailObject =
    details && typeof details === "object"
      ? (details as Record<string, unknown>)
      : { details };

  await prisma.sfJob.update({
    where: { id: jobId },
    data: {
      syncStatus: SfJobSyncStatus.FAILED,
      failureReason: reason,
      updatedAt: new Date(),
    },
  });

  await prisma.sfJobEvent.create({
    data: {
      jobId,
      eventType: "ACUMATICA_SEND_FAILED",
      message: reason,
      detailsJson: toJsonValue({
        ...detailObject,
        requestPayload,
      }),
    },
  });
}

async function markDeferred(
  jobId: string,
  reason: string,
  details: unknown,
  requestPayload: unknown,
): Promise<void> {
  const detailObject =
    details && typeof details === "object"
      ? (details as Record<string, unknown>)
      : { details };

  await prisma.sfJobEvent.create({
    data: {
      jobId,
      eventType: "ACUMATICA_SEND_DEFERRED",
      message: reason,
      detailsJson: toJsonValue({
        ...detailObject,
        requestPayload,
      }),
    },
  });
}

export async function sendReadyInvoicesForRun(runId: string): Promise<SendReadyInvoicesResult> {
  const useQueue = shouldUseQueueForInvoiceSend();
  const client = useQueue ? null : new AcumaticaClient();
  const candidates = await loadCandidates(runId);

  const deduped = new Map<string, JobCandidate>();
  for (const candidate of [...candidates.currentRunReady, ...candidates.retryFailed]) {
    deduped.set(jobIdentity(candidate.job), candidate);
  }

  const queue = [...deduped.values()];
  let sent = 0;
  let failed = 0;
  let currentRunSent = 0;
  let currentRunFailed = 0;
  const sentJobs: SendReadyInvoicesResult["sentJobs"] = [];
  const failedJobs: SendReadyInvoicesResult["failedJobs"] = [];

  for (const candidate of queue) {
    const { job } = candidate;

    try {
      const payload = mapSfJobToAcumaticaInvoicePayload(job);
      const response = useQueue
        ? await sendSalesInvoiceViaQueue(payload as Record<string, unknown>)
        : await client!.putInvoice(payload);

      if (!response.ok) {
        const failureLog = buildAcumaticaFailureLog(response.status, response.body);
        const apiMessage = failureLog.message;
        const statusLabel =
          typeof response.status === "number"
            ? `HTTP ${response.status}`
            : "queue job failed";
        const reason = apiMessage
          ? `Acumatica invoice PUT failed (${statusLabel}): ${apiMessage}`
          : `Acumatica invoice PUT failed (${statusLabel}).`;
        await markFailed(job.id, reason, {
          response: response.body,
          source: candidate.source,
          runId,
        }, payload);
        failed += 1;
        if (job.runId === runId) {
          currentRunFailed += 1;
        }
        failedJobs.push({
          jobId: job.id,
          serviceFusionJobId: job.serviceFusionJobId.toString(),
          serviceFusionJobNumber: job.serviceFusionJobNumber,
          source: candidate.source,
          reason,
          acumatica: failureLog,
        });
        continue;
      }

      const acumaticaRef = extractAcumaticaRef(response.body);
      await markSent(job.id, acumaticaRef, {
        response: response.body,
        source: candidate.source,
        runId,
      }, payload);
      sent += 1;
      if (job.runId === runId) {
        currentRunSent += 1;
      }
      sentJobs.push({
        jobId: job.id,
        serviceFusionJobId: job.serviceFusionJobId.toString(),
        serviceFusionJobNumber: job.serviceFusionJobNumber,
        source: candidate.source,
        acumaticaRef,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown send error.";
      const isQueuePollTimeout = message.includes("Queue polling timed out");
      if (isQueuePollTimeout) {
        await markDeferred(
          job.id,
          message,
          {
            source: candidate.source,
            runId,
          },
          null,
        );
        continue;
      }
      const failureLog = buildAcumaticaFailureLog(null, {
        error: message,
      });
      await markFailed(job.id, message, {
        source: candidate.source,
        runId,
      }, null);
      failed += 1;
      if (job.runId === runId) {
        currentRunFailed += 1;
      }
      failedJobs.push({
        jobId: job.id,
        serviceFusionJobId: job.serviceFusionJobId.toString(),
        serviceFusionJobNumber: job.serviceFusionJobNumber,
        source: candidate.source,
        reason: message,
        acumatica: failureLog,
      });
    }
  }

  await prisma.sfSyncRun.update({
    where: { id: runId },
    data: {
      sentSuccessCount: currentRunSent,
      sentFailedCount: currentRunFailed,
      errorSummary:
        currentRunFailed > 0 ? `${currentRunFailed} invoices failed during Acumatica send.` : null,
    },
  });

  return {
    runId,
    stats: {
      currentRunReady: candidates.currentRunReady.length,
      retryFailedSend: candidates.retryFailed.length,
      attempted: queue.length,
      sent,
      failed,
    },
    sentJobs,
    failedJobs,
  };
}
