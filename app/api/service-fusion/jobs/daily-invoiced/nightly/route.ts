import { NextResponse } from "next/server";
import { SfJobSyncStatus, SfSyncRunStatus } from "@prisma/client";

import { prisma } from "@/lib/prisma";
import { getDailyInvoicedJobsForDenverDate } from "@/lib/service-fusion/daily-invoiced-jobs";
import { sendInvoiceRecapEmail } from "@/lib/service-fusion/invoice-recap-email";
import { persistDbReadyJobs } from "@/lib/service-fusion/persist-db-ready";
import { sendReadyInvoicesForRun } from "@/lib/service-fusion/send-ready-invoices";
import {
  transformDailyInvoicedJobsToDbReady,
  type DbReadyJob,
  type DbReadyJobsResult,
} from "@/lib/service-fusion/transform-for-db";

const DENVER_TZ = "America/Denver";
const DEFAULT_FAILED_RETRY_LOOKBACK_DAYS = 3;

function getDenverDateString(now = new Date()): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: DENVER_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(now);
}

function isAuthorized(request: Request): boolean {
  const configuredSecret =
    process.env.SERVICE_FUSION_CRON_SECRET || process.env.CRON_SECRET || "";
  if (!configuredSecret) return false;

  const authHeader = request.headers.get("authorization") || "";
  const bearerToken = authHeader.toLowerCase().startsWith("bearer ")
    ? authHeader.slice(7).trim()
    : "";
  const cronSecretHeader = request.headers.get("x-cron-secret") || "";
  const url = new URL(request.url);
  const querySecret = url.searchParams.get("secret") || "";

  return (
    bearerToken === configuredSecret ||
    cronSecretHeader === configuredSecret ||
    querySecret === configuredSecret
  );
}

function parseSendFlag(url: URL): boolean {
  const value = (url.searchParams.get("send") ?? "true").toLowerCase();
  return value === "true" || value === "1" || value === "yes";
}

function parseForceFlag(url: URL): boolean {
  const value = (url.searchParams.get("force") ?? "false").toLowerCase();
  return value === "true" || value === "1" || value === "yes";
}

function parseRetryFailedFlag(url: URL): boolean {
  const value = (url.searchParams.get("retryFailed") ?? "true").toLowerCase();
  return value === "true" || value === "1" || value === "yes";
}

function parseRetryFailedLookbackDays(url: URL): number {
  const raw = url.searchParams.get("retryFailedLookbackDays");
  if (!raw) {
    return DEFAULT_FAILED_RETRY_LOOKBACK_DAYS;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 30) {
    throw new Error("Invalid retryFailedLookbackDays value. Expected a number from 0 to 30.");
  }

  return parsed;
}

function parseSkipAlreadySentFlag(url: URL): boolean {
  const value = (url.searchParams.get("skipAlreadySent") ?? "true").toLowerCase();
  return value === "true" || value === "1" || value === "yes";
}

function parseRecapFlag(url: URL): boolean {
  const value = (url.searchParams.get("recap") ?? "true").toLowerCase();
  return value === "true" || value === "1" || value === "yes";
}

function parseJobNumberFilter(url: URL): Set<string> | null {
  const raw = url.searchParams.get("jobNumbers") ?? "";
  const jobNumbers = raw
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return jobNumbers.length > 0 ? new Set(jobNumbers) : null;
}

function parseAdditionalJobIds(url: URL): number[] {
  const raw = url.searchParams.get("jobIds") ?? "";
  const jobIds = raw
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const parsed = jobIds.map((value) => {
    const id = Number(value);
    if (!Number.isSafeInteger(id) || id <= 0) {
      throw new Error(`Invalid jobIds value: ${value}`);
    }
    return id;
  });
  return Array.from(new Set(parsed));
}

function withFilteredJobs(source: DbReadyJobsResult, jobs: DbReadyJob[]): DbReadyJobsResult {
  return {
    ...source,
    stats: {
      ...source.stats,
      processedJobs: jobs.length,
    },
    jobs,
  };
}

async function filterAlreadySentJobs(jobs: DbReadyJob[]): Promise<{
  jobs: DbReadyJob[];
  skippedAlreadySent: Array<{
    serviceFusionJobId: string;
    serviceFusionJobNumber: string | null;
    acumaticaRef: string | null;
  }>;
}> {
  const serviceFusionJobIds = jobs.map((job) => BigInt(job.serviceFusionJobId));
  if (serviceFusionJobIds.length === 0) {
    return { jobs, skippedAlreadySent: [] };
  }

  const alreadySent = await prisma.sfJob.findMany({
    where: {
      serviceFusionJobId: { in: serviceFusionJobIds },
      OR: [{ syncStatus: SfJobSyncStatus.SENT }, { acumaticaRef: { not: null } }],
    },
    select: {
      serviceFusionJobId: true,
      serviceFusionJobNumber: true,
      acumaticaRef: true,
    },
  });

  const alreadySentIds = new Set(
    alreadySent.map((job) => job.serviceFusionJobId.toString()),
  );

  return {
    jobs: jobs.filter((job) => !alreadySentIds.has(String(job.serviceFusionJobId))),
    skippedAlreadySent: alreadySent.map((job) => ({
      serviceFusionJobId: job.serviceFusionJobId.toString(),
      serviceFusionJobNumber: job.serviceFusionJobNumber,
      acumaticaRef: job.acumaticaRef,
    })),
  };
}

async function loadFreshFailedRetryTargets(options: {
  jobNumbers: Set<string> | null;
  lookbackDays: number;
}): Promise<{
  jobIds: number[];
  targets: Array<{
    serviceFusionJobId: string;
    serviceFusionJobNumber: string | null;
    failureReason: string | null;
    updatedAt: Date;
  }>;
  skippedAlreadySent: Array<{
    serviceFusionJobId: string;
    serviceFusionJobNumber: string | null;
    acumaticaRef: string | null;
  }>;
}> {
  const since =
    options.lookbackDays > 0
      ? new Date(Date.now() - options.lookbackDays * 24 * 60 * 60 * 1000)
      : null;

  const failedJobs = await prisma.sfJob.findMany({
    where: {
      syncStatus: SfJobSyncStatus.FAILED,
      acumaticaRef: null,
      ...(since ? { updatedAt: { gte: since } } : {}),
      ...(options.jobNumbers
        ? { serviceFusionJobNumber: { in: Array.from(options.jobNumbers) } }
        : {}),
      events: {
        some: {
          eventType: "ACUMATICA_SEND_FAILED",
        },
      },
    },
    select: {
      serviceFusionJobId: true,
      serviceFusionJobNumber: true,
      failureReason: true,
      updatedAt: true,
    },
    orderBy: [{ updatedAt: "desc" }],
    take: 500,
  });

  const latestByServiceFusionJobId = new Map<string, (typeof failedJobs)[number]>();
  for (const job of failedJobs) {
    const key = job.serviceFusionJobId.toString();
    if (!latestByServiceFusionJobId.has(key)) {
      latestByServiceFusionJobId.set(key, job);
    }
  }

  const candidates = Array.from(latestByServiceFusionJobId.values());
  if (candidates.length === 0) {
    return { jobIds: [], targets: [], skippedAlreadySent: [] };
  }

  const alreadySent = await prisma.sfJob.findMany({
    where: {
      serviceFusionJobId: { in: candidates.map((job) => job.serviceFusionJobId) },
      OR: [{ syncStatus: SfJobSyncStatus.SENT }, { acumaticaRef: { not: null } }],
    },
    select: {
      serviceFusionJobId: true,
      serviceFusionJobNumber: true,
      acumaticaRef: true,
    },
  });
  const alreadySentIds = new Set(alreadySent.map((job) => job.serviceFusionJobId.toString()));

  const targets = candidates.filter(
    (job) => !alreadySentIds.has(job.serviceFusionJobId.toString()),
  );
  return {
    jobIds: targets.map((job) => Number(job.serviceFusionJobId)),
    targets: targets.map((job) => ({
      serviceFusionJobId: job.serviceFusionJobId.toString(),
      serviceFusionJobNumber: job.serviceFusionJobNumber,
      failureReason: job.failureReason,
      updatedAt: job.updatedAt,
    })),
    skippedAlreadySent: alreadySent.map((job) => ({
      serviceFusionJobId: job.serviceFusionJobId.toString(),
      serviceFusionJobNumber: job.serviceFusionJobNumber,
      acumaticaRef: job.acumaticaRef,
    })),
  };
}

async function runNightly(request: Request) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const date = url.searchParams.get("date") ?? getDenverDateString();
  const send = parseSendFlag(url);
  const force = parseForceFlag(url);
  const retryFailed = parseRetryFailedFlag(url);
  const retryFailedLookbackDays = parseRetryFailedLookbackDays(url);
  const skipAlreadySent = parseSkipAlreadySentFlag(url);
  const recap = parseRecapFlag(url);
  const jobNumberFilter = parseJobNumberFilter(url);
  const additionalJobIds = parseAdditionalJobIds(url);
  const startedAt = Date.now();

  if (!force) {
    const existing = await prisma.sfSyncRun.findFirst({
      where: {
        runType: "DAILY_INVOICED",
        denverDate: date,
        status: { in: [SfSyncRunStatus.SUCCESS, SfSyncRunStatus.PARTIAL] },
      },
      orderBy: { startedAt: "desc" },
    });

    if (existing) {
      return NextResponse.json(
        {
          ok: true,
          skipped: true,
          reason: "A successful/partial run already exists for this Denver date.",
          denverDate: date,
          existingRun: {
            runId: existing.id,
            status: existing.status,
            startedAt: existing.startedAt,
            finishedAt: existing.finishedAt,
            fetchedCount: existing.fetchedCount,
            processedCount: existing.processedCount,
            sentSuccessCount: existing.sentSuccessCount,
            sentFailedCount: existing.sentFailedCount,
          },
        },
        { status: 200 },
      );
    }
  }

  try {
    const freshRetryTargets = retryFailed
      ? await loadFreshFailedRetryTargets({
          jobNumbers: jobNumberFilter,
          lookbackDays: retryFailedLookbackDays,
        })
      : { jobIds: [], targets: [], skippedAlreadySent: [] };
    const serviceFusionJobIds = Array.from(
      new Set([...additionalJobIds, ...freshRetryTargets.jobIds]),
    );

    const extracted = await getDailyInvoicedJobsForDenverDate({
      date,
      additionalJobIds: serviceFusionJobIds,
    });
    const transformedRaw = transformDailyInvoicedJobsToDbReady(extracted);
    const jobNumberFiltered = jobNumberFilter
      ? transformedRaw.jobs.filter(
          (job) =>
            job.serviceFusionJobNumber &&
            jobNumberFilter.has(job.serviceFusionJobNumber),
        )
      : transformedRaw.jobs;
    const alreadySentFilter = skipAlreadySent
      ? await filterAlreadySentJobs(jobNumberFiltered)
      : { jobs: jobNumberFiltered, skippedAlreadySent: [] };
    const transformed = withFilteredJobs(transformedRaw, alreadySentFilter.jobs);
    const persisted = await persistDbReadyJobs(transformed);
    const sendResult = send ? await sendReadyInvoicesForRun(persisted.runId) : null;
    const recapEmail = sendResult && recap
      ? await sendInvoiceRecapEmail(extracted.window.date, sendResult)
      : null;

    return NextResponse.json(
      {
        ok: true,
        skipped: false,
        denverDate: extracted.window.date,
        durationMs: Date.now() - startedAt,
        window: extracted.window,
        extractStats: extracted.stats,
        transformStats: transformed.stats,
        persist: persisted,
        send: sendResult,
        retryFailed,
        retryFailedMode: "fresh-service-fusion-fetch",
        retryFailedLookbackDays,
        freshRetryTargets: freshRetryTargets.targets,
        skipAlreadySent,
        skippedAlreadySent: [
          ...alreadySentFilter.skippedAlreadySent,
          ...freshRetryTargets.skippedAlreadySent,
        ],
        jobNumbers: jobNumberFilter ? Array.from(jobNumberFilter) : null,
        additionalJobIds: serviceFusionJobIds,
        recapEmail,
      },
      { status: 200 },
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Nightly run failed.";
    return NextResponse.json(
      {
        ok: false,
        denverDate: date,
        durationMs: Date.now() - startedAt,
        error: message,
      },
      { status: 500 },
    );
  }
}

export async function GET(request: Request) {
  return runNightly(request);
}

export async function POST(request: Request) {
  return runNightly(request);
}
