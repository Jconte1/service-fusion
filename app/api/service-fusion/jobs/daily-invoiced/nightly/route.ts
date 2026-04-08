import { NextResponse } from "next/server";
import { SfSyncRunStatus } from "@prisma/client";

import { prisma } from "@/lib/prisma";
import { getDailyInvoicedJobsForDenverDate } from "@/lib/service-fusion/daily-invoiced-jobs";
import { persistDbReadyJobs } from "@/lib/service-fusion/persist-db-ready";
import { sendReadyInvoicesForRun } from "@/lib/service-fusion/send-ready-invoices";
import { transformDailyInvoicedJobsToDbReady } from "@/lib/service-fusion/transform-for-db";

const DENVER_TZ = "America/Denver";

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

async function runNightly(request: Request) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const url = new URL(request.url);
  const date = url.searchParams.get("date") ?? getDenverDateString();
  const send = parseSendFlag(url);
  const force = parseForceFlag(url);
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
    const extracted = await getDailyInvoicedJobsForDenverDate({ date });
    const transformed = transformDailyInvoicedJobsToDbReady(extracted);
    const persisted = await persistDbReadyJobs(transformed);
    const sendResult = send ? await sendReadyInvoicesForRun(persisted.runId) : null;

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

