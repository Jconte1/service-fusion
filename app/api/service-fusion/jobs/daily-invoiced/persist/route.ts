import { NextResponse } from "next/server";

import { getDailyInvoicedJobsForDenverDate } from "@/lib/service-fusion/daily-invoiced-jobs";
import { persistDbReadyJobs } from "@/lib/service-fusion/persist-db-ready";
import { sendReadyInvoicesForRun } from "@/lib/service-fusion/send-ready-invoices";
import { transformDailyInvoicedJobsToDbReady } from "@/lib/service-fusion/transform-for-db";

export async function POST(request: Request) {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  try {
    const url = new URL(request.url);
    const date = url.searchParams.get("date") ?? undefined;
    const send = (url.searchParams.get("send") ?? "").toLowerCase() === "true";

    const extracted = await getDailyInvoicedJobsForDenverDate({ date });
    const transformed = transformDailyInvoicedJobsToDbReady(extracted);
    const persisted = await persistDbReadyJobs(transformed);
    const sendResult = send ? await sendReadyInvoicesForRun(persisted.runId) : null;

    return NextResponse.json(
      {
        ok: true,
        window: extracted.window,
        extractStats: extracted.stats,
        transformStats: transformed.stats,
        persist: persisted,
        send: sendResult,
      },
      { status: 200 },
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown persistence route error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
