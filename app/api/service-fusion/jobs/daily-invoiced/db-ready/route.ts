import { NextResponse } from "next/server";

import { getDailyInvoicedJobsForDenverDate } from "@/lib/service-fusion/daily-invoiced-jobs";
import { transformDailyInvoicedJobsToDbReady } from "@/lib/service-fusion/transform-for-db";

export async function POST(request: Request) {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  try {
    const url = new URL(request.url);
    const date = url.searchParams.get("date") ?? undefined;

    const extracted = await getDailyInvoicedJobsForDenverDate({ date });
    const transformed = transformDailyInvoicedJobsToDbReady(extracted);

    return NextResponse.json(transformed, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown DB-ready transform error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
