import { NextResponse } from "next/server";

import { getDailyInvoicedJobsForDenverDate } from "@/lib/service-fusion/daily-invoiced-jobs";

export async function POST(request: Request) {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  try {
    const url = new URL(request.url);
    const date = url.searchParams.get("date") ?? undefined;
    const result = await getDailyInvoicedJobsForDenverDate({ date });
    return NextResponse.json(result, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown daily extraction error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
