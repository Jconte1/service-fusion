import { NextResponse } from "next/server";

import { sendReadyInvoicesForRun } from "@/lib/service-fusion/send-ready-invoices";

export async function POST(request: Request) {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  try {
    const url = new URL(request.url);
    const runId = url.searchParams.get("runId");
    if (!runId) {
      return NextResponse.json(
        { error: "Missing required query param: runId" },
        { status: 400 },
      );
    }

    const result = await sendReadyInvoicesForRun(runId);
    return NextResponse.json({ ok: true, send: result }, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown send route error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

