import { NextResponse } from "next/server";

import { fetchClientCredentialsToken } from "@/lib/service-fusion/oauth";

export async function POST() {
  if (process.env.NODE_ENV === "production") {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  try {
    const token = await fetchClientCredentialsToken();
    return NextResponse.json(token, { status: 200 });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown token request error.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
