import type { SendReadyInvoicesResult } from "@/lib/service-fusion/send-ready-invoices";

type GraphConfig = {
  tenantId: string;
  clientId: string;
  clientSecret: string;
  fromEmail: string;
  toEmails: string[];
  ccEmails: string[];
};

export type InvoiceRecapEmailResult = {
  sent: boolean;
  skippedReason: string | null;
  error: string | null;
  subject: string;
  toEmails: string[];
  ccEmails: string[];
  successCount: number;
  failureCount: number;
};

type FailedLineError = {
  serviceFusionJobNumber: string;
  lineNumber: string;
  inventoryId: string;
  field: string;
  message: string;
};

function optionalEnv(name: string): string | null {
  const value = process.env[name]?.trim();
  return value ? value : null;
}

function parseEmailList(value: string | null): string[] {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((email) => email.trim())
    .filter(Boolean);
}

function getGraphConfig(): { config: GraphConfig | null; reason: string | null } {
  const tenantId = optionalEnv("MS_GRAPH_TENANT_ID");
  const clientId = optionalEnv("MS_GRAPH_CLIENT_ID");
  const clientSecret = optionalEnv("MS_GRAPH_CLIENT_SECRET");
  const fromEmail = optionalEnv("MS_GRAPH_FROM_EMAIL");
  const toEmails = parseEmailList(optionalEnv("MS_GRAPH_TO_EMAIL"));
  const ccEmails = parseEmailList(optionalEnv("MS_GRAPH_CC_EMAIL"));

  const missing = [
    ["MS_GRAPH_TENANT_ID", tenantId],
    ["MS_GRAPH_CLIENT_ID", clientId],
    ["MS_GRAPH_CLIENT_SECRET", clientSecret],
    ["MS_GRAPH_FROM_EMAIL", fromEmail],
  ]
    .filter(([, value]) => !value)
    .map(([name]) => name);

  if (missing.length > 0) {
    return {
      config: null,
      reason: `Missing required Graph email env vars: ${missing.join(", ")}.`,
    };
  }

  if (toEmails.length === 0 && ccEmails.length === 0) {
    return {
      config: null,
      reason: "No recap recipients configured. Set MS_GRAPH_TO_EMAIL or MS_GRAPH_CC_EMAIL.",
    };
  }

  return {
    config: {
      tenantId: tenantId!,
      clientId: clientId!,
      clientSecret: clientSecret!,
      fromEmail: fromEmail!,
      toEmails,
      ccEmails,
    },
    reason: null,
  };
}

function htmlEscape(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function valueField(value: unknown): unknown {
  if (value && typeof value === "object" && "value" in value) {
    return (value as { value?: unknown }).value;
  }
  return value;
}

function asDisplay(value: unknown, fallback = "-"): string {
  const raw = valueField(value);
  if (raw == null) {
    return fallback;
  }
  const text = String(raw).trim();
  return text || fallback;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function parseJsonObjectFromText(value: unknown): Record<string, unknown> | null {
  if (typeof value !== "string") {
    return null;
  }

  const jsonStart = value.indexOf("{");
  if (jsonStart < 0) {
    return null;
  }

  try {
    const parsed = JSON.parse(value.slice(jsonStart)) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function responseCandidates(response: unknown): Array<Record<string, unknown>> {
  const candidates: Array<Record<string, unknown>> = [];
  const queue: unknown[] = [response];
  const seen = new Set<Record<string, unknown>>();

  for (let index = 0; index < queue.length; index += 1) {
    const current = queue[index];
    if (!isRecord(current) || seen.has(current)) {
      continue;
    }

    seen.add(current);
    candidates.push(current);

    for (const key of ["acumaticaResponse", "responseBody", "body", "response", "result"]) {
      queue.push(current[key]);
    }

    for (const key of ["rawAcumaticaResponseText", "responseText", "rawError", "error"]) {
      const parsed = parseJsonObjectFromText(current[key]);
      if (parsed) {
        queue.push(parsed);
      }
    }
  }

  return candidates;
}

function extractDetails(response: unknown): Array<Record<string, unknown>> {
  for (const candidate of responseCandidates(response)) {
    const details = candidate.Details;
    if (Array.isArray(details)) {
      return details.filter((line): line is Record<string, unknown> => isRecord(line));
    }
  }

  return [];
}

function collectFieldErrors(
  node: unknown,
  path = "$",
  acc: Array<{ path: string; message: string }> = [],
): Array<{ path: string; message: string }> {
  if (!node || typeof node !== "object") {
    return acc;
  }

  if (Array.isArray(node)) {
    for (let i = 0; i < node.length; i += 1) {
      collectFieldErrors(node[i], `${path}[${i}]`, acc);
    }
    return acc;
  }

  const obj = node as Record<string, unknown>;
  const error = asDisplay(obj.error, "");
  if (error) {
    acc.push({ path, message: error });
  }
  const exceptionMessage = asDisplay(obj.exceptionMessage, "");
  if (exceptionMessage) {
    acc.push({ path: `${path}.exceptionMessage`, message: exceptionMessage });
  }

  for (const [key, value] of Object.entries(obj)) {
    collectFieldErrors(value, `${path}.${key}`, acc);
  }

  return acc;
}

function collectResponseFieldErrors(response: unknown): Array<{ path: string; message: string }> {
  const seen = new Set<string>();
  const errors: Array<{ path: string; message: string }> = [];

  for (const candidate of responseCandidates(response)) {
    for (const error of collectFieldErrors(candidate)) {
      const key = `${error.path}\n${error.message}`;
      if (!seen.has(key)) {
        seen.add(key);
        errors.push(error);
      }
    }
  }

  return errors;
}

function parseDetailIndex(path: string): number | null {
  const match = path.match(/Details\[(\d+)\]/);
  if (!match) {
    return null;
  }
  const index = Number(match[1]);
  return Number.isInteger(index) && index >= 0 ? index : null;
}

function parseFieldName(path: string): string {
  const parts = path.split(".");
  return parts[parts.length - 1] || path;
}

function compactWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function isGenericAcumaticaWrapperError(message: string): boolean {
  return (
    /Inserting\s+'?AR Transactions'? record raised at least one error/i.test(message) ||
    /Inserting\s+'?AR Invoice\/Memo'? record raised at least one error/i.test(message) ||
    /^Operation failed$/i.test(message) ||
    /^Exception has been thrown by the target of an invocation\.$/i.test(message)
  );
}

function getLocationFromNotFoundMessage(message: string): string | null {
  const match = message.match(/Location '([^']+)' cannot be found/i);
  return match?.[1]?.trim() || null;
}

function isLocationNotFoundError(error: { path: string; message: string }): boolean {
  return /LocationID$/i.test(error.path) || getLocationFromNotFoundMessage(error.message) !== null;
}

function isInventoryProcessingCrash(error: { message: string }): boolean {
  return (
    /processing of the field InventoryID/i.test(error.message) ||
    /ARTran_InventoryID_FieldUpdated/i.test(error.message) ||
    /Object reference not set to an instance of an object/i.test(error.message)
  );
}

function friendlyMessage(
  error: { path: string; message: string },
  detail?: Record<string, unknown> | null,
): string {
  const inventoryId = detail ? asDisplay(detail.InventoryID, "") : "";

  if (/unassigned Location and\/or Lot\/Serial Number/i.test(error.message)) {
    const itemLabel = inventoryId ? ` ${inventoryId}` : "";
    return `Inventory setup issue: Acumatica needs a valid bin/location or lot/serial setup for item${itemLabel} before this invoice can be created.`;
  }

  const missingLocation = getLocationFromNotFoundMessage(error.message);
  if (missingLocation) {
    return `Customer/location mapping issue: Acumatica does not have location ${missingLocation}. Fix the customer location or Service Fusion location mapping, then retry.`;
  }

  if (isInventoryProcessingCrash(error)) {
    return "Item setup issue: Acumatica crashed while processing Inventory ID. Check the listed stock item(s) for posting/account/default setup in Acumatica, then retry.";
  }

  return compactWhitespace(error.message);
}

function requestInventoryLines(details: Array<Record<string, unknown>>): Array<{
  lineNumber: string;
  inventoryId: string;
  description: string;
}> {
  const allLines = details.map((detail, index) => ({
    lineNumber: asDisplay(detail.LineNbr, String(index + 1)),
    inventoryId: asDisplay(detail.InventoryID),
    description: asDisplay(detail.TransactionDescr, ""),
  }));

  const productLines = allLines.filter((line) => line.inventoryId !== "-" && !line.inventoryId.startsWith("INS-"));
  return productLines.length > 0 ? productLines : allLines.filter((line) => line.inventoryId !== "-");
}

function joinLimited(values: string[], limit = 4): string {
  const unique = Array.from(new Set(values.filter((value) => value && value !== "-")));
  if (unique.length <= limit) {
    return unique.join(", ") || "-";
  }
  return `${unique.slice(0, limit).join(", ")} +${unique.length - limit} more`;
}

function buildFailedLineErrors(failedJob: SendReadyInvoicesResult["failedJobs"][number]): FailedLineError[] {
  const details = extractDetails(failedJob.acumatica.response);
  const requestDetails = extractDetails(failedJob.requestPayload);
  const providedFieldErrors = failedJob.acumatica.fieldErrors ?? [];
  const hasProvidedLineErrors = providedFieldErrors.some((error) => parseDetailIndex(error.path) != null);
  const fieldErrors = hasProvidedLineErrors
    ? providedFieldErrors
    : [...providedFieldErrors, ...collectResponseFieldErrors(failedJob.acumatica.response)];
  const lineErrors = fieldErrors.filter((error) => parseDetailIndex(error.path) != null);
  const meaningfulDocumentErrors = fieldErrors.filter(
    (error) => parseDetailIndex(error.path) == null && !isGenericAcumaticaWrapperError(error.message),
  );

  if (lineErrors.length > 0) {
    return lineErrors.map((error) => {
      const detailIndex = parseDetailIndex(error.path);
      const detail =
        detailIndex == null ? null : details[detailIndex] ?? requestDetails[detailIndex] ?? null;
      return {
        serviceFusionJobNumber: failedJob.serviceFusionJobNumber ?? failedJob.serviceFusionJobId,
        lineNumber: detail ? asDisplay(detail.LineNbr, String((detailIndex ?? 0) + 1)) : "-",
        inventoryId: detail ? asDisplay(detail.InventoryID) : "-",
        field: parseFieldName(error.path),
        message: friendlyMessage(error, detail),
      };
    });
  }

  const locationError = meaningfulDocumentErrors.find(isLocationNotFoundError);
  if (locationError) {
    return [
      {
        serviceFusionJobNumber: failedJob.serviceFusionJobNumber ?? failedJob.serviceFusionJobId,
        lineNumber: "-",
        inventoryId: "-",
        field: "LocationID",
        message: friendlyMessage(locationError),
      },
    ];
  }

  const inventoryCrash = meaningfulDocumentErrors.find(isInventoryProcessingCrash);
  if (inventoryCrash) {
    const likelyLines = requestInventoryLines(requestDetails);
    return [
      {
        serviceFusionJobNumber: failedJob.serviceFusionJobNumber ?? failedJob.serviceFusionJobId,
        lineNumber: joinLimited(likelyLines.map((line) => line.lineNumber)),
        inventoryId: joinLimited(likelyLines.map((line) => line.inventoryId)),
        field: "InventoryID",
        message: friendlyMessage(inventoryCrash),
      },
    ];
  }

  const selectedErrors =
    meaningfulDocumentErrors.length > 0
      ? meaningfulDocumentErrors
      : fieldErrors.filter((error) => !isGenericAcumaticaWrapperError(error.message));

  if (selectedErrors.length === 0) {
    return [
      {
        serviceFusionJobNumber: failedJob.serviceFusionJobNumber ?? failedJob.serviceFusionJobId,
        lineNumber: "-",
        inventoryId: "-",
        field: "Document",
        message:
          failedJob.acumatica.message ??
          "Acumatica rejected the invoice, but did not return a line-level reason.",
      },
    ];
  }

  return selectedErrors.map((error) => {
    return {
      serviceFusionJobNumber: failedJob.serviceFusionJobNumber ?? failedJob.serviceFusionJobId,
      lineNumber: "-",
      inventoryId: "-",
      field: parseFieldName(error.path).replace(/^\$/, "Document"),
      message: friendlyMessage(error),
    };
  });
}

function buildHtml(denverDate: string, sendResult: SendReadyInvoicesResult): string {
  const successes = sendResult.sentJobs;
  const failures = sendResult.failedJobs.flatMap(buildFailedLineErrors);

  const successRows =
    successes.length > 0
      ? successes
          .map(
            (job) => `
              <tr>
                <td>${htmlEscape(job.serviceFusionJobNumber ?? job.serviceFusionJobId)}</td>
                <td>${htmlEscape(job.acumaticaRef ?? "-")}</td>
              </tr>`,
          )
          .join("")
      : `<tr><td colspan="2">No successful invoice sends.</td></tr>`;

  const failureRows =
    failures.length > 0
      ? failures
          .map(
            (error) => `
              <tr>
                <td>${htmlEscape(error.serviceFusionJobNumber)}</td>
                <td>${htmlEscape(error.lineNumber)}</td>
                <td>${htmlEscape(error.inventoryId)}</td>
                <td>${htmlEscape(error.field)}</td>
                <td>${htmlEscape(error.message)}</td>
              </tr>`,
          )
          .join("")
      : `<tr><td colspan="5">No failed invoice sends.</td></tr>`;

  return `<!doctype html>
<html>
  <body style="font-family: Arial, sans-serif; color: #1f2937;">
    <h2 style="margin-bottom: 4px;">Service Fusion Install Invoices Recap for ${htmlEscape(denverDate)}</h2>
    <p style="margin-top: 0;">
      Attempted: ${htmlEscape(sendResult.stats.attempted)} |
      Succeeded: ${htmlEscape(sendResult.stats.sent)} |
      Failed: ${htmlEscape(sendResult.stats.failed)}
    </p>

    <h3>Successful Invoices</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse: collapse;">
      <thead>
        <tr>
          <th align="left">Service Fusion Job #</th>
          <th align="left">Acumatica Invoice #</th>
        </tr>
      </thead>
      <tbody>${successRows}</tbody>
    </table>

    <h3>Failed Invoices</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse: collapse;">
      <thead>
        <tr>
          <th align="left">Service Fusion Job #</th>
          <th align="left">Line #</th>
          <th align="left">Inventory ID</th>
          <th align="left">Field</th>
          <th align="left">Error</th>
        </tr>
      </thead>
      <tbody>${failureRows}</tbody>
    </table>
  </body>
</html>`;
}

async function getGraphAccessToken(config: GraphConfig): Promise<string> {
  const body = new URLSearchParams({
    client_id: config.clientId,
    client_secret: config.clientSecret,
    grant_type: "client_credentials",
    scope: "https://graph.microsoft.com/.default",
  });

  const response = await fetch(
    `https://login.microsoftonline.com/${encodeURIComponent(config.tenantId)}/oauth2/v2.0/token`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
      cache: "no-store",
    },
  );
  const data = (await response.json()) as { access_token?: string; error?: string; error_description?: string };

  if (!response.ok || !data.access_token) {
    throw new Error(
      `Graph token request failed: ${data.error_description || data.error || response.statusText}`,
    );
  }

  return data.access_token;
}

async function sendGraphMail(config: GraphConfig, subject: string, html: string): Promise<void> {
  const accessToken = await getGraphAccessToken(config);
  const response = await fetch(
    `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(config.fromEmail)}/sendMail`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: {
          subject,
          body: {
            contentType: "HTML",
            content: html,
          },
          toRecipients: config.toEmails.map((address) => ({ emailAddress: { address } })),
          ccRecipients: config.ccEmails.map((address) => ({ emailAddress: { address } })),
        },
        saveToSentItems: true,
      }),
      cache: "no-store",
    },
  );

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Graph sendMail failed: ${response.status} ${response.statusText} ${body}`);
  }
}

export async function sendInvoiceRecapEmail(
  denverDate: string,
  sendResult: SendReadyInvoicesResult,
): Promise<InvoiceRecapEmailResult> {
  const subject = `Service Fusion Install Invoices Recap for ${denverDate}`;
  const { config, reason } = getGraphConfig();
  const baseResult = {
    sent: false,
    subject,
    toEmails: config?.toEmails ?? [],
    ccEmails: config?.ccEmails ?? [],
    successCount: sendResult.sentJobs.length,
    failureCount: sendResult.failedJobs.length,
  };

  if (!config) {
    return {
      ...baseResult,
      skippedReason: reason,
      error: null,
    };
  }

  try {
    await sendGraphMail(config, subject, buildHtml(denverDate, sendResult));
    return {
      ...baseResult,
      sent: true,
      skippedReason: null,
      error: null,
    };
  } catch (error) {
    return {
      ...baseResult,
      skippedReason: null,
      error: error instanceof Error ? error.message : "Unknown Graph email error.",
    };
  }
}
