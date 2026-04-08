import { serviceFusionFetch } from "./client";

const DENVER_TIME_ZONE = "America/Denver";
const JOB_PAGE_SIZE = 100;
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 500;

type ServiceFusionJobsListItem = {
  id: number;
  number?: string | null;
  closed_at?: string | null;
  customer_id?: number | null;
  status?: string | null;
};

type ServiceFusionJobsListResponse = {
  items?: ServiceFusionJobsListItem[];
  _meta?: {
    pageCount?: number;
    currentPage?: number;
    perPage?: number;
    totalCount?: number;
  };
};

type ServiceFusionJobServiceLine = {
  name?: string | null;
  service?: string | null;
  multiplier?: number | null;
  rate?: number | null;
  total?: number | null;
  tax?: string | null;
};

type ServiceFusionJobProductLine = {
  name?: string | null;
  product?: string | null;
  multiplier?: number | null;
  rate?: number | null;
  total?: number | null;
  tax?: string | null;
};

type ServiceFusionJobDetail = {
  id: number;
  number?: string | null;
  closed_at?: string | null;
  status?: string | null;
  customer_id?: number | null;
  customer_name?: string | null;
  location_name?: string | null;
  street_1?: string | null;
  street_2?: string | null;
  city?: string | null;
  state_prov?: string | null;
  postal_code?: string | null;
  total?: number | null;
  taxes_fees_total?: number | null;
  due_total?: number | null;
  services?: ServiceFusionJobServiceLine[];
  products?: ServiceFusionJobProductLine[];
};

type ServiceFusionCustomerCustomField = {
  name?: string | null;
  value?: string | null;
};

type ServiceFusionCustomerLocation = {
  street_1?: string | null;
  city?: string | null;
  state_prov?: string | null;
  postal_code?: string | null;
  nickname?: string | null;
  is_primary?: boolean | null;
  is_bill_to?: boolean | null;
};

type ServiceFusionCustomerResponse = {
  id: number;
  custom_fields?: ServiceFusionCustomerCustomField[];
  locations?: ServiceFusionCustomerLocation[];
};

type ExtractionIssue = {
  code: string;
  message: string;
};

type NormalizedServiceLine = {
  type: "service";
  inventoryId: string | null;
  description: string | null;
  quantity: number;
  unitPrice: number;
  total: number;
  taxNameRaw: string | null;
};

type NormalizedProductLine = {
  type: "product";
  description: string | null;
  quantity: number;
  unitPrice: number;
  total: number;
  taxNameRaw: string | null;
};

export type NormalizedInvoicedJob = {
  jobId: number;
  jobNumber: string | null;
  updatedAt: string | null;
  status: string | null;
  customerId: number | null;
  customerName: string | null;
  acumaticaCustomerId: string | null;
  locationNameRaw: string | null;
  locationNickname: string | null;
  address: {
    street1: string | null;
    street2: string | null;
    city: string | null;
    stateProv: string | null;
    postalCode: string | null;
  };
  totals: {
    total: number;
    taxesFeesTotal: number;
    dueTotal: number;
  };
  lines: Array<NormalizedServiceLine | NormalizedProductLine>;
  issues: ExtractionIssue[];
};

export type DailyInvoicedJobsResult = {
  window: {
    timeZone: string;
    date: string;
    gte: string;
    lte: string;
  };
  stats: {
    fetchedCandidates: number;
    processedJobs: number;
    skippedDuplicates: number;
    failedJobs: number;
    uniqueCustomersFetched: number;
  };
  jobs: NormalizedInvoicedJob[];
  failures: Array<{ jobId: number; reason: string }>;
};

type DateParts = {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
};

export type DailyInvoicedJobsOptions = {
  date?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asNumber(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  return 0;
}

function getZonedParts(date: Date, timeZone: string): DateParts {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(date);
  const lookup = (type: Intl.DateTimeFormatPartTypes): number => {
    const value = parts.find((part) => part.type === type)?.value;
    return value ? Number.parseInt(value, 10) : 0;
  };

  return {
    year: lookup("year"),
    month: lookup("month"),
    day: lookup("day"),
    hour: lookup("hour"),
    minute: lookup("minute"),
    second: lookup("second"),
  };
}

function zonedLocalToUtc(local: DateParts, timeZone: string): Date {
  // Iteratively solve the UTC timestamp that renders as the desired local time in the zone.
  let guess = Date.UTC(
    local.year,
    local.month - 1,
    local.day,
    local.hour,
    local.minute,
    local.second,
  );

  for (let index = 0; index < 3; index += 1) {
    const rendered = getZonedParts(new Date(guess), timeZone);
    const renderedAsUtc = Date.UTC(
      rendered.year,
      rendered.month - 1,
      rendered.day,
      rendered.hour,
      rendered.minute,
      rendered.second,
    );
    const targetAsUtc = Date.UTC(
      local.year,
      local.month - 1,
      local.day,
      local.hour,
      local.minute,
      local.second,
    );
    guess += targetAsUtc - renderedAsUtc;
  }

  return new Date(guess);
}

function formatUtcWithOffset(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = String(date.getUTCHours()).padStart(2, "0");
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  const second = String(date.getUTCSeconds()).padStart(2, "0");
  return `${year}-${month}-${day}T${hour}:${minute}:${second}+00:00`;
}

function isValidDateValue(year: number, month: number, day: number): boolean {
  const test = new Date(Date.UTC(year, month - 1, day));
  return (
    test.getUTCFullYear() === year &&
    test.getUTCMonth() + 1 === month &&
    test.getUTCDate() === day
  );
}

function parseDateOverride(value: string): Pick<DateParts, "year" | "month" | "day"> {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) {
    throw new Error("Invalid date format. Expected YYYY-MM-DD.");
  }

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  if (!isValidDateValue(year, month, day)) {
    throw new Error("Invalid date value. Expected a real calendar date.");
  }

  return { year, month, day };
}

function getDenverWindowForDate(
  dateOverride?: string,
): { gte: string; lte: string; date: string } {
  const denverDate = dateOverride
    ? parseDateOverride(dateOverride)
    : (() => {
        const now = new Date();
        const today = getZonedParts(now, DENVER_TIME_ZONE);
        return { year: today.year, month: today.month, day: today.day };
      })();

  const startUtc = zonedLocalToUtc(
    {
      year: denverDate.year,
      month: denverDate.month,
      day: denverDate.day,
      hour: 0,
      minute: 0,
      second: 0,
    },
    DENVER_TIME_ZONE,
  );
  const endUtc = zonedLocalToUtc(
    {
      year: denverDate.year,
      month: denverDate.month,
      day: denverDate.day,
      hour: 23,
      minute: 59,
      second: 59,
    },
    DENVER_TIME_ZONE,
  );

  return {
    gte: formatUtcWithOffset(startUtc),
    lte: formatUtcWithOffset(endUtc),
    date: `${String(denverDate.year).padStart(4, "0")}-${String(denverDate.month).padStart(2, "0")}-${String(denverDate.day).padStart(2, "0")}`,
  };
}

async function fetchJsonWithRetry<T>(path: string): Promise<T> {
  let attempt = 0;

  while (attempt <= MAX_RETRIES) {
    const response = await serviceFusionFetch(path);

    if (response.ok) {
      return (await response.json()) as T;
    }

    const shouldRetry = response.status === 429 || response.status >= 500;
    if (!shouldRetry || attempt === MAX_RETRIES) {
      const body = await response.text();
      throw new Error(`Service Fusion request failed (${response.status}): ${body}`);
    }

    const delay = RETRY_BASE_DELAY_MS * 2 ** attempt;
    await sleep(delay);
    attempt += 1;
  }

  throw new Error("Service Fusion request failed after retries.");
}

function buildJobsListPath(page: number, gte: string, lte: string): string {
  const params = new URLSearchParams();
  params.set("filters[status]", "Invoiced");
  params.set("filters[closed_date][gte]", gte);
  params.set("filters[closed_date][lte]", lte);
  params.set("fields", "id,number,closed_at,customer_id,status");
  params.set("per-page", String(JOB_PAGE_SIZE));
  params.set("page", String(page));
  params.set("sort", "closed_at");
  return `/jobs?${params.toString()}`;
}

function toServiceLines(services: ServiceFusionJobServiceLine[] | undefined): NormalizedServiceLine[] {
  if (!services || services.length === 0) {
    return [];
  }

  return services.map((line) => ({
    type: "service",
    inventoryId: line.service ?? null,
    description: line.name ?? null,
    quantity: asNumber(line.multiplier),
    unitPrice: asNumber(line.rate),
    total: asNumber(line.total),
    taxNameRaw: line.tax ?? null,
  }));
}

function toProductLines(products: ServiceFusionJobProductLine[] | undefined): NormalizedProductLine[] {
  if (!products || products.length === 0) {
    return [];
  }

  return products.map((line) => ({
    type: "product",
    // TODO(next phase): Parse model from `MODEL - Description` during DB/Acumatica write transform.
    // Keep source fetch payload untouched here so extraction remains raw and auditable.
    description: line.name ?? line.product ?? null,
    quantity: asNumber(line.multiplier),
    unitPrice: asNumber(line.rate),
    total: asNumber(line.total),
    taxNameRaw: line.tax ?? null,
  }));
}

function extractAcumaticaCustomerId(customer: ServiceFusionCustomerResponse): string | null {
  const fields = customer.custom_fields ?? [];
  const match = fields.find((field) => field.name === "Acumatica CustomerID");
  const value = match?.value?.trim();
  return value ? value : null;
}

function normalizeCompareValue(value: string | null | undefined): string {
  if (!value) {
    return "";
  }
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function normalizePostal(value: string | null | undefined): string {
  if (!value) {
    return "";
  }
  return value.trim().toLowerCase().replace(/\s+/g, "");
}

function resolveLocationNickname(
  job: ServiceFusionJobDetail,
  customer: ServiceFusionCustomerResponse | null,
): string | null {
  if (!customer?.locations || customer.locations.length === 0) {
    return null;
  }

  const locations = customer.locations;
  const rawName = normalizeCompareValue(job.location_name);

  if (rawName) {
    const exact = locations.find((loc) => normalizeCompareValue(loc.nickname) === rawName);
    if (exact?.nickname) {
      return exact.nickname;
    }
  }

  const street = normalizeCompareValue(job.street_1);
  const city = normalizeCompareValue(job.city);
  const state = normalizeCompareValue(job.state_prov);
  const postal = normalizePostal(job.postal_code);

  const byAddress = locations.find((loc) => {
    return (
      normalizeCompareValue(loc.street_1) === street &&
      normalizeCompareValue(loc.city) === city &&
      normalizeCompareValue(loc.state_prov) === state &&
      normalizePostal(loc.postal_code) === postal
    );
  });

  if (byAddress?.nickname) {
    return byAddress.nickname;
  }

  if (!rawName) {
    const billTo = locations.find((loc) => loc.is_bill_to && normalizeCompareValue(loc.nickname));
    if (billTo?.nickname) {
      return billTo.nickname;
    }

    const primary = locations.find((loc) => loc.is_primary && normalizeCompareValue(loc.nickname));
    if (primary?.nickname) {
      return primary.nickname;
    }
  }

  return null;
}

export async function getDailyInvoicedJobsForDenverDate(
  options?: DailyInvoicedJobsOptions,
): Promise<DailyInvoicedJobsResult> {
  const window = getDenverWindowForDate(options?.date);

  const candidates: ServiceFusionJobsListItem[] = [];
  let page = 1;
  let pageCount = 1;
  do {
    const data = await fetchJsonWithRetry<ServiceFusionJobsListResponse>(
      buildJobsListPath(page, window.gte, window.lte),
    );
    candidates.push(...(data.items ?? []));
    pageCount = data._meta?.pageCount ?? page;
    page += 1;
  } while (page <= pageCount);

  const dedupe = new Set<string>();
  const jobs: NormalizedInvoicedJob[] = [];
  const failures: Array<{ jobId: number; reason: string }> = [];
  const customerCache = new Map<number, ServiceFusionCustomerResponse>();

  let skippedDuplicates = 0;

  for (const candidate of candidates) {
    const dedupeKey = `${candidate.id}:${candidate.closed_at ?? ""}`;
    if (dedupe.has(dedupeKey)) {
      skippedDuplicates += 1;
      continue;
    }
    dedupe.add(dedupeKey);

    try {
      const job = await fetchJsonWithRetry<ServiceFusionJobDetail>(
        `/jobs/${candidate.id}?expand=products,services`,
      );

      const issues: ExtractionIssue[] = [];
      const customerId = job.customer_id ?? null;

      let acumaticaCustomerId: string | null = null;
      let customer: ServiceFusionCustomerResponse | null = null;
      if (customerId) {
        customer = customerCache.get(customerId) ?? null;
        if (!customer) {
          customer = await fetchJsonWithRetry<ServiceFusionCustomerResponse>(
            `/customers/${customerId}?expand=locations,custom_fields`,
          );
          customerCache.set(customerId, customer);
        }
        acumaticaCustomerId = extractAcumaticaCustomerId(customer);
      }

      const locationNickname = resolveLocationNickname(job, customer);
      if (job.location_name && !locationNickname) {
        issues.push({
          code: "LOCATION_NICKNAME_NOT_RESOLVED",
          message:
            "Job location_name was present but no matching customer location nickname could be resolved.",
        });
      }

      if (!acumaticaCustomerId) {
        issues.push({
          code: "MISSING_ACUMATICA_CUSTOMER_ID",
          message:
            "Acumatica CustomerID custom field was not found or empty for this job's customer.",
        });
      }

      jobs.push({
        jobId: job.id,
        jobNumber: job.number ?? null,
        updatedAt: job.closed_at ?? null,
        status: job.status ?? null,
        customerId,
        customerName: job.customer_name ?? null,
        acumaticaCustomerId,
        locationNameRaw: job.location_name ?? null,
        locationNickname,
        address: {
          street1: job.street_1 ?? null,
          street2: job.street_2 ?? null,
          city: job.city ?? null,
          stateProv: job.state_prov ?? null,
          postalCode: job.postal_code ?? null,
        },
        totals: {
          total: asNumber(job.total),
          taxesFeesTotal: asNumber(job.taxes_fees_total),
          dueTotal: asNumber(job.due_total),
        },
        lines: [...toServiceLines(job.services), ...toProductLines(job.products)],
        issues,
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : "Unknown extraction error.";
      failures.push({ jobId: candidate.id, reason });
    }
  }

  return {
    window: {
      timeZone: DENVER_TIME_ZONE,
      date: window.date,
      gte: window.gte,
      lte: window.lte,
    },
    stats: {
      fetchedCandidates: candidates.length,
      processedJobs: jobs.length,
      skippedDuplicates,
      failedJobs: failures.length,
      uniqueCustomersFetched: customerCache.size,
    },
    jobs,
    failures,
  };
}

export async function getDailyInvoicedJobsForDenverToday(): Promise<DailyInvoicedJobsResult> {
  return getDailyInvoicedJobsForDenverDate();
}
