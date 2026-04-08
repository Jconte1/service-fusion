import type {
  DailyInvoicedJobsResult,
  NormalizedInvoicedJob,
} from "./daily-invoiced-jobs";

const PRODUCT_FALLBACK_INVENTORY_ID = "INSPARTS";
const SERVICE_FALLBACK_INVENTORY_ID = "INS-LABOR";
const UNKNOWN_TAX_FALLBACK_ACUMATICA_TAX_ID = "SLC";

const SERVICE_FUSION_TAX_TO_ACUMATICA_TAX_ID: Record<string, string> = {
  "CEDAR CITY TAX": "CEDAR CITY",
  "IDAHO TAX": "IDAHO",
  "JACKSON TAX": "JACKSON WY",
  "KETCHUM TAX": "KETCHUM ID",
  "PROVO TAX": "PROVO",
  "SALT LAKE TAX": "SLC",
  "WYOMING TAX": "JACKSON WY",
};

type TransformIssue = {
  code: string;
  message: string;
};

type TaxCategory = "TAXABLE" | "EXEMPT";

type DbReadyServiceLine = {
  lineType: "SERVICE";
  inventoryId: string | null;
  description: string | null;
  quantity: number;
  unitPrice: number;
  lineTotal: number;
  taxNameRaw: string | null;
  taxCategory: TaxCategory;
  issues: TransformIssue[];
};

type DbReadyProductLine = {
  lineType: "PRODUCT";
  inventoryId: string;
  description: string | null;
  quantity: number;
  unitPrice: number;
  lineTotal: number;
  sourceName: string | null;
  taxNameRaw: string | null;
  taxCategory: TaxCategory;
  issues: TransformIssue[];
};

export type DbReadyTaxDetail = {
  taxNameRaw: string;
  acumaticaTaxId: string;
  taxRate: number;
  taxableAmount: number;
  taxAmount: number;
};

export type DbReadyJob = {
  serviceFusionJobId: number;
  serviceFusionJobNumber: string | null;
  updatedAt: string | null;
  status: string | null;
  customerId: number | null;
  customerName: string | null;
  acumaticaCustomerId: string | null;
  locationNameRaw: string | null;
  locationNickname: string | null;
  locationIdForAcumatica: string | null;
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
  syncStatus: "PENDING";
  failureReason: string | null;
  isTaxValid: true;
  taxableAmount: number;
  taxAmount: number;
  effectiveTaxRate: number;
  taxDetails: DbReadyTaxDetail[];
  extractionIssues: TransformIssue[];
  lines: Array<DbReadyServiceLine | DbReadyProductLine>;
};

export type DbReadyJobsResult = {
  window: DailyInvoicedJobsResult["window"];
  stats: DailyInvoicedJobsResult["stats"] & {
    productLinesUsingFallbackInventoryId: number;
  };
  failures: DailyInvoicedJobsResult["failures"];
  jobs: DbReadyJob[];
};

type ParsedModelAndDescription = {
  model: string | null;
  description: string;
};

function round2(value: number): number {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function normalizeTaxName(value: string | null | undefined): string {
  return (value ?? "").trim().toUpperCase();
}

function isTaxExemptValue(value: string | null | undefined): boolean {
  const normalized = normalizeTaxName(value);
  return normalized === "" || normalized === "NON" || normalized === "0";
}

function resolveAcumaticaTaxId(value: string | null | undefined): string {
  const normalized = normalizeTaxName(value);
  if (!normalized) {
    return UNKNOWN_TAX_FALLBACK_ACUMATICA_TAX_ID;
  }
  return SERVICE_FUSION_TAX_TO_ACUMATICA_TAX_ID[normalized] ?? UNKNOWN_TAX_FALLBACK_ACUMATICA_TAX_ID;
}

function isKnownTaxName(value: string | null | undefined): boolean {
  const normalized = normalizeTaxName(value);
  if (!normalized || normalized === "NON" || normalized === "0") {
    return true;
  }
  return Boolean(SERVICE_FUSION_TAX_TO_ACUMATICA_TAX_ID[normalized]);
}

function parseModelAndDescription(raw: string): ParsedModelAndDescription {
  const parts = raw.split(" - ");
  if (parts.length < 2) {
    return { model: null, description: raw };
  }

  const model = parts.shift()?.trim() ?? "";
  const description = parts.join(" - ").trim();
  if (!model) {
    return { model: null, description: raw };
  }

  return { model, description: description || raw };
}

function toDbServiceLine(line: {
  inventoryId: string | null;
  description: string | null;
  quantity: number;
  unitPrice: number;
  total: number;
  taxNameRaw: string | null;
}): DbReadyServiceLine {
  const taxCategory = isTaxExemptValue(line.taxNameRaw) ? "EXEMPT" : "TAXABLE";
  const issues: TransformIssue[] = [];
  if (taxCategory === "TAXABLE" && !isKnownTaxName(line.taxNameRaw)) {
    issues.push({
      code: "UNKNOWN_TAX_CODE_FALLBACK_SLC",
      message: "Unknown taxable tax name. Falling back to Acumatica TaxID SLC.",
    });
  }

  const normalizedInventoryId = line.inventoryId?.trim() ?? "";
  if (!normalizedInventoryId) {
    issues.push({
      code: "SERVICE_INVENTORY_ID_MISSING_FALLBACK_INS_LABOR",
      message: "Service inventory ID was empty. Using fallback inventory ID INS-LABOR.",
    });
    return {
      lineType: "SERVICE",
      inventoryId: SERVICE_FALLBACK_INVENTORY_ID,
      description: line.description,
      quantity: line.quantity,
      unitPrice: line.unitPrice,
      lineTotal: line.total,
      taxNameRaw: line.taxNameRaw,
      taxCategory,
      issues,
    };
  }

  return {
    lineType: "SERVICE",
    inventoryId: normalizedInventoryId,
    description: line.description,
    quantity: line.quantity,
    unitPrice: line.unitPrice,
    lineTotal: line.total,
    taxNameRaw: line.taxNameRaw,
    taxCategory,
    issues,
  };
}

function toDbProductLine(
  line: {
    description: string | null;
    quantity: number;
    unitPrice: number;
    total: number;
    taxNameRaw: string | null;
  },
  stats: DbReadyJobsResult["stats"],
): DbReadyProductLine {
  const sourceName = line.description;
  const issues: TransformIssue[] = [];
  const taxCategory = isTaxExemptValue(line.taxNameRaw) ? "EXEMPT" : "TAXABLE";
  if (taxCategory === "TAXABLE" && !isKnownTaxName(line.taxNameRaw)) {
    issues.push({
      code: "UNKNOWN_TAX_CODE_FALLBACK_SLC",
      message: "Unknown taxable tax name. Falling back to Acumatica TaxID SLC.",
    });
  }

  if (!sourceName) {
    stats.productLinesUsingFallbackInventoryId += 1;
    issues.push({
      code: "PRODUCT_MODEL_NOT_FOUND_FALLBACK_INSPARTS",
      message: "Product description/name was empty. Using fallback inventory ID INSPARTS.",
    });
    return {
      lineType: "PRODUCT",
      inventoryId: PRODUCT_FALLBACK_INVENTORY_ID,
      description: null,
      quantity: line.quantity,
      unitPrice: line.unitPrice,
      lineTotal: line.total,
      sourceName: null,
      taxNameRaw: line.taxNameRaw,
      taxCategory,
      issues,
    };
  }

  const parsed = parseModelAndDescription(sourceName);
  if (!parsed.model) {
    stats.productLinesUsingFallbackInventoryId += 1;
    issues.push({
      code: "PRODUCT_MODEL_NOT_FOUND_FALLBACK_INSPARTS",
      message:
        "Product name did not match `MODEL - Description`. Using fallback inventory ID INSPARTS.",
    });
    return {
      lineType: "PRODUCT",
      inventoryId: PRODUCT_FALLBACK_INVENTORY_ID,
      description: sourceName,
      quantity: line.quantity,
      unitPrice: line.unitPrice,
      lineTotal: line.total,
      sourceName,
      taxNameRaw: line.taxNameRaw,
      taxCategory,
      issues,
    };
  }

  return {
    lineType: "PRODUCT",
    inventoryId: parsed.model,
    description: parsed.description,
    quantity: line.quantity,
    unitPrice: line.unitPrice,
    lineTotal: line.total,
    sourceName,
    taxNameRaw: line.taxNameRaw,
    taxCategory,
    issues,
  };
}

function computeTaxDetails(lines: DbReadyJob["lines"], jobTotal: number): {
  taxableAmount: number;
  taxAmount: number;
  effectiveTaxRate: number;
  taxDetails: DbReadyTaxDetail[];
} {
  const lineSubtotal = round2(lines.reduce((sum, line) => sum + line.lineTotal, 0));
  const taxableLines = lines.filter((line) => line.taxCategory === "TAXABLE");
  const taxableAmount = round2(taxableLines.reduce((sum, line) => sum + line.lineTotal, 0));
  // Service Fusion's `taxes_fees_total` does not consistently represent tax-only amount.
  // Derive tax amount from total minus line subtotal to match UI-calculated tax.
  const derivedTax = round2(jobTotal - lineSubtotal);
  const taxAmount = derivedTax > 0 ? derivedTax : 0;
  const effectiveTaxRate = taxableAmount > 0 ? round2((taxAmount / taxableAmount) * 100) : 0;

  if (taxableAmount <= 0) {
    return { taxableAmount: 0, taxAmount, effectiveTaxRate: 0, taxDetails: [] };
  }

  const groups = new Map<string, { taxNameRaw: string; taxableAmount: number }>();
  for (const line of taxableLines) {
    const taxNameRaw = line.taxNameRaw ?? "UNKNOWN";
    const acumaticaTaxId = resolveAcumaticaTaxId(taxNameRaw);
    const existing = groups.get(acumaticaTaxId);
    if (existing) {
      existing.taxableAmount += line.lineTotal;
    } else {
      groups.set(acumaticaTaxId, { taxNameRaw, taxableAmount: line.lineTotal });
    }
  }

  const groupEntries = Array.from(groups.entries()).map(([acumaticaTaxId, data]) => ({
    acumaticaTaxId,
    taxNameRaw: data.taxNameRaw,
    taxableAmount: round2(data.taxableAmount),
  }));

  let allocated = 0;
  const taxDetails: DbReadyTaxDetail[] = groupEntries.map((group, index) => {
    const isLast = index === groupEntries.length - 1;
    const proposed = isLast
      ? round2(taxAmount - allocated)
      : round2((taxAmount * group.taxableAmount) / taxableAmount);
    allocated = round2(allocated + proposed);
    const groupRate = group.taxableAmount > 0 ? round2((proposed / group.taxableAmount) * 100) : 0;
    return {
      taxNameRaw: group.taxNameRaw,
      acumaticaTaxId: group.acumaticaTaxId,
      taxRate: groupRate,
      taxableAmount: group.taxableAmount,
      taxAmount: proposed,
    };
  });

  return { taxableAmount, taxAmount, effectiveTaxRate, taxDetails };
}

function deriveLocationIdForAcumatica(locationNameRaw: string | null): string | null {
  const source = (locationNameRaw ?? "").trim();
  if (!source) {
    return null;
  }

  const dashIndex = source.indexOf(" - ");
  if (dashIndex <= 0) {
    return null;
  }

  const parsed = source.slice(0, dashIndex).trim();
  return parsed || null;
}

function toDbJob(job: NormalizedInvoicedJob, stats: DbReadyJobsResult["stats"]): DbReadyJob {
  const extractionIssues: TransformIssue[] = job.issues.map((issue) => ({
    code: issue.code,
    message: issue.message,
  }));

  const lines: DbReadyJob["lines"] = job.lines
    .filter((line) => line.quantity !== 0)
    .map((line) => {
    if (line.type === "service") {
      return toDbServiceLine(line);
    }
    return toDbProductLine(line, stats);
    });

  // Only write LocationID using the canonical parsed field.
  const locationIdForAcumatica = deriveLocationIdForAcumatica(job.locationNameRaw);
  const taxComputed = computeTaxDetails(lines, job.totals.total);

  return {
    serviceFusionJobId: job.jobId,
    serviceFusionJobNumber: job.jobNumber,
    updatedAt: job.updatedAt,
    status: job.status,
    customerId: job.customerId,
    customerName: job.customerName,
    acumaticaCustomerId: job.acumaticaCustomerId,
    locationNameRaw: job.locationNameRaw,
    locationNickname: job.locationNickname,
    locationIdForAcumatica,
    address: { ...job.address },
    totals: { ...job.totals },
    syncStatus: "PENDING",
    failureReason: null,
    isTaxValid: true,
    taxableAmount: taxComputed.taxableAmount,
    taxAmount: taxComputed.taxAmount,
    effectiveTaxRate: taxComputed.effectiveTaxRate,
    taxDetails: taxComputed.taxDetails,
    extractionIssues,
    lines,
  };
}

export function transformDailyInvoicedJobsToDbReady(
  source: DailyInvoicedJobsResult,
): DbReadyJobsResult {
  const result: DbReadyJobsResult = {
    window: source.window,
    stats: {
      ...source.stats,
      productLinesUsingFallbackInventoryId: 0,
    },
    failures: source.failures,
    jobs: [],
  };

  result.jobs = source.jobs.map((job) => toDbJob(job, result.stats));
  return result;
}
