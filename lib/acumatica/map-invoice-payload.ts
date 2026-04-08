import type { SfJob, SfJobLine, SfJobTaxDetail } from "@prisma/client";

type ValueField<T> = { value: T };

export type AcumaticaInvoicePayload = {
  CustomerID: ValueField<string>;
  ExternalRef: ValueField<string>;
  LocationID?: ValueField<string>;
  Description: ValueField<string>;
  IsTaxValid: ValueField<boolean>;
  Details: Array<{
    InventoryID: ValueField<string>;
    TransactionDescr?: ValueField<string>;
    LineNbr: ValueField<number>;
    Qty: ValueField<number>;
    UnitPrice: ValueField<number>;
    Amount: ValueField<number>;
    TaxCategory: ValueField<"TAXABLE" | "EXEMPT">;
  }>;
  TaxDetails: Array<{
    TaxID: ValueField<string>;
    TaxRate: ValueField<number>;
    TaxableAmount: ValueField<number>;
    TaxAmount: ValueField<number>;
  }>;
  custom: {
    Document: {
      UsrExtRefNbr: ValueField<string>;
    };
  };
};

export type SendableSfJob = SfJob & {
  lines: SfJobLine[];
  taxDetails: SfJobTaxDetail[];
};

function toNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function mapSfJobToAcumaticaInvoicePayload(job: SendableSfJob): AcumaticaInvoicePayload {
  if (!job.acumaticaCustomerId) {
    throw new Error("Missing acumaticaCustomerId.");
  }

  if (!job.serviceFusionJobNumber) {
    throw new Error("Missing serviceFusionJobNumber.");
  }

  const details = [...job.lines]
    .sort((a, b) => a.lineNo - b.lineNo)
    .map((line) => {
      if (!line.inventoryId) {
        throw new Error(`Missing inventoryId on lineNo=${line.lineNo}.`);
      }
      const mappedLine: AcumaticaInvoicePayload["Details"][number] = {
        InventoryID: { value: line.inventoryId },
        LineNbr: { value: line.lineNo },
        Qty: { value: toNumber(line.quantity) },
        UnitPrice: { value: toNumber(line.unitPrice) },
        Amount: { value: toNumber(line.lineTotal) },
        TaxCategory: { value: line.taxCategory },
      };

      const lineDescription = typeof line.description === "string" ? line.description.trim() : "";
      if (lineDescription) {
        mappedLine.TransactionDescr = { value: lineDescription };
      }

      return mappedLine;
    });

  const taxDetails = [...job.taxDetails].map((tax) => ({
    TaxID: { value: tax.acumaticaTaxId },
    TaxRate: { value: toNumber(tax.taxRate) },
    TaxableAmount: { value: toNumber(tax.taxableAmount) },
    TaxAmount: { value: toNumber(tax.taxAmount) },
  }));

  const payload: AcumaticaInvoicePayload = {
    CustomerID: { value: job.acumaticaCustomerId },
    ExternalRef: { value: job.serviceFusionJobNumber },
    Description: { value: "Service Fusion Install Invoice" },
    IsTaxValid: { value: true },
    Details: details,
    TaxDetails: taxDetails,
    custom: {
      Document: {
        UsrExtRefNbr: { value: job.serviceFusionJobNumber },
      },
    },
  };

  if (job.locationIdForAcumatica) {
    payload.LocationID = { value: job.locationIdForAcumatica };
  }

  return payload;
}
