import { SfJobSyncStatus, SfSyncRunStatus, type Prisma } from "@prisma/client";

import { prisma } from "@/lib/prisma";
import type { DbReadyJob, DbReadyJobsResult } from "@/lib/service-fusion/transform-for-db";

export type PersistDbReadyResult = {
  runId: string;
  denverDate: string;
  stats: {
    fetchedCount: number;
    processedCount: number;
    readyCount: number;
    failedCount: number;
    sentSuccessCount: number;
    sentFailedCount: number;
  };
};

function toDecimalValue(value: number): Prisma.Decimal | number {
  return Number.isFinite(value) ? value : 0;
}

function toVarchar(value: string | null | undefined, maxLength: number): string | null {
  if (value == null) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.length > maxLength ? trimmed.slice(0, maxLength) : trimmed;
}

function toNullableDate(value: string | null | undefined): Date | null {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? null : parsed;
}

function toUpsertKeyDate(value: string | null | undefined): Date {
  const parsed = toNullableDate(value);
  if (parsed) {
    return parsed;
  }
  // Use a deterministic fallback so composite unique key remains stable when SF omits updated_at.
  return new Date("1970-01-01T00:00:00.000Z");
}

function resolveJobSyncStatus(job: DbReadyJob): { status: SfJobSyncStatus; failureReason: string | null } {
  if (!job.acumaticaCustomerId) {
    return {
      status: SfJobSyncStatus.FAILED,
      failureReason: "Missing Acumatica CustomerID custom field value.",
    };
  }

  return {
    status: SfJobSyncStatus.READY,
    failureReason: null,
  };
}

async function upsertJobWithLines(runId: string, job: DbReadyJob): Promise<SfJobSyncStatus> {
  const syncDecision = resolveJobSyncStatus(job);
  const serviceFusionUpdatedAt = toUpsertKeyDate(job.updatedAt);

  const upserted = await prisma.sfJob.upsert({
    where: {
      serviceFusionJobId_serviceFusionUpdatedAt: {
        serviceFusionJobId: BigInt(job.serviceFusionJobId),
        serviceFusionUpdatedAt,
      },
    },
    create: {
      runId,
      serviceFusionJobId: BigInt(job.serviceFusionJobId),
      serviceFusionJobNumber: job.serviceFusionJobNumber,
      serviceFusionUpdatedAt,
      statusSf: job.status,
      customerIdSf: job.customerId ? BigInt(job.customerId) : null,
      customerName: job.customerName,
      acumaticaCustomerId: job.acumaticaCustomerId,
      locationNameRaw: job.locationNameRaw,
      locationNickname: job.locationNickname,
      locationIdForAcumatica: job.locationIdForAcumatica,
      street1: job.address.street1,
      street2: job.address.street2,
      city: job.address.city,
      stateProv: job.address.stateProv,
      postalCode: job.address.postalCode,
      total: toDecimalValue(job.totals.total),
      taxesFeesTotal: toDecimalValue(job.totals.taxesFeesTotal),
      dueTotal: toDecimalValue(job.totals.dueTotal),
      taxableAmount: toDecimalValue(job.taxableAmount),
      taxAmount: toDecimalValue(job.taxAmount),
      effectiveTaxRate: toDecimalValue(job.effectiveTaxRate),
      isTaxValid: job.isTaxValid,
      syncStatus: syncDecision.status,
      failureReason: syncDecision.failureReason,
      payloadJson: job,
    },
    update: {
      runId,
      serviceFusionJobNumber: job.serviceFusionJobNumber,
      statusSf: job.status,
      customerIdSf: job.customerId ? BigInt(job.customerId) : null,
      customerName: job.customerName,
      acumaticaCustomerId: job.acumaticaCustomerId,
      locationNameRaw: job.locationNameRaw,
      locationNickname: job.locationNickname,
      locationIdForAcumatica: job.locationIdForAcumatica,
      street1: job.address.street1,
      street2: job.address.street2,
      city: job.address.city,
      stateProv: job.address.stateProv,
      postalCode: job.address.postalCode,
      total: toDecimalValue(job.totals.total),
      taxesFeesTotal: toDecimalValue(job.totals.taxesFeesTotal),
      dueTotal: toDecimalValue(job.totals.dueTotal),
      taxableAmount: toDecimalValue(job.taxableAmount),
      taxAmount: toDecimalValue(job.taxAmount),
      effectiveTaxRate: toDecimalValue(job.effectiveTaxRate),
      isTaxValid: job.isTaxValid,
      syncStatus: syncDecision.status,
      failureReason: syncDecision.failureReason,
      payloadJson: job,
      updatedAt: new Date(),
    },
  });

  await prisma.sfJobLine.deleteMany({
    where: { jobId: upserted.id },
  });

  if (job.lines.length > 0) {
    await prisma.sfJobLine.createMany({
      data: job.lines.map((line, index) => ({
        jobId: upserted.id,
        lineNo: index + 1,
        lineType: line.lineType,
        inventoryId: toVarchar(line.inventoryId, 128),
        description: toVarchar(line.description, 512),
        quantity: toDecimalValue(line.quantity),
        unitPrice: toDecimalValue(line.unitPrice),
        lineTotal: toDecimalValue(line.lineTotal),
        taxNameRaw: toVarchar(line.taxNameRaw, 128),
        taxCategory: line.taxCategory,
        sourceName: "sourceName" in line ? toVarchar(line.sourceName, 512) : null,
        lineIssuesJson: line.issues,
      })),
    });
  }

  await prisma.sfJobTaxDetail.deleteMany({
    where: { jobId: upserted.id },
  });

  if (job.taxDetails.length > 0) {
    await prisma.sfJobTaxDetail.createMany({
      data: job.taxDetails.map((detail) => ({
        jobId: upserted.id,
        taxNameRaw: toVarchar(detail.taxNameRaw, 128) ?? "UNKNOWN",
        acumaticaTaxId: toVarchar(detail.acumaticaTaxId, 64) ?? "SLC",
        taxRate: toDecimalValue(detail.taxRate),
        taxableAmount: toDecimalValue(detail.taxableAmount),
        taxAmount: toDecimalValue(detail.taxAmount),
      })),
    });
  }

  const eventMessage =
    syncDecision.status === SfJobSyncStatus.FAILED
      ? `Persisted with FAILED status: ${syncDecision.failureReason}`
      : "Persisted and marked READY for outbound Acumatica write.";

  await prisma.sfJobEvent.create({
    data: {
      jobId: upserted.id,
      eventType: "INGEST_PERSISTED",
      message: eventMessage,
      detailsJson: {
        jobId: job.serviceFusionJobId,
        syncStatus: syncDecision.status,
      },
    },
  });

  return syncDecision.status;
}

export async function persistDbReadyJobs(source: DbReadyJobsResult): Promise<PersistDbReadyResult> {
  // TODO(next phase): Add outbound Acumatica invoice write step after READY persistence.
  // TODO(next phase): Retry failed writes and email accounting a failure summary.
  const run = await prisma.sfSyncRun.create({
    data: {
      runType: "DAILY_INVOICED",
      denverDate: source.window.date,
      status: SfSyncRunStatus.RUNNING,
      fetchedCount: source.stats.fetchedCandidates,
      processedCount: source.jobs.length,
      sentSuccessCount: 0,
      sentFailedCount: 0,
    },
  });

  let readyCount = 0;
  let failedCount = 0;

  try {
    for (const job of source.jobs) {
      const status = await upsertJobWithLines(run.id, job);
      if (status === SfJobSyncStatus.READY) {
        readyCount += 1;
      } else if (status === SfJobSyncStatus.FAILED) {
        failedCount += 1;
      }
    }

    const finalStatus =
      failedCount === 0
        ? SfSyncRunStatus.SUCCESS
        : readyCount > 0
          ? SfSyncRunStatus.PARTIAL
          : SfSyncRunStatus.FAILED;

    await prisma.sfSyncRun.update({
      where: { id: run.id },
      data: {
        finishedAt: new Date(),
        status: finalStatus,
        processedCount: source.jobs.length,
        sentSuccessCount: 0,
        sentFailedCount: failedCount,
        errorSummary: failedCount > 0 ? `${failedCount} jobs marked FAILED during persistence.` : null,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown persistence error.";
    await prisma.sfSyncRun.update({
      where: { id: run.id },
      data: {
        finishedAt: new Date(),
        status: SfSyncRunStatus.FAILED,
        errorSummary: message,
      },
    });
    throw error;
  }

  return {
    runId: run.id,
    denverDate: source.window.date,
    stats: {
      fetchedCount: source.stats.fetchedCandidates,
      processedCount: source.jobs.length,
      readyCount,
      failedCount,
      sentSuccessCount: 0,
      sentFailedCount: failedCount,
    },
  };
}
