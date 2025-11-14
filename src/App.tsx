import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast, Toaster } from "sonner";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:4000";
const SANDBOX_WALLET_ENABLED = import.meta.env.VITE_WALLET_SANDBOX === "true";
const CAPABILITIES = [
  "image-processing",
  "data-analysis",
  "text-processing",
  "web-scraping",
  "machine-learning",
];

const DEFAULT_PLATFORM_FEE_PERCENT = 10;

let activeSessionId: string | null = null;

function setActiveSessionId(value: string) {
  activeSessionId = value;
}

function withSession(init?: RequestInit): RequestInit {
  const base: RequestInit = { ...(init || {}) };
  const headers = new Headers(init?.headers ?? {});
  if (activeSessionId) {
    headers.set("x-session-id", activeSessionId);
  }
  base.headers = headers;
  return base;
}

type TaskStatus = "queued" | "processing" | "completed" | "failed";

interface BucketConfig {
  maxBuckets: number | null;
  maxBucketBytes: number | null;
}

interface Task {
  id: string;
  name: string;
  status: TaskStatus;
  capabilityRequired: string;
  creditCost: number;
  inputType: string;
  metadataJson: string | null;
  totalChunks: number | null;
  processedChunks: number | null;
  totalItems?: number | null;
  processedItems?: number | null;
  progress: number | null;
  createdAt: string;
  creatorId?: string | null;
  workerId?: string | null;
  assignedWorkers?: string[];
  maxBucketBytes?: number | null;
  bucketConfig?: BucketConfig;
  nextChunkIndex?: number | null;
  costPerChunk?: number | null;
  budgetTotal?: number | null;
  budgetSpent?: number | null;
  maxBillableChunks?: number | null;
  chunksPaid?: number | null;
  platformFeePercent?: number | null;
  revoked?: boolean | null;
}

interface ItemResult {
  globalIndex: number | null;
  localIndex: number | null;
  status: string | null;
  inputPreview?: string | null;
  output?: string | null;
  error?: string | null;
}

interface BucketResult {
  id: string;
  taskId: string;
  chunkIndex: number;
  status: "completed" | "failed" | "skipped" | "processing";
  resultText: string | null;
  createdAt?: string;
  updatedAt?: string;
  rangeStart?: number | null;
  rangeEnd?: number | null;
  itemsCount?: number | null;
  bytesUsed?: number | null;
  output?: string | null;
  error?: string | null;
  itemResults?: ItemResult[];
  itemResultsTotal?: number | null;
  itemResultsTruncated?: boolean;
  processedItems?: number | null;
  workerId?: string | null;
}

interface BucketAssignment {
  taskId?: string | null;
  chunkIndex: number | null;
  workerId: string | null;
  assignedAt: string | null;
  expiresAt: string | null;
  rangeStart: number | null;
  rangeEnd: number | null;
  itemsCount: number | null;
  bytesUsed: number | null;
  processedCount?: number | null;
  progressRangeEnd?: number | null;
  updatedAt?: string | null;
  lastBatchOffset?: number | null;
  lastBatchSize?: number | null;
}

interface UserProfile {
  id: string;
  sessionId: string | null;
  walletBalance: number;
  roles: string[];
  createdAt?: string | null;
  updatedAt?: string | null;
}

interface WalletTransaction {
  id: string;
  type: string;
  amount: number;
  balanceAfter: number | null;
  createdAt?: string | null;
  meta?: Record<string, unknown> | null;
}

type WalletDepositResult =
  | { kind: "redirect"; redirectUrl: string }
  | { kind: "transaction"; user: UserProfile; transaction: WalletTransaction };

type WalletDepositHandler = (amount: number) => Promise<WalletDepositResult>;

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, withSession(init));
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed (${res.status})`);
  }
  return res.json();
}

async function listTasks(status?: string) {
  const query = status ? `?status=${encodeURIComponent(status)}` : "";
  const data = await fetchJSON<{ tasks: Task[] }>(`${API_BASE}/api/tasks${query}`);
  return data.tasks;
}

async function fetchTask(taskId: string) {
  return fetchJSON<{ task: Task; results: BucketResult[] }>(`${API_BASE}/api/tasks/${taskId}`);
}

async function createTask(formData: FormData) {
  const res = await fetch(
    `${API_BASE}/api/tasks`,
    withSession({
      method: "POST",
      body: formData,
    })
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to create task");
  }
  return res.json();
}

async function fetchCurrentUser(sessionId: string) {
  if (sessionId && sessionId !== activeSessionId) {
    setActiveSessionId(sessionId);
  }
  const res = await fetch(`${API_BASE}/api/me`, withSession());
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to load wallet");
  }
  return res.json() as Promise<{
    user: UserProfile;
    walletTransactions: WalletTransaction[];
    walletTransactionsTotal: number;
  }>;
}

async function depositToWallet(amount: number): Promise<WalletDepositResult> {
  let stripeError: Error | null = null;
  try {
    const res = await fetch(
      `${API_BASE}/api/stripe/create-checkout-session`,
      withSession({
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ amount }),
      })
    );

    if (res.status === 501) {
      stripeError = new Error("Stripe is not configured on the server. Provide STRIPE credentials or enable sandbox mode.");
    } else {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || "Failed to create Stripe Checkout session");
      }
      const data = await res.json();
      if (data?.url) {
        return { kind: "redirect", redirectUrl: data.url };
      }
      if (data?.id) {
        return { kind: "redirect", redirectUrl: `https://checkout.stripe.com/pay/${data.id}` };
      }
      throw new Error("Invalid response from Stripe session creation");
    }
  } catch (error) {
    if (!stripeError) {
      stripeError = error instanceof Error ? error : new Error("Failed to start Stripe checkout");
    }
  }

  if (!SANDBOX_WALLET_ENABLED) {
    if (stripeError) throw stripeError;
    throw new Error("Manual wallet adjustments are disabled. Configure Stripe to add funds.");
  }

  if (stripeError) {
    console.warn("Stripe checkout unavailable; falling back to sandbox wallet flow.", stripeError);
  }

  const response = await fetchJSON<{ ok: boolean; user: UserProfile; transaction: WalletTransaction }>(
    `${API_BASE}/api/wallet/deposit`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ amount, reason: "ui-deposit" }),
    }
  );
  return { kind: "transaction", user: response.user, transaction: response.transaction };
}

async function withdrawFromWallet(amount: number) {
  return fetchJSON<{ ok: boolean; user: UserProfile; transaction: WalletTransaction }>(`${API_BASE}/api/wallet/withdraw`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ amount, reason: "ui-withdraw" }),
  });
}

async function claimTask(taskId: string, workerId?: string) {
  const init: RequestInit = {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: workerId ? JSON.stringify({ workerId }) : JSON.stringify({}),
  };
  const res = await fetch(`${API_BASE}/api/tasks/${taskId}/claim`, withSession(init));
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Failed to claim task ${taskId}`);
  }
  return res.json();
}

async function dropTask(taskId: string, workerId?: string) {
  const init: RequestInit = {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: workerId ? JSON.stringify({ workerId }) : JSON.stringify({}),
  };
  const res = await fetch(`${API_BASE}/api/tasks/${taskId}/drop`, withSession(init));
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Failed to drop task ${taskId}`);
  }
  return res.json();
}

async function fetchBucketResults(taskId: string) {
  const data = await fetchJSON<{ results: BucketResult[]; assignments?: BucketAssignment[] }>(`${API_BASE}/api/tasks/${taskId}/results`);
  return {
    results: data.results,
    assignments: Array.isArray(data.assignments) ? data.assignments : [],
  };
}

async function fetchWorkerOnline(workerId: string) {
  return fetchJSON<{ online: boolean; lastHeartbeat?: string }>(`${API_BASE}/api/worker/online/${encodeURIComponent(workerId)}`);
}

function formatStatus(status: TaskStatus) {
  switch (status) {
    case "queued":
      return "Queued";
    case "processing":
      return "Processing";
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    default:
      return status;
  }
}

function formatCapability(capability: string) {
  return capability
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatCurrency(value: number | null | undefined, fallback = "—") {
  if (typeof value !== "number" || Number.isNaN(value)) return fallback;
  return `$${value.toFixed(2)}`;
}

function formatSignedCurrency(value: number | null | undefined) {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${formatCurrency(Math.abs(value))}`;
}

function formatTransactionType(value?: string) {
  if (!value) return "Transaction";
  return value
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatNumber(value: number | null | undefined, decimals = 0) {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toFixed(decimals);
}

function ProgressBar({ value }: { value: number | null }) {
  if (value === null || Number.isNaN(value)) return null;
  return (
    <div className="progress">
      <div className="progress-value" style={{ width: `${Math.min(100, value)}%` }} />
    </div>
  );
}

function TaskCard({ task, onSelect, onClaim, onDrop, isActive }: { task: Task; onSelect?: (task: Task) => void; onClaim?: (task: Task) => void; onDrop?: (task: Task) => void; isActive?: boolean }) {
  return (
    <div className={`task-card${isActive ? " active" : ""}`}>
      <h3 className="task-name">{task.name}</h3>
      <div className="task-header">
        <span className={`status status-${task.status}`}>{formatStatus(task.status)}</span>
        <span className="capability">{formatCapability(task.capabilityRequired)}</span>
        <span className="credits">{task.creditCost} credits</span>
      </div>
      <ProgressBar value={task.progress ?? null} />
      <div className="task-meta">
        <span>
          Created {new Date(task.createdAt).toLocaleString()} · ID: {task.id}
        </span>
        {typeof task.processedChunks === "number" && typeof task.totalChunks === "number" && (
          <span>
            {task.processedChunks}/{task.totalChunks} chunks
          </span>
        )}
      </div>
      <div className="task-actions">
        {onSelect && (
          <button className="btn" onClick={() => onSelect(task)}>
            View
          </button>
        )}
        {onClaim && (
          <button className="btn primary" onClick={() => onClaim(task)}>
            Claim
          </button>
        )}
        {onDrop && (
          <button className="btn" onClick={() => onDrop(task)}>
            Drop
          </button>
        )}
      </div>
    </div>
  );
}

function bucketTextToLines(result?: string | null) {
  if (!result) return { input: "", output: "" };
  const inputMatch = result.match(/\[input\]\s*([\s\S]*?)(?:\n\[|$)/i);
  const outputMatch = result.match(/\[output\]\s*([\s\S]*)/i);
  return {
    input: inputMatch?.[1]?.trim() ?? "",
    output: outputMatch?.[1]?.trim() ?? "",
  };
}

function getItemsCount(result: BucketResult): number | null {
  if (typeof result.itemsCount === "number" && Number.isFinite(result.itemsCount)) return result.itemsCount;
  if (typeof result.itemResultsTotal === "number" && Number.isFinite(result.itemResultsTotal)) return result.itemResultsTotal;
  if (typeof result.rangeStart === "number" && typeof result.rangeEnd === "number") {
    return Math.max(0, result.rangeEnd - result.rangeStart);
  }
  if (Array.isArray(result.itemResults)) return result.itemResults.length;
  return null;
}

function getProcessedItems(result: BucketResult): number | null {
  if (typeof result.processedItems === "number" && Number.isFinite(result.processedItems)) return result.processedItems;
  if (typeof result.itemResultsTotal === "number" && Number.isFinite(result.itemResultsTotal)) return result.itemResultsTotal;
  if (typeof result.rangeStart === "number" && typeof result.rangeEnd === "number") {
    return Math.max(0, result.rangeEnd - result.rangeStart);
  }
  return null;
}

function formatItemsCount(count: number | null) {
  if (count === null) return null;
  return `${count} item${count === 1 ? "" : "s"}`;
}

function formatRangeLabel(start?: number | null, end?: number | null) {
  if (typeof start === "number" && typeof end === "number") {
    if (end > start) return `items ${start}-${end - 1}`;
    if (end === start) return `item ${start}`;
  }
  return null;
}

function formatBytes(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  const formatted = unitIndex === 0 ? size.toString() : size.toFixed(size >= 10 ? 0 : 1);
  return `${formatted} ${units[unitIndex]}`;
}

function formatItemLabel(item: ItemResult, fallback: number) {
  if (typeof item.globalIndex === "number") return `#${item.globalIndex}`;
  if (typeof item.localIndex === "number") return `item ${item.localIndex}`;
  return `item ${fallback}`;
}

function formatTimestamp(value?: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString();
}

function ItemDetails({
  label,
  items,
  truncated,
  formatter,
}: {
  label: string;
  items: ItemResult[];
  truncated: boolean;
  formatter: (item: ItemResult, idx: number) => string;
}) {
  if (!items.length) return null;
  return (
    <details>
      <summary>
        {label} ({items.length}
        {truncated ? "+" : ""})
      </summary>
      <ul className="item-list">
        {items.map((item, idx) => {
          const key = item.globalIndex !== null ? `g-${item.globalIndex}` : `l-${item.localIndex ?? idx}`;
          const content = formatter(item, idx) || "—";
          return (
            <li key={key}>
              <span className={`item-status status-${item.status}`}>{item.status}</span>
              <span className="item-label">{formatItemLabel(item, idx)}</span>
              <code className="item-preview">{content}</code>
            </li>
          );
        })}
      </ul>
      {truncated ? <div className="muted small-note">Additional items truncated.</div> : null}
    </details>
  );
}

function BucketResultsTable({ results }: { results: BucketResult[] }) {
  if (!results.length) {
    return <p className="muted">No chunk results yet.</p>;
  }
  return (
    <table className="results">
      <thead>
        <tr>
          <th>Chunk</th>
          <th>Status</th>
          <th>Input</th>
          <th>Output</th>
        </tr>
      </thead>
      <tbody>
        {results.map((result) => {
          const items = Array.isArray(result.itemResults) ? result.itemResults : [];
          const lines = bucketTextToLines(result.resultText);
          const rangeLabel = formatRangeLabel(result.rangeStart, result.rangeEnd);
          const countLabel = formatItemsCount(getItemsCount(result));
          const bytesLabel = formatBytes(result.bytesUsed);
          const metaParts = [rangeLabel, countLabel, bytesLabel].filter((part): part is string => Boolean(part));
          return (
            <tr key={result.id}>
              <td>#{result.chunkIndex}</td>
              <td>{result.status}</td>
              <td>
                <div className="bucket-line">Chunk #{result.chunkIndex}</div>
                {metaParts.length ? <div className="bucket-meta-line">{metaParts.join(' | ')}</div> : null}
                {items.length ? (
                  <ItemDetails
                    label="Inputs"
                    items={items}
                    truncated={Boolean(result.itemResultsTruncated)}
                    formatter={(item) => item.inputPreview || ''}
                  />
                ) : (
                  <pre>{lines.input || "—"}</pre>
                )}
              </td>
              <td>
                {result.output ? <div className="bucket-meta-line">{result.output}</div> : null}
                {items.length ? (
                  <ItemDetails
                    label="Outputs"
                    items={items}
                    truncated={Boolean(result.itemResultsTruncated)}
                    formatter={(item) =>
                      item.status === "failed"
                        ? item.error || item.output || "Failed"
                        : item.output || "—"
                    }
                  />
                ) : (
                  <pre>{lines.output || "—"}</pre>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function BucketProgressBar({
  task,
  results,
  assignments,
}: {
  task: Task | null;
  results: BucketResult[];
  assignments: BucketAssignment[];
}) {
  const segments = useMemo<Array<{
    index: number | null;
    status: "completed" | "processing" | "assigned" | "pending";
    processed: number;
    total: number | null;
  }>>(() => {
    if (!task) return [];

    const finishedStatuses = new Set<BucketResult["status"]>(["completed", "failed", "skipped"]);
    const bucketMap = new Map<number, {
      processed: number;
      total: number | null;
      status: "completed" | "processing" | "assigned";
    }>();

    results.forEach((result) => {
      if (typeof result.chunkIndex !== "number" || Number.isNaN(result.chunkIndex)) return;
      const idx = result.chunkIndex;
      const total = getItemsCount(result);
      const processed = getProcessedItems(result) ?? (total ?? 0);
      const status = finishedStatuses.has(result.status) ? "completed" : "processing";
      bucketMap.set(idx, {
        processed: Math.max(0, processed ?? 0),
        total: total ?? null,
        status,
      });
    });

    assignments.forEach((assignment) => {
      if (typeof assignment.chunkIndex !== "number" || Number.isNaN(assignment.chunkIndex)) return;
      const idx = assignment.chunkIndex;
      const processed = typeof assignment.processedCount === "number" && Number.isFinite(assignment.processedCount)
        ? Math.max(0, assignment.processedCount)
        : null;
      const processedFromRange = typeof assignment.rangeStart === "number"
        && Number.isFinite(assignment.rangeStart)
        && typeof assignment.progressRangeEnd === "number"
        && Number.isFinite(assignment.progressRangeEnd)
        ? Math.max(0, assignment.progressRangeEnd - assignment.rangeStart)
        : null;
      const totalFromAssignment = typeof assignment.itemsCount === "number" && Number.isFinite(assignment.itemsCount)
        ? Math.max(0, assignment.itemsCount)
        : null;
      const existing = bucketMap.get(idx);
      const total = totalFromAssignment ?? existing?.total ?? null;
      const normalizedProcessed = processed ?? processedFromRange ?? existing?.processed ?? 0;
      if (existing && existing.status === "completed") {
        if (normalizedProcessed > existing.processed) {
          existing.processed = normalizedProcessed;
        }
        if (total !== null) existing.total = total;
        return;
      }
      const status: "completed" | "processing" | "assigned" = normalizedProcessed > 0 ? "processing" : "assigned";
      bucketMap.set(idx, {
        processed: normalizedProcessed,
        total,
        status,
      });
    });

    const totalChunksConfigured = Number.isFinite(Number(task.totalChunks))
      ? Math.max(0, Math.floor(Number(task.totalChunks)))
      : null;
    const maxBillableChunks = Number.isFinite(Number(task.maxBillableChunks))
      ? Math.max(0, Math.floor(Number(task.maxBillableChunks)))
      : null;
    const bucketConcurrencyRaw = task.bucketConfig?.maxBuckets;
    const bucketConcurrency = Number.isFinite(Number(bucketConcurrencyRaw))
      ? Math.max(0, Math.floor(Number(bucketConcurrencyRaw)))
      : null;
    const segmentsList: Array<{ index: number | null; status: "completed" | "processing" | "assigned" | "pending"; processed: number; total: number | null; }> = [];
    const seen = new Set<number>();

    bucketMap.forEach((value, key) => {
      segmentsList.push({
        index: key,
        status: value.status,
        processed: value.processed,
        total: value.total,
      });
      seen.add(key);
    });

    const highestObservedIndex = seen.size > 0 ? Math.max(...Array.from(seen)) : null;
    const segmentCount = totalChunksConfigured
      ?? maxBillableChunks
      ?? (highestObservedIndex !== null ? highestObservedIndex + 1 : null)
      ?? bucketConcurrency
      ?? null;

    if (segmentCount !== null && segmentCount > 0) {
      for (let i = 0; i < segmentCount; i += 1) {
        if (!seen.has(i)) {
          segmentsList.push({
            index: i,
            status: "pending",
            processed: 0,
            total: null,
          });
        }
      }
    }

    segmentsList.sort((a, b) => {
      const aIdx = typeof a.index === "number" && !Number.isNaN(a.index) ? a.index : Number.MAX_SAFE_INTEGER;
      const bIdx = typeof b.index === "number" && !Number.isNaN(b.index) ? b.index : Number.MAX_SAFE_INTEGER;
      if (aIdx !== bIdx) return aIdx - bIdx;
      const order = { completed: 0, processing: 1, assigned: 2, pending: 3 } as const;
      return order[a.status] - order[b.status];
    });

    return segmentsList;
  }, [task, results, assignments]);

  if (!task || segments.length === 0) return null;

  return (
    <div className="bucket-progress-bar" role="list" aria-label="Chunk progress overview">
      {segments.map((segment) => {
        const total = typeof segment.total === "number" && Number.isFinite(segment.total) ? Math.max(0, segment.total) : null;
        const processed = Math.max(0, segment.processed ?? 0);
        let fillPercent = 0;
        if (total && total > 0) {
          fillPercent = Math.min(100, Math.round((processed / total) * 100));
        } else if (segment.status === "completed") {
          fillPercent = 100;
        } else if (segment.status === "processing") {
          fillPercent = processed > 0 ? 60 : 35;
        }
        const hasIndex = typeof segment.index === "number" && !Number.isNaN(segment.index);
        const displayLabel = hasIndex ? `#${segment.index}` : "—";
        const titleParts = [
          hasIndex ? `Chunk #${segment.index}` : "Unnumbered chunk",
          total !== null ? `${processed}/${total} item(s)` : `${processed} item(s)`,
          segment.status.charAt(0).toUpperCase() + segment.status.slice(1),
        ];
        return (
          <div
            key={`${hasIndex ? segment.index : "pending"}-${segment.status}`}
            className={`bucket-progress-segment status-${segment.status}`}
            style={{ flexGrow: total ?? 1, flexBasis: 0 }}
            role="listitem"
            title={titleParts.join(" · ")}
          >
            <div className="bucket-progress-fill" style={{ width: `${fillPercent}%` }} />
            <span className="bucket-progress-label">{displayLabel}</span>
          </div>
        );
      })}
    </div>
  );
}

function BucketSummaryPanel({
  task,
  results,
  assignments,
}: {
  task: Task | null;
  results: BucketResult[];
  assignments: BucketAssignment[];
}) {
  const summaryRows = useMemo(() => {
    if (!task) return [];

    type SummaryRow = {
      key: string;
      chunkIndex: number | null;
      status: string;
      rangeStart: number | null;
      rangeEnd: number | null;
      itemsCount: number | null;
      processedCount: number | null;
      bytesUsed: number | null;
      workerId: string | null;
      timestamp: string | null;
    };

    const rows: SummaryRow[] = [];
    const assignmentByChunk = new Map<number, BucketAssignment>();
    const leftoverAssignments: BucketAssignment[] = [];

    assignments.forEach((assignment) => {
      if (typeof assignment.chunkIndex === "number") {
        assignmentByChunk.set(assignment.chunkIndex, assignment);
      } else {
        leftoverAssignments.push(assignment);
      }
    });

    results.forEach((result) => {
      const assignment = typeof result.chunkIndex === "number" ? assignmentByChunk.get(result.chunkIndex) : undefined;
      if (assignment && typeof result.chunkIndex === "number") {
        assignmentByChunk.delete(result.chunkIndex);
      }
      const total = getItemsCount(result);
      const processedFromResult = getProcessedItems(result);
      const processedFromAssignment = assignment && typeof assignment.processedCount === "number" && Number.isFinite(assignment.processedCount)
        ? assignment.processedCount
        : null;
      const processedFromRange = assignment
        && typeof assignment.rangeStart === "number"
        && Number.isFinite(assignment.rangeStart)
        && typeof assignment.progressRangeEnd === "number"
        && Number.isFinite(assignment.progressRangeEnd)
        ? Math.max(0, assignment.progressRangeEnd - assignment.rangeStart)
        : null;
      const processedCandidates = [processedFromResult, processedFromAssignment].filter(
        (value): value is number => typeof value === "number" && Number.isFinite(value)
      );
      const rangeCandidates = typeof processedFromRange === "number" && Number.isFinite(processedFromRange)
        ? processedCandidates.concat(processedFromRange)
        : processedCandidates;
      const processedCount = rangeCandidates.length
        ? Math.max(...rangeCandidates)
        : processedFromResult ?? processedFromAssignment ?? processedFromRange ?? null;

      const rangeStart = typeof result.rangeStart === "number"
        ? result.rangeStart
        : assignment && typeof assignment.rangeStart === "number"
        ? assignment.rangeStart
        : null;
      const rangeEnd = typeof result.rangeEnd === "number"
        ? result.rangeEnd
        : assignment && typeof assignment.rangeEnd === "number"
        ? assignment.rangeEnd
        : null;
      const itemsCount = typeof total === "number"
        ? total
        : assignment && typeof assignment.itemsCount === "number"
        ? assignment.itemsCount
        : null;
      const bytesUsed = typeof result.bytesUsed === "number"
        ? result.bytesUsed
        : assignment && typeof assignment.bytesUsed === "number"
        ? assignment.bytesUsed
        : null;

      rows.push({
        key: `result-${result.id}`,
        chunkIndex: typeof result.chunkIndex === "number" ? result.chunkIndex : null,
        status: result.status,
        rangeStart,
        rangeEnd,
        itemsCount,
        processedCount,
        bytesUsed,
        workerId: assignment?.workerId || null,
        timestamp: result.updatedAt || result.createdAt || assignment?.updatedAt || assignment?.assignedAt || null,
      });
    });

    [...assignmentByChunk.values(), ...leftoverAssignments].forEach((assignment, index) => {
      const processedFromCount = typeof assignment.processedCount === "number" && Number.isFinite(assignment.processedCount)
        ? assignment.processedCount
        : null;
      const processedFromRange = typeof assignment.rangeStart === "number"
        && Number.isFinite(assignment.rangeStart)
        && typeof assignment.progressRangeEnd === "number"
        && Number.isFinite(assignment.progressRangeEnd)
        ? Math.max(0, assignment.progressRangeEnd - assignment.rangeStart)
        : null;
      const normalizedProcessed = processedFromCount ?? processedFromRange ?? null;
      const itemsCount = typeof assignment.itemsCount === "number" && Number.isFinite(assignment.itemsCount)
        ? assignment.itemsCount
        : null;
      const status = normalizedProcessed !== null && normalizedProcessed > 0 ? "processing" : "assigned";
      rows.push({
        key: `assignment-${assignment.chunkIndex ?? index}-${assignment.assignedAt ?? "na"}`,
        chunkIndex: typeof assignment.chunkIndex === "number" ? assignment.chunkIndex : null,
        status,
        rangeStart: typeof assignment.rangeStart === "number" ? assignment.rangeStart : null,
        rangeEnd: typeof assignment.rangeEnd === "number" ? assignment.rangeEnd : null,
        itemsCount,
        processedCount: normalizedProcessed,
        bytesUsed: typeof assignment.bytesUsed === "number" ? assignment.bytesUsed : null,
        workerId: assignment.workerId,
        timestamp: assignment.updatedAt || assignment.assignedAt || null,
      });
    });

    return rows.sort((a, b) => {
      const aIndex = typeof a.chunkIndex === "number" ? a.chunkIndex : Number.MAX_SAFE_INTEGER;
      const bIndex = typeof b.chunkIndex === "number" ? b.chunkIndex : Number.MAX_SAFE_INTEGER;
      if (aIndex !== bIndex) return aIndex - bIndex;
      const aTime = a.timestamp ? new Date(a.timestamp).getTime() : Number.MAX_SAFE_INTEGER;
      const bTime = b.timestamp ? new Date(b.timestamp).getTime() : Number.MAX_SAFE_INTEGER;
      return aTime - bTime;
    });
  }, [task, results, assignments]);

  if (!task) return null;

  const configuredChunks = Number.isFinite(Number(task.totalChunks))
    ? Math.max(0, Math.floor(Number(task.totalChunks)))
    : Number.isFinite(Number(task.maxBillableChunks))
    ? Math.max(0, Math.floor(Number(task.maxBillableChunks)))
    : null;
  const bucketSlotsRaw = task.bucketConfig?.maxBuckets;
  const bucketSlots = Number.isFinite(Number(bucketSlotsRaw))
    ? Math.max(0, Math.floor(Number(bucketSlotsRaw)))
    : configuredChunks;
  const configBytes = task.bucketConfig?.maxBucketBytes ?? task.maxBucketBytes ?? null;
  const assignedCount = typeof task.nextChunkIndex === "number"
    ? task.nextChunkIndex
    : summaryRows.length;
  const completedStatuses: Array<BucketResult["status"]> = ["completed", "failed", "skipped"];
  const completedCount = results.filter((result) => completedStatuses.includes(result.status)).length;
  const inProgressCount = summaryRows.filter((row) => row.status === "processing").length;
  const activeAssignments = assignments.length;
  const processedItems = typeof task.processedItems === "number" && Number.isFinite(task.processedItems)
    ? task.processedItems
    : null;
  const totalItems = typeof task.totalItems === "number" && Number.isFinite(task.totalItems)
    ? task.totalItems
    : null;
  const processedItemsPct = totalItems && totalItems > 0 && processedItems !== null
    ? Math.round((processedItems / totalItems) * 100)
    : null;
  const processedItemsLabel = totalItems !== null
    ? `${processedItems ?? 0}/${totalItems}${processedItemsPct !== null ? ` (${processedItemsPct}%)` : ""}`
    : processedItems !== null
    ? `${processedItems}`
    : "—";

  return (
    <div className="bucket-summary">
      <BucketProgressBar task={task} results={results} assignments={assignments} />
      <div className="bucket-overview">
        <div><strong>Configured max bytes:</strong> {formatBytes(configBytes) ?? "—"}</div>
        <div><strong>Chunks assigned:</strong> {assignedCount}</div>
        <div><strong>Completed chunks:</strong> {completedCount}</div>
        <div><strong>Active assignments:</strong> {activeAssignments}</div>
        <div><strong>Processed items:</strong> {processedItemsLabel}</div>
      </div>
      {summaryRows.length === 0 ? (
        <p className="muted small-note">No chunk history yet.</p>
      ) : (
        <table className="bucket-table">
          <thead>
            <tr>
              <th>Chunk</th>
              <th>Status</th>
              <th>Range</th>
              <th>Progress</th>
              <th>Items</th>
              <th>Size</th>
              <th>Worker</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {summaryRows.map((row, index) => {
              const key = row.key || `${row.status}-${row.chunkIndex ?? index}`;
              const range = formatRangeLabel(row.rangeStart, row.rangeEnd) || "—";
              const totalItemsLabel = formatItemsCount(row.itemsCount) || "—";
              const size = formatBytes(row.bytesUsed) || "—";
              const worker = row.workerId || "—";
              const updated = formatTimestamp(row.timestamp);
              const processed = typeof row.processedCount === "number" && Number.isFinite(row.processedCount)
                ? row.processedCount
                : null;
              const totalItems = typeof row.itemsCount === "number" && Number.isFinite(row.itemsCount)
                ? row.itemsCount
                : null;
              let progressLabel = "—";
              if (processed !== null && totalItems !== null) {
                const safeProcessed = Math.min(processed, totalItems);
                const pct = totalItems > 0 ? Math.round((safeProcessed / totalItems) * 100) : null;
                const safePct = pct !== null && Number.isFinite(pct) ? Math.min(100, Math.max(0, pct)) : null;
                progressLabel = safePct !== null
                  ? `${safeProcessed}/${totalItems} (${safePct}%)`
                  : `${safeProcessed}/${totalItems}`;
              } else if (processed !== null) {
                progressLabel = `${processed}`;
              }
              return (
                <tr key={key}>
                  <td>{typeof row.chunkIndex === "number" ? `#${row.chunkIndex}` : "—"}</td>
                  <td>{row.status}</td>
                  <td>{range}</td>
                  <td>{progressLabel}</td>
                  <td>{totalItemsLabel}</td>
                  <td>{size}</td>
                  <td>{worker}</td>
                  <td>{updated}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function WalletSection({
  profile,
  transactions,
  total,
  loading,
  onRefresh,
  sessionId,
  onDeposit,
  onWithdraw,
}: {
  profile: UserProfile | null;
  transactions: WalletTransaction[];
  total: number;
  loading: boolean;
  onRefresh: () => void;
  sessionId: string;
  onDeposit: WalletDepositHandler;
  onWithdraw: (amount: number) => Promise<WalletTransaction>;
}) {
  const displayBalance = typeof profile?.walletBalance === "number" ? profile.walletBalance : 0;
  const sessionLabel = profile?.sessionId || sessionId;
  const [amountInput, setAmountInput] = useState<string>("");
  const [action, setAction] = useState<"deposit" | "withdraw" | null>(null);
  const busyRef = useRef(false);
  const manualWalletEnabled = SANDBOX_WALLET_ENABLED;

  const handleAction = useCallback(
    async (mode: "deposit" | "withdraw") => {
      if (loading || busyRef.current) return;
      if (!manualWalletEnabled && mode === "withdraw") {
        toast.info("Withdrawals require manual review. Please contact support.");
        return;
      }
      const parsed = Number(amountInput);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        toast.error("Enter an amount greater than zero.");
        return;
      }
      const normalized = Math.round(parsed * 100) / 100;
      if (normalized <= 0) {
        toast.error("Amount is too small.");
        return;
      }
      try {
        busyRef.current = true;
        setAction(mode);
        if (mode === "deposit") {
          const outcome = await onDeposit(normalized);
          if (outcome.kind === "redirect") {
            toast.message("Redirecting to Stripe Checkout…");
            window.location.href = outcome.redirectUrl;
            return;
          }
          const absoluteAmount = Math.abs(outcome.transaction.amount);
          toast.success(`Added ${formatCurrency(absoluteAmount, "$0.00")} to wallet`);
          setAmountInput("");
        } else {
          const transaction = await onWithdraw(normalized);
          const absoluteAmount = Math.abs(transaction.amount);
          toast.success(`Withdrew ${formatCurrency(absoluteAmount, "$0.00")} from wallet`);
          setAmountInput("");
        }
      } catch (error: any) {
        const message = error?.message || (mode === "deposit" ? "Deposit failed" : "Withdrawal failed");
        toast.error(message);
      } finally {
        busyRef.current = false;
        setAction(null);
      }
    },
    [amountInput, loading, onDeposit, onWithdraw, busyRef, manualWalletEnabled]
  );

  const isBusy = loading || action !== null;
  return (
    <section className="wallet-section">
      <div className="wallet-section-header">
        <h2>Wallet</h2>
        <button type="button" className="btn" onClick={onRefresh} disabled={loading}>
          {loading ? "Refreshing..." : "Refresh"}
        </button>
      </div>
      <div className="card wallet-summary-card">
        <div className="wallet-balance">
          <span className="wallet-label">Current balance</span>
          <span className="wallet-amount">{formatCurrency(displayBalance, "$0.00")}</span>
        </div>
        <div className="wallet-details">
          <div>
            <span className="wallet-detail-label">Session ID</span>
            <code>{sessionLabel}</code>
          </div>
          <div>
            <span className="wallet-detail-label">Roles</span>
            <span>{profile?.roles?.length ? profile.roles.join(", ") : "customer, worker"}</span>
          </div>
          <div>
            <span className="wallet-detail-label">Updated</span>
            <span>{formatTimestamp(profile?.updatedAt || profile?.createdAt || null)}</span>
          </div>
        </div>
      </div>
      <div className="card wallet-actions-card">
        <label className="wallet-amount-input">
          Amount (USD)
          <input
            type="number"
            min="0"
            step="0.01"
            value={amountInput}
            onChange={(event) => setAmountInput(event.target.value)}
            placeholder="50.00"
            disabled={isBusy}
          />
        </label>
        <div className="wallet-action-buttons">
          <button
            type="button"
            className="btn primary"
            onClick={() => handleAction("deposit")}
            disabled={isBusy}
          >
            {action === "deposit" ? "Adding..." : "Add funds"}
          </button>
          <button
            type="button"
            className="btn"
            onClick={() => handleAction("withdraw")}
            disabled={isBusy || !manualWalletEnabled}
          >
            {action === "withdraw" ? "Withdrawing..." : "Withdraw"}
          </button>
        </div>
        <small className="muted">
          {manualWalletEnabled
            ? "Sandbox mode enabled: use instant balance tweaks for local testing."
            : "Stripe Checkout handles deposits; wallet updates after successful payment."}
        </small>
      </div>
      <div className="card wallet-history-card">
        <div className="wallet-history-header">
          <h3>Recent activity</h3>
          {total > transactions.length ? (
            <span className="wallet-history-note">Showing latest {transactions.length} of {total}</span>
          ) : null}
        </div>
        {transactions.length === 0 ? (
          <p className="muted">No wallet history yet. Create or complete tasks to see activity.</p>
        ) : (
          <ul className="wallet-transaction-list">
            {transactions.map((tx) => {
              const createdLabel = formatTimestamp(tx.createdAt || null);
              const balanceAfter = typeof tx.balanceAfter === "number" && !Number.isNaN(tx.balanceAfter)
                ? formatCurrency(tx.balanceAfter, "—")
                : "—";
              const meta = typeof tx.meta === "object" && tx.meta !== null ? (tx.meta as Record<string, unknown>) : null;
              const details: string[] = [formatTransactionType(tx.type)];
              if (meta) {
                const taskIdValue = meta["taskId"];
                const chunkIndexValue = meta["chunkIndex"];
                const reasonValue = meta["reason"];
                if (typeof taskIdValue === "string" && taskIdValue.trim().length > 0) {
                  details.push(`Task ${taskIdValue}`);
                }
                if (typeof chunkIndexValue === "number" && Number.isFinite(chunkIndexValue)) {
                  details.push(`Chunk ${chunkIndexValue}`);
                }
                if (typeof reasonValue === "string" && reasonValue.trim().length > 0) {
                  details.push(formatTransactionType(reasonValue));
                }
              }
              return (
                <li key={tx.id} className="wallet-transaction-item">
                  <div className="wallet-transaction-text">
                    <div className="wallet-transaction-type">{details.join(" | ")}</div>
                    <div className="wallet-transaction-meta">
                      <span>{createdLabel}</span>
                      <span>Balance: {balanceAfter}</span>
                    </div>
                  </div>
                  <div
                    className={`wallet-transaction-amount ${tx.amount >= 0 ? "positive" : tx.amount < 0 ? "negative" : "neutral"}`}
                  >
                    {formatSignedCurrency(tx.amount)}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </section>
  );
}

function CustomerView({
  sessionId,
  wallet,
  walletTransactions,
  walletTransactionsTotal,
  onRefreshWallet,
  walletLoading,
  onDeposit,
  onWithdraw,
}: {
  sessionId: string;
  wallet: UserProfile | null;
  walletTransactions: WalletTransaction[];
  walletTransactionsTotal: number;
  onRefreshWallet: () => void;
  walletLoading: boolean;
  onDeposit: WalletDepositHandler;
  onWithdraw: (amount: number) => Promise<WalletTransaction>;
}) {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [results, setResults] = useState<BucketResult[]>([]);
  const [assignments, setAssignments] = useState<BucketAssignment[]>([]);
  const [loading, setLoading] = useState(false);
  const [inputMode, setInputMode] = useState<"file" | "database">("file");
  const [dbUri, setDbUri] = useState("");
  const [dbName, setDbName] = useState("");
  const [dbCollection, setDbCollection] = useState("");
  const [dbAttached, setDbAttached] = useState(false);
  const [metadataJsonValue, setMetadataJsonValue] = useState("");
  const [activeTab, setActiveTab] = useState<"wallet" | "submit" | "tasks" | "details">("wallet");

  const refreshTasks = async () => {
    try {
      const data = await listTasks();
      // show only tasks created by this session in the Customer "My Tasks" list
      const mine = data.filter((t) => (t.creatorId || null) === sessionId);
      setTasks(mine);
      if (selectedTask) {
        const found = data.find((t) => t.id === selectedTask.id);
        if (found) {
          setSelectedTask(found);
        } else {
          setSelectedTask(null);
          setResults([]);
          setAssignments([]);
        }
      }
    } catch (error: any) {
      toast.error(error.message || "Failed to fetch tasks");
    }
  };

  useEffect(() => {
    refreshTasks();
    const interval = setInterval(refreshTasks, 4000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (!selectedTask && tasks.length > 0) {
      setSelectedTask(tasks[0]);
    }
  }, [tasks, selectedTask]);

  useEffect(() => {
    if (activeTab === "details" && !selectedTask) {
      setActiveTab(tasks.length > 0 ? "tasks" : "submit");
    }
  }, [activeTab, selectedTask, tasks.length]);

  useEffect(() => {
    if (!selectedTask) {
      setResults([]);
      setAssignments([]);
      return;
    }
    let cancelled = false;
    const load = async () => {
      try {
        const data = await fetchBucketResults(selectedTask.id);
        if (!cancelled) {
          setResults(data.results);
          setAssignments(data.assignments);
        }
      } catch (error) {
        if (!cancelled) {
          console.error("Failed to load chunk results", error);
        }
      }
    };

    load();
    const timer = setInterval(load, 4000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [selectedTask?.id, selectedTask?.processedChunks, selectedTask?.status]);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const nameValue = (formData.get("name") as string | null)?.trim() ?? "";
    if (!nameValue) {
      toast.error("Please enter a task name");
      return;
    }
    formData.set("name", nameValue);
    formData.set("inputType", inputMode === "database" ? "database" : "file");
    // include creator session id so backend can associate tasks with a creator
    formData.set("creatorId", sessionId);
    const codeFile = formData.get("code") as File | null;
    if (!codeFile || !codeFile.name.endsWith(".zip")) {
      toast.error("Please upload a code.zip file containing main.js");
      return;
    }
    if (inputMode === "database") {
      if (!dbAttached) {
        toast.error("Attach the database connection before submitting");
        return;
      }
      const trimmedUri = dbUri.trim();
      const trimmedDb = dbName.trim();
      const trimmedCollection = dbCollection.trim();
      if (!trimmedUri || !trimmedDb || !trimmedCollection) {
        toast.error("Database connection fields are required");
        return;
      }
      const metadata = metadataJsonValue || JSON.stringify({
        type: "mongodb",
        uri: trimmedUri,
        database: trimmedDb,
        collection: trimmedCollection,
      });
      formData.set("metadataJson", metadata);
      formData.delete("data");
    } else {
      formData.set("metadataJson", "");
    }
    setLoading(true);
    try {
      await createTask(formData);
      toast.success("Task submitted");
      form.reset();
      setInputMode("file");
      setDbUri("");
      setDbName("");
      setDbCollection("");
      setDbAttached(false);
      setMetadataJsonValue("");
      await refreshTasks();
    } catch (error: any) {
      toast.error(error.message || "Failed to submit task");
    } finally {
      setLoading(false);
    }
  };

  const handleAttachDatabase = () => {
    const trimmedUri = dbUri.trim();
    const trimmedDb = dbName.trim();
    const trimmedCollection = dbCollection.trim();
    if (!trimmedUri || !trimmedDb || !trimmedCollection) {
      toast.error("Fill in URI, database, and collection to attach");
      return;
    }
    const metadata = {
      type: "mongodb",
      uri: trimmedUri,
      database: trimmedDb,
      collection: trimmedCollection,
    };
    setMetadataJsonValue(JSON.stringify(metadata));
    setDbAttached(true);
    toast.success("Database attached");
  };

  const revokeTask = async (taskId: string) => {
    try {
      const res = await fetch(`${API_BASE}/api/tasks/${taskId}/revoke`, withSession({ method: 'POST' }));
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || 'Failed to revoke task');
      }
      const data = await res.json();
      setSelectedTask(data.task || null);
      toast.success('Task revoked — workers will stop receiving new chunks');
      await refreshTasks();
    } catch (err: any) {
      toast.error(err?.message || 'Failed to revoke task');
    }
  };

  const reinvokeTask = async (taskId: string) => {
    try {
      const res = await fetch(`${API_BASE}/api/tasks/${taskId}/reinvoke`, withSession({ method: 'POST' }));
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || 'Failed to reinvoke task');
      }
      const data = await res.json();
      setSelectedTask(data.task || null);
      toast.success('Task reinvoked — workers may claim it again');
      await refreshTasks();
    } catch (err: any) {
      toast.error(err?.message || 'Failed to reinvoke task');
    }
  };

  const deleteTask = async (taskId: string) => {
    if (!confirm('Delete this task and all related files/results? This cannot be undone.')) return;
    try {
      const res = await fetch(`${API_BASE}/api/tasks/${taskId}`, withSession({ method: 'DELETE' }));
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || 'Failed to delete task');
      }
      toast.success('Task deleted');
      setSelectedTask(null);
      await refreshTasks();
    } catch (err: any) {
      toast.error(err?.message || 'Failed to delete task');
    }
  };

  const switchInputMode = (mode: "file" | "database") => {
    setInputMode(mode);
    if (mode === "file") {
      setDbAttached(false);
      setMetadataJsonValue("");
    }
  };

  const handleDownloadResults = () => {
    if (!selectedTask || results.length === 0) {
      toast.info("No results available to download yet");
      return;
    }
    const payload = {
      taskId: selectedTask.id,
      taskName: selectedTask.name,
      generatedAt: new Date().toISOString(),
      buckets: results.map((result) => ({
        chunkIndex: result.chunkIndex,
        status: result.status,
        rangeStart: result.rangeStart ?? null,
        rangeEnd: result.rangeEnd ?? null,
        itemsCount: getItemsCount(result),
        bytesUsed: result.bytesUsed ?? null,
        outputSummary: result.output ?? null,
        error: result.error ?? null,
        itemResults: result.itemResults ?? [],
      })),
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    const safeName = selectedTask.name.replace(/[^a-z0-9_-]+/gi, "_").slice(0, 60) || "task";
    link.href = url;
    link.download = `${safeName}_results.json`;
    link.click();
    URL.revokeObjectURL(url);
    toast.success("Results download started");
  };

  return (
    <div className="tabbed-view">
      <div className="view-tab-bar" role="tablist" aria-label="Customer workspace">
        <button
          type="button"
          id="customer-wallet-tab"
          className={`tab-button${activeTab === "wallet" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "wallet"}
          aria-controls="customer-wallet-panel"
          onClick={() => setActiveTab("wallet")}
        >
          Wallet
        </button>
        <button
          type="button"
          id="customer-submit-tab"
          className={`tab-button${activeTab === "submit" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "submit"}
          aria-controls="customer-submit-panel"
          onClick={() => setActiveTab("submit")}
        >
          Submit Task
        </button>
        <button
          type="button"
          id="customer-tasks-tab"
          className={`tab-button${activeTab === "tasks" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "tasks"}
          aria-controls="customer-tasks-panel"
          onClick={() => setActiveTab("tasks")}
        >
          My Tasks
        </button>
        <button
          type="button"
          id="customer-details-tab"
          className={`tab-button${activeTab === "details" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "details"}
          aria-controls="customer-details-panel"
          onClick={() => setActiveTab("details")}
        >
          Task Details
        </button>
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="customer-wallet-panel"
        aria-labelledby="customer-wallet-tab"
        hidden={activeTab !== "wallet"}
      >
        <WalletSection
          profile={wallet}
          transactions={walletTransactions}
          total={walletTransactionsTotal}
          loading={walletLoading}
          onRefresh={onRefreshWallet}
          sessionId={sessionId}
          onDeposit={onDeposit}
          onWithdraw={onWithdraw}
        />
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="customer-submit-panel"
        aria-labelledby="customer-submit-tab"
        hidden={activeTab !== "submit"}
      >
        <h2>Submit New Task</h2>
        <form className="card" onSubmit={handleSubmit}>
          <label>
            Task Name
            <input type="text" name="name" placeholder="Summarize invoices" maxLength={120} required />
          </label>
          <div className="tab-switch">
            <button
              type="button"
              className={`tab-button${inputMode === "file" ? " active" : ""}`}
              onClick={() => switchInputMode("file")}
            >
              Data File
            </button>
            <button
              type="button"
              className={`tab-button${inputMode === "database" ? " active" : ""}`}
              onClick={() => switchInputMode("database")}
            >
              Database
            </button>
          </div>
          <div className="grid">
            <label>
              Capability Required
              <select name="capabilityRequired" defaultValue="image-processing" required>
                {CAPABILITIES.map((cap) => (
                  <option key={cap} value={cap}>
                    {formatCapability(cap)}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Credit Cost
              <input type="number" name="creditCost" min={1} max={100} defaultValue={10} required />
            </label>
          </div>
          <label>
            Code ZIP (must include main.js)
            <input type="file" name="code" accept=".zip" required />
          </label>
          {inputMode === "file" ? (
            <div className="tab-panel">
              <label>
                data.json (optional)
                <input type="file" name="data" accept="application/json" />
              </label>
              <small className="muted">Upload a JSON array to distribute work across chunks.</small>
            </div>
          ) : (
            <div className="tab-panel">
              <div className="grid">
                <label>
                  MongoDB URI
                  <input
                    type="url"
                    value={dbUri}
                    onChange={(event) => {
                      setDbUri(event.target.value);
                      setDbAttached(false);
                    }}
                    placeholder="mongodb+srv://user:pass@cluster.example"
                    required={inputMode === "database"}
                  />
                </label>
                <label>
                  Database Name
                  <input
                    type="text"
                    value={dbName}
                    onChange={(event) => {
                      setDbName(event.target.value);
                      setDbAttached(false);
                    }}
                    placeholder="analytics"
                    required={inputMode === "database"}
                  />
                </label>
              </div>
              <label>
                Collection Name
                <input
                  type="text"
                  value={dbCollection}
                  onChange={(event) => {
                    setDbCollection(event.target.value);
                    setDbAttached(false);
                  }}
                  placeholder="invoices"
                  required={inputMode === "database"}
                />
              </label>
              <div className="attach-row">
                <button type="button" className="btn" onClick={handleAttachDatabase}>
                  Attach Database
                </button>
                {dbAttached ? <span className="success-pill">Attached</span> : <span className="muted">Fill details and click attach.</span>}
              </div>
            </div>
          )}
          <label>
            Total chunks (optional)
            <input type="number" name="totalChunks" min={1} />
          </label>
          <input type="hidden" name="inputType" value={inputMode === "database" ? "database" : "file"} readOnly />
          <input type="hidden" name="metadataJson" value={metadataJsonValue} readOnly />
          <button type="submit" disabled={loading}>
            {loading ? "Submitting..." : "Submit Task"}
          </button>
        </form>
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="customer-tasks-panel"
        aria-labelledby="customer-tasks-tab"
        hidden={activeTab !== "tasks"}
      >
        <h2>My Tasks</h2>
        {tasks.length === 0 ? (
          <p className="muted">No tasks submitted yet.</p>
        ) : (
          <div className="task-grid">
            {tasks.map((task) => (
              <TaskCard
                key={task.id}
                task={task}
                onSelect={(next) => {
                  setSelectedTask(next);
                  setActiveTab("details");
                }}
                isActive={selectedTask?.id === task.id}
              />
            ))}
          </div>
        )}
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="customer-details-panel"
        aria-labelledby="customer-details-tab"
        hidden={activeTab !== "details"}
      >
        {selectedTask ? (
          <>
            <h2>Task Details</h2>
            <div className="card">
              <h3 className="task-detail-name">{selectedTask.name}</h3>
              <div className="task-header">
                <span className={`status status-${selectedTask.status}`}>{formatStatus(selectedTask.status)}</span>
                <span className="capability">{formatCapability(selectedTask.capabilityRequired)}</span>
                {selectedTask.progress !== null && <span>{selectedTask.progress}%</span>}
              </div>
              <ProgressBar value={selectedTask.progress ?? null} />
              <div className="task-meta">
                <span>
                  {selectedTask.processedChunks ?? 0}/{selectedTask.totalChunks ?? "?"} chunks processed
                </span>
                <span>Created {new Date(selectedTask.createdAt).toLocaleString()}</span>
              </div>
              <h3>Chunk Summary</h3>
              <BucketSummaryPanel task={selectedTask} results={results} assignments={assignments} />
              <div className="task-actions-row">
                <button className="btn" onClick={handleDownloadResults} disabled={results.length === 0}>
                  Download Results (JSON)
                </button>
                {selectedTask.revoked ? (
                  <button className="btn" onClick={() => reinvokeTask(selectedTask.id)}>
                    Reinvoke Task
                  </button>
                ) : (
                  <button className="btn warning" onClick={() => revokeTask(selectedTask.id)}>
                    Revoke Task
                  </button>
                )}
                <button className="btn" onClick={() => deleteTask(selectedTask.id)}>
                  Delete Task
                </button>
              </div>
              <h3>Chunk Results</h3>
              <BucketResultsTable results={results} />
            </div>
          </>
        ) : (
          <>
            <h2>Task Details</h2>
            <p className="muted">Select a task from the My Tasks tab to inspect progress and download results.</p>
          </>
        )}
      </div>
    </div>
  );
}

function WorkerView({
  sessionId,
  wallet,
  walletTransactions,
  walletTransactionsTotal,
  onRefreshWallet,
  walletLoading,
  onDeposit,
  onWithdraw,
}: {
  sessionId: string;
  wallet: UserProfile | null;
  walletTransactions: WalletTransaction[];
  walletTransactionsTotal: number;
  onRefreshWallet: () => void;
  walletLoading: boolean;
  onDeposit: WalletDepositHandler;
  onWithdraw: (amount: number) => Promise<WalletTransaction>;
}) {
  const [availableTasks, setAvailableTasks] = useState<Task[]>([]);
  const [assignedTasks, setAssignedTasks] = useState<Task[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [results, setResults] = useState<BucketResult[]>([]);
  const [assignments, setAssignments] = useState<BucketAssignment[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [capabilityFilter, setCapabilityFilter] = useState("all");
  const [workerOnline, setWorkerOnline] = useState<boolean | null>(null);
  const [activeTab, setActiveTab] = useState<"wallet" | "queue" | "assigned" | "details">("queue");

  const refresh = useCallback(async () => {
    try {
      const allTasks = await listTasks();
      const mine = allTasks.filter((t) => (t.assignedWorkers || []).includes(sessionId));
      const available = allTasks.filter((t) => {
        if (t.revoked) return false;
        if (t.status === "completed" || t.status === "failed") return false;
        const assigned = t.assignedWorkers || [];
        if (assigned.includes(sessionId)) return false;
        return true; // keep visible so additional workers can opt in
      });
      setAvailableTasks(available);
      setAssignedTasks(mine);
      setSelectedTask((prev) => {
        if (!prev) return prev;
        const updated =
          mine.find((t) => t.id === prev.id) ||
          allTasks.find((t) => t.id === prev.id) ||
          null;
        return updated || null;
      });
    } catch (error: any) {
      toast.error(error.message || "Failed to refresh tasks");
    }
  }, [sessionId]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 4000);
    return () => clearInterval(interval);
  }, [refresh]);

  const checkWorkerStatus = useCallback(async () => {
    try {
      const status = await fetchWorkerOnline(sessionId);
      setWorkerOnline(status.online);
    } catch (error) {
      setWorkerOnline(false);
    }
  }, [sessionId]);

  useEffect(() => {
    checkWorkerStatus();
    const timer = setInterval(checkWorkerStatus, 5000);
    return () => clearInterval(timer);
  }, [checkWorkerStatus]);

  const normalizedSearch = searchTerm.trim().toLowerCase();
  const filteredAvailable = useMemo(() => {
    return availableTasks.filter((task) => {
      const matchesName =
        !normalizedSearch || task.name.toLowerCase().includes(normalizedSearch);
      const matchesCapability =
        capabilityFilter === "all" || task.capabilityRequired === capabilityFilter;
      return matchesName && matchesCapability;
    });
  }, [availableTasks, normalizedSearch, capabilityFilter]);

  const filteredAssigned = useMemo(() => {
    return assignedTasks.filter((task) => {
      const matchesName =
        !normalizedSearch || task.name.toLowerCase().includes(normalizedSearch);
      const matchesCapability =
        capabilityFilter === "all" || task.capabilityRequired === capabilityFilter;
      return matchesName && matchesCapability;
    });
  }, [assignedTasks, normalizedSearch, capabilityFilter]);

  useEffect(() => {
    setSelectedTask((prev) => {
      if (filteredAssigned.length === 0) {
        return null;
      }
      if (prev && filteredAssigned.some((task) => task.id === prev.id)) {
        return prev;
      }
      return filteredAssigned[0];
    });
  }, [filteredAssigned]);

  useEffect(() => {
    if (activeTab === "details" && !selectedTask) {
      setActiveTab(filteredAssigned.length > 0 ? "assigned" : "queue");
    }
  }, [activeTab, selectedTask, filteredAssigned.length]);

  const renderStatusBanner = () => (
    <div
      className={`worker-status ${
        workerOnline === null ? "unknown" : workerOnline ? "online" : "offline"
      }`}
    >
      {workerOnline === null ? (
        <span>Checking worker status…</span>
      ) : workerOnline ? (
        <span>Worker online</span>
      ) : (
        <span>Worker offline. Start the runner process shown below before claiming tasks.</span>
      )}
    </div>
  );

  const renderFilterControls = () => (
    <div className="card task-filters">
      <div className="task-filters-group">
        <label className="sr-only" htmlFor="worker-search">
          Search by task name
        </label>
        <input
          id="worker-search"
          className="search-input"
          type="search"
          placeholder="Search tasks by name"
          value={searchTerm}
          onChange={(event) => setSearchTerm(event.target.value)}
        />
      </div>
      <div className="task-filters-group">
        <label className="sr-only" htmlFor="worker-capability-filter">
          Filter by capability
        </label>
        <select
          id="worker-capability-filter"
          className="capability-filter"
          value={capabilityFilter}
          onChange={(event) => setCapabilityFilter(event.target.value)}
        >
          <option value="all">All capabilities</option>
          {CAPABILITIES.map((cap) => (
            <option key={cap} value={cap}>
              {formatCapability(cap)}
            </option>
          ))}
        </select>
      </div>
    </div>
  );

  useEffect(() => {
    if (!selectedTask) {
      setResults([]);
      setAssignments([]);
      return;
    }
    let cancelled = false;

    const loadDetails = async () => {
      try {
        const data = await fetchBucketResults(selectedTask.id);
        if (!cancelled) {
          setResults(data.results);
          setAssignments(data.assignments);
        }
      } catch (error) {
        if (!cancelled) {
          console.error("Failed to load chunk results", error);
        }
      }
    };

    loadDetails();

    if (activeTab === "details") {
      const interval = setInterval(loadDetails, 3000);
      return () => {
        cancelled = true;
        clearInterval(interval);
      };
    }

    return () => {
      cancelled = true;
    };
  }, [selectedTask?.id, selectedTask?.status, activeTab]);

  const workerAssignments = useMemo(() => {
    if (!selectedTask) return [] as BucketAssignment[];
    return assignments.filter((assignment) => {
      if (assignment.workerId !== sessionId) return false;
      if (assignment.taskId && assignment.taskId !== selectedTask.id) return false;
      return true;
    });
  }, [assignments, selectedTask?.id, sessionId]);

  const activeAssignment = useMemo(() => {
    if (workerAssignments.length === 0) return null;
    return [...workerAssignments].sort((a, b) => {
      const aTime = Date.parse(a.updatedAt || a.assignedAt || "") || 0;
      const bTime = Date.parse(b.updatedAt || b.assignedAt || "") || 0;
      return bTime - aTime;
    })[0];
  }, [workerAssignments]);

  const activeAssignmentItems = useMemo(() => {
    if (!activeAssignment) return null as number | null;
    if (typeof activeAssignment.itemsCount === "number" && Number.isFinite(activeAssignment.itemsCount)) {
      return Math.max(0, activeAssignment.itemsCount);
    }
    if (typeof activeAssignment.rangeStart === "number" && typeof activeAssignment.rangeEnd === "number") {
      return Math.max(0, activeAssignment.rangeEnd - activeAssignment.rangeStart);
    }
    return null;
  }, [activeAssignment]);

  const activeAssignmentProcessed = useMemo(() => {
    if (!activeAssignment) return null as number | null;
    if (typeof activeAssignment.processedCount === "number" && Number.isFinite(activeAssignment.processedCount)) {
      return Math.max(0, activeAssignment.processedCount);
    }
    if (
      typeof activeAssignment.progressRangeEnd === "number"
      && typeof activeAssignment.rangeStart === "number"
    ) {
      return Math.max(0, activeAssignment.progressRangeEnd - activeAssignment.rangeStart);
    }
    return null;
  }, [activeAssignment]);

  const activeAssignmentProgress = useMemo(() => {
    if (activeAssignmentItems && activeAssignmentItems > 0) {
      const processed = activeAssignmentProcessed ?? 0;
      return Math.min(100, Math.max(0, Math.round((processed / activeAssignmentItems) * 100)));
    }
    if (activeAssignmentProcessed && activeAssignmentProcessed > 0) {
      return 100;
    }
    return null;
  }, [activeAssignmentItems, activeAssignmentProcessed]);

  const workerResults = useMemo(() => {
    if (!selectedTask) return [] as BucketResult[];
    return results.filter((result) => {
      if (result.workerId === sessionId) return true;
      if (typeof result.chunkIndex === "number") {
        return workerAssignments.some((assignment) => assignment.chunkIndex === result.chunkIndex);
      }
      return false;
    });
  }, [results, workerAssignments, selectedTask?.id, sessionId]);

  const workerCompletedChunks = useMemo(() => {
    return workerResults.filter((result) => result.status === "completed");
  }, [workerResults]);

  const totalItemsProcessed = useMemo(() => {
    return workerCompletedChunks.reduce((sum, result) => {
      const processed = getProcessedItems(result) ?? getItemsCount(result) ?? 0;
      return sum + Math.max(0, processed);
    }, 0);
  }, [workerCompletedChunks]);

  const costPerChunk = useMemo(() => {
    if (typeof selectedTask?.costPerChunk === "number" && Number.isFinite(selectedTask.costPerChunk)) {
      return selectedTask.costPerChunk;
    }
    if (typeof selectedTask?.creditCost === "number" && Number.isFinite(selectedTask.creditCost)) {
      return selectedTask.creditCost;
    }
    return 0;
  }, [selectedTask?.costPerChunk, selectedTask?.creditCost]);

  const workerSharePerChunk = useMemo(() => {
    const feePercent = typeof selectedTask?.platformFeePercent === "number" && Number.isFinite(selectedTask.platformFeePercent)
      ? selectedTask.platformFeePercent
      : DEFAULT_PLATFORM_FEE_PERCENT;
    const share = costPerChunk * (1 - feePercent / 100);
    return Number.isFinite(share) ? Number(share.toFixed(2)) : 0;
  }, [costPerChunk, selectedTask?.platformFeePercent]);

  const creditsEarned = useMemo(() => {
    const total = workerSharePerChunk * workerCompletedChunks.length;
    return Number.isFinite(total) ? Number(total.toFixed(2)) : 0;
  }, [workerSharePerChunk, workerCompletedChunks.length]);

  const chunkBreakdown = useMemo(() => {
    type Row = {
      chunkIndex: number;
      status: string;
      processed: number | null;
      items: number | null;
      bytes: number | null;
      updatedAt: string | null;
    };
    const map = new Map<number, Row>();
    workerResults.forEach((result) => {
      if (typeof result.chunkIndex !== "number") return;
      map.set(result.chunkIndex, {
        chunkIndex: result.chunkIndex,
        status: result.status,
        processed: getProcessedItems(result),
        items: getItemsCount(result),
        bytes: typeof result.bytesUsed === "number" ? result.bytesUsed : null,
        updatedAt: result.updatedAt || result.createdAt || null,
      });
    });

    workerAssignments.forEach((assignment) => {
      if (typeof assignment.chunkIndex !== "number") return;
      const items = typeof assignment.itemsCount === "number" && Number.isFinite(assignment.itemsCount)
        ? Math.max(0, assignment.itemsCount)
        : typeof assignment.rangeStart === "number" && typeof assignment.rangeEnd === "number"
        ? Math.max(0, assignment.rangeEnd - assignment.rangeStart)
        : null;
      const processed = typeof assignment.processedCount === "number" && Number.isFinite(assignment.processedCount)
        ? Math.max(0, assignment.processedCount)
        : typeof assignment.progressRangeEnd === "number" && typeof assignment.rangeStart === "number"
        ? Math.max(0, assignment.progressRangeEnd - assignment.rangeStart)
        : null;
      const updatedAt = assignment.updatedAt || assignment.assignedAt || null;
      const existing = map.get(assignment.chunkIndex);
      if (existing) {
        if (processed !== null && (existing.processed === null || processed > existing.processed)) {
          existing.processed = processed;
        }
        if (items !== null && (existing.items === null || items > existing.items)) {
          existing.items = items;
        }
        if (updatedAt && (!existing.updatedAt || updatedAt > existing.updatedAt)) {
          existing.updatedAt = updatedAt;
        }
        if (existing.status !== "completed" && existing.status !== "failed") {
          if (processed !== null && items !== null && processed >= items) {
            existing.status = "completed";
          } else if (processed !== null && processed > 0) {
            existing.status = "processing";
          } else {
            existing.status = "assigned";
          }
        }
        if (existing.bytes === null && typeof assignment.bytesUsed === "number") {
          existing.bytes = assignment.bytesUsed;
        }
      } else {
        map.set(assignment.chunkIndex, {
          chunkIndex: assignment.chunkIndex,
          status:
            processed !== null && items !== null && processed >= items
              ? "completed"
              : processed !== null && processed > 0
              ? "processing"
              : "assigned",
          processed,
          items,
          bytes: typeof assignment.bytesUsed === "number" ? assignment.bytesUsed : null,
          updatedAt,
        });
      }
    });

    return Array.from(map.values()).sort((a, b) => a.chunkIndex - b.chunkIndex);
  }, [workerResults, workerAssignments]);

  const latestUnits = useMemo(() => {
    type Unit = {
      chunkIndex: number | null;
      label: string;
      status: string;
      summary: string;
      timestamp: number;
    };
    const collected: Unit[] = [];
    workerResults.forEach((result) => {
      const baseTime = Date.parse(result.updatedAt || result.createdAt || "") || 0;
      const items = Array.isArray(result.itemResults) ? result.itemResults : [];
      if (items.length > 0) {
        items.forEach((item, idx) => {
          const label = `${typeof result.chunkIndex === "number" ? `Chunk #${result.chunkIndex} · ` : ""}${formatItemLabel(item, idx)}`;
          const summary = item.status === "failed"
            ? item.error || item.output || "Failed"
            : item.output || "—";
          collected.push({
            chunkIndex: typeof result.chunkIndex === "number" ? result.chunkIndex : null,
            label,
            status: item.status || "completed",
            summary,
            timestamp: baseTime + idx / 100,
          });
        });
      } else {
        const label = typeof result.chunkIndex === "number" ? `Chunk #${result.chunkIndex}` : "Chunk";
        const summary = result.output || (result.error ? `Error: ${result.error}` : "No output yet");
        collected.push({
          chunkIndex: typeof result.chunkIndex === "number" ? result.chunkIndex : null,
          label,
          status: result.status,
          summary,
          timestamp: baseTime,
        });
      }
    });
    collected.sort((a, b) => b.timestamp - a.timestamp);
    return collected.slice(0, 5);
  }, [workerResults]);

  const handleClaim = async (task: Task) => {
    try {
      const status = await fetchWorkerOnline(sessionId);
      setWorkerOnline(status.online);
      if (!status.online) {
        toast.error("Start the worker process before claiming a task.");
        return;
      }
    } catch (error) {
      setWorkerOnline(false);
      toast.error("Start the worker process before claiming a task.");
      return;
    }
    try {
      const resp = await claimTask(task.id, sessionId);
      const updated = resp.task || task;
      setSelectedTask(updated);
      setActiveTab("details");
      toast.success("Task claimed");
      await refresh();
    } catch (error: any) {
      toast.error(error.message || "Failed to claim task");
    }
  };

  const handleDrop = async (task: Task) => {
    if (!sessionId) {
      toast.error('Worker not started');
      return;
    }
    try {
      const resp = await dropTask(task.id, sessionId);
      const updated = resp.task || task;
      setSelectedTask(null);
      setActiveTab("assigned");
      toast.success('Dropped task — you will stop processing it');
      await refresh();
    } catch (error: any) {
      toast.error(error.message || 'Failed to drop task');
    }
  };

  return (
    <div className="tabbed-view">
      <div className="view-tab-bar" role="tablist" aria-label="Worker workspace">
        <button
          type="button"
          id="worker-queue-tab"
          className={`tab-button${activeTab === "queue" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "queue"}
          aria-controls="worker-queue-panel"
          onClick={() => setActiveTab("queue")}
        >
          Task Queue
        </button>
        <button
          type="button"
          id="worker-assigned-tab"
          className={`tab-button${activeTab === "assigned" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "assigned"}
          aria-controls="worker-assigned-panel"
          onClick={() => setActiveTab("assigned")}
        >
          My Assignments
        </button>
        <button
          type="button"
          id="worker-details-tab"
          className={`tab-button${activeTab === "details" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "details"}
          aria-controls="worker-details-panel"
          onClick={() => setActiveTab("details")}
        >
          Task Details
        </button>
        <button
          type="button"
          id="worker-wallet-tab"
          className={`tab-button${activeTab === "wallet" ? " active" : ""}`}
          role="tab"
          aria-selected={activeTab === "wallet"}
          aria-controls="worker-wallet-panel"
          onClick={() => setActiveTab("wallet")}
        >
          Wallet
        </button>
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="worker-queue-panel"
        aria-labelledby="worker-queue-tab"
        hidden={activeTab !== "queue"}
      >
        {renderStatusBanner()}
        {renderFilterControls()}
        <h2>Available Tasks</h2>
        {availableTasks.length === 0 ? (
          <p className="muted">No queued tasks right now.</p>
        ) : filteredAvailable.length === 0 ? (
          <p className="muted">No queued tasks match your filters.</p>
        ) : (
          <div className="task-grid">
            {filteredAvailable.map((task) => (
              <TaskCard key={task.id} task={task} onClaim={handleClaim} />
            ))}
          </div>
        )}
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="worker-assigned-panel"
        aria-labelledby="worker-assigned-tab"
        hidden={activeTab !== "assigned"}
      >
        {renderStatusBanner()}
        {renderFilterControls()}
        <h2>My Assigned Tasks</h2>
        {assignedTasks.length === 0 ? (
          <p className="muted">
            No tasks assigned yet. Claim a task from the queue tab to start working.
          </p>
        ) : filteredAssigned.length === 0 ? (
          <p className="muted">No assigned tasks match your filters.</p>
        ) : (
          <div className="task-grid">
            {filteredAssigned.map((task) => (
              <TaskCard
                key={task.id}
                task={task}
                onSelect={(next) => {
                  setSelectedTask(next);
                  setActiveTab("details");
                }}
                onDrop={(t) => handleDrop(t)}
                isActive={selectedTask?.id === task.id}
              />
            ))}
          </div>
        )}
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="worker-details-panel"
        aria-labelledby="worker-details-tab"
        hidden={activeTab !== "details"}
      >
        {renderStatusBanner()}
        {selectedTask ? (
          <>
            <h2>Task Details</h2>
            <div className="card">
              <h3 className="task-detail-name">{selectedTask.name}</h3>
              <div className="task-header">
                <span className={`status status-${selectedTask.status}`}>{formatStatus(selectedTask.status)}</span>
                <span className="capability">{formatCapability(selectedTask.capabilityRequired)}</span>
                <span className="credits">{selectedTask.creditCost} credits</span>
              </div>
              <ProgressBar value={selectedTask.progress ?? null} />
              <div className="task-meta">
                <span>
                  {selectedTask.processedChunks ?? 0}/{selectedTask.totalChunks ?? "?"} chunks processed
                </span>
                <span>Task ID: {selectedTask.id}</span>
              </div>
              <h3>Worker Progress</h3>
              <div className="worker-progress-grid">
                <div className="worker-progress-card">
                  <span className="label">Current chunk</span>
                  <span className="value">
                    {activeAssignment && typeof activeAssignment.chunkIndex === "number"
                      ? `#${activeAssignment.chunkIndex}`
                      : "Idle"}
                  </span>
                  {activeAssignmentItems !== null ? (
                    <span className="sub">
                      {activeAssignmentProcessed ?? 0}/{activeAssignmentItems} items
                    </span>
                  ) : null}
                </div>
                <div className="worker-progress-card">
                  <span className="label">Chunks completed</span>
                  <span className="value">{workerCompletedChunks.length}</span>
                  <span className="sub">Assigned to you</span>
                </div>
                <div className="worker-progress-card">
                  <span className="label">Items processed</span>
                  <span className="value">{formatNumber(totalItemsProcessed, 0)}</span>
                  <span className="sub">Across completed chunks</span>
                </div>
                <div className="worker-progress-card">
                  <span className="label">Credits earned</span>
                  <span className="value">{formatCurrency(creditsEarned, "$0.00")}</span>
                  <span className="sub">{workerCompletedChunks.length} × {formatCurrency(workerSharePerChunk, "$0.00")}</span>
                </div>
              </div>
              {activeAssignment ? (
                <div className="worker-current-progress">
                  <div className="progress-meta">
                    <span>
                      Chunk {typeof activeAssignment.chunkIndex === "number" ? `#${activeAssignment.chunkIndex}` : "—"}
                    </span>
                    {activeAssignmentItems !== null ? (
                      <span>
                        {activeAssignmentProcessed ?? 0}/{activeAssignmentItems} item(s)
                      </span>
                    ) : null}
                    {activeAssignment.expiresAt ? (
                      <span>Expires {formatTimestamp(activeAssignment.expiresAt)}</span>
                    ) : null}
                  </div>
                  <ProgressBar value={activeAssignmentProgress} />
                </div>
              ) : (
                <p className="muted">No active chunk currently assigned. Claim a chunk from the queue.</p>
              )}
              <div className="worker-progress-sections">
                <div>
                  <h4>Latest processed units</h4>
                  {latestUnits.length === 0 ? (
                    <p className="muted">No processed items yet.</p>
                  ) : (
                    <ul className="latest-items-list">
                      {latestUnits.map((unit, idx) => {
                        const statusClass = unit.status === "failed"
                          ? "status status-failed"
                          : unit.status === "completed"
                          ? "status status-completed"
                          : "status status-processing";
                        return (
                          <li key={`${unit.chunkIndex ?? "no-chunk"}-${idx}`} className="latest-item">
                            <div className="latest-item-header">
                              <span>{unit.label}</span>
                              <span className={statusClass}>{unit.status}</span>
                            </div>
                            <div className="latest-item-output">{unit.summary}</div>
                          </li>
                        );
                      })}
                    </ul>
                  )}
                </div>
                <div>
                  <h4>Chunk item sizes</h4>
                  {chunkBreakdown.length === 0 ? (
                    <p className="muted">No chunk data yet.</p>
                  ) : (
                    <table className="worker-chunk-table">
                      <thead>
                        <tr>
                          <th>Chunk</th>
                          <th>Status</th>
                          <th>Processed</th>
                          <th>Items</th>
                        </tr>
                      </thead>
                      <tbody>
                        {chunkBreakdown.map((row) => (
                          <tr key={row.chunkIndex}>
                            <td>#{row.chunkIndex}</td>
                            <td>{row.status}</td>
                            <td>{row.processed ?? "—"}</td>
                            <td>{row.items ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
              <h3>Chunk Summary</h3>
              <BucketSummaryPanel task={selectedTask} results={results} assignments={assignments} />
              <div className="runner-box">
                <p>Worker service / runner</p>
                <p>
                  Start a worker process (long-running). The worker will <b>only</b> process tasks that have been claimed for this worker via the UI.
                </p>
                <pre>{`WORKER_ID=${sessionId} node scripts/worker-runner.mjs`}</pre>
                <small>
                  Ensure the backend is running on <code>http://localhost:4000</code>. Claim a task in the queue tab to assign it to this worker.
                </small>
              </div>
              <h3>Chunk Results</h3>
              <BucketResultsTable results={results} />
            </div>
          </>
        ) : assignedTasks.length > 0 ? (
          <>
            <h2>Task Details</h2>
            <p className="muted">Select a task from the My Assignments tab to inspect its chunk results.</p>
          </>
        ) : (
          <>
            <h2>Task Details</h2>
            <p className="muted">Claim a task first to see worker progress and chunk output.</p>
          </>
        )}
      </div>

      <div
        className="tab-section"
        role="tabpanel"
        id="worker-wallet-panel"
        aria-labelledby="worker-wallet-tab"
        hidden={activeTab !== "wallet"}
      >
        <WalletSection
          profile={wallet}
          transactions={walletTransactions}
          total={walletTransactionsTotal}
          loading={walletLoading}
          onRefresh={onRefreshWallet}
          sessionId={sessionId}
          onDeposit={onDeposit}
          onWithdraw={onWithdraw}
        />
      </div>
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState<"customer" | "worker">("customer");
  // generate or reuse a session id for this browser instance (used as creatorId or workerId)
  const [sessionId] = useState<string>(() => {
    const key = 'shifted_session_id';
    const existing = localStorage.getItem(key);
    if (existing) return existing;
    const gen = `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`;
    try { localStorage.setItem(key, gen); } catch (e) {}
    return gen;
  });
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [walletTransactions, setWalletTransactions] = useState<WalletTransaction[]>([]);
  const [walletTransactionsTotal, setWalletTransactionsTotal] = useState(0);
  const [walletLoading, setWalletLoading] = useState(false);

  useEffect(() => {
    setActiveSessionId(sessionId);
  }, [sessionId]);

  const refreshWallet = useCallback(async () => {
    setWalletLoading(true);
    try {
      const data = await fetchCurrentUser(sessionId);
      setProfile(data.user);
      setWalletTransactions(Array.isArray(data.walletTransactions) ? data.walletTransactions : []);
      setWalletTransactionsTotal(
        typeof data.walletTransactionsTotal === "number"
          ? data.walletTransactionsTotal
          : Array.isArray(data.walletTransactions)
          ? data.walletTransactions.length
          : 0
      );
    } catch (error) {
      console.error("wallet refresh failed", error);
      toast.error("Failed to refresh wallet");
    } finally {
      setWalletLoading(false);
    }
  }, [sessionId]);

  const handleRefreshWallet = useCallback(() => {
    void refreshWallet();
  }, [refreshWallet]);

  const handleWalletDeposit: WalletDepositHandler = useCallback(
    async (amount: number) => {
      try {
        const result = await depositToWallet(amount);
        if (result.kind === "redirect") {
          return result;
        }
        setProfile(result.user);
        await refreshWallet();
        return result;
      } catch (error) {
        throw error instanceof Error ? error : new Error("Deposit failed");
      }
    },
    [refreshWallet, setProfile]
  );

  const handleWalletWithdraw = useCallback(
    async (amount: number) => {
      try {
        if (!SANDBOX_WALLET_ENABLED) {
          throw new Error("Withdrawals are disabled outside sandbox mode. Please contact support.");
        }
        const response = await withdrawFromWallet(amount);
        setProfile(response.user);
        await refreshWallet();
        return response.transaction;
      } catch (error) {
        throw error instanceof Error ? error : new Error("Withdrawal failed");
      }
    },
    [refreshWallet, setProfile]
  );

  useEffect(() => {
    refreshWallet();
  }, [refreshWallet]);

  const headerBalance = formatCurrency(
    typeof profile?.walletBalance === "number" ? profile.walletBalance : 0,
    "$0.00"
  );

  return (
    <div className="app">
      <header>
        <h1>Distributed Computing</h1>
        <p>Offline-friendly asset-backed task processing</p>
        <nav>
          <button
            className={tab === "customer" ? "active" : ""}
            onClick={() => setTab("customer")}
          >
            Customer
          </button>
          <button
            className={tab === "worker" ? "active" : ""}
            onClick={() => setTab("worker")}
          >
            Worker
          </button>
        </nav>
        <div className="header-meta">
          <div className="session">Session: <code>{sessionId}</code></div>
          <div className={`wallet-chip${walletLoading ? " syncing" : ""}`}>
            Wallet: <span>{walletLoading ? "Updating..." : headerBalance}</span>
          </div>
        </div>
      </header>
      <main>
        {tab === "customer" ? (
          <CustomerView
            sessionId={sessionId}
            wallet={profile}
            walletTransactions={walletTransactions}
            walletTransactionsTotal={walletTransactionsTotal}
            onRefreshWallet={handleRefreshWallet}
            walletLoading={walletLoading}
            onDeposit={handleWalletDeposit}
            onWithdraw={handleWalletWithdraw}
          />
        ) : (
          <WorkerView
            sessionId={sessionId}
            wallet={profile}
            walletTransactions={walletTransactions}
            walletTransactionsTotal={walletTransactionsTotal}
            onRefreshWallet={handleRefreshWallet}
            walletLoading={walletLoading}
            onDeposit={handleWalletDeposit}
            onWithdraw={handleWalletWithdraw}
          />
        )}
      </main>
      <Toaster position="top-right" richColors closeButton />
    </div>
  );
}
