import express from "express";
import cors from "cors";
import multer from "multer";
import path from "path";
import fs from "fs";
import { initDb, getDb, saveDb } from "./db.js";
import { nanoid } from "nanoid";
import Stripe from "stripe";
import dotenv from "dotenv";
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;
const WORKER_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes
const DEFAULT_MAX_BUCKETS = 10;
const DEFAULT_BUCKET_BYTES = 1024 * 1024; // 1MB
const BUCKET_TIMEOUT_MS = 20 * 60 * 1000; // 20 minutes
const WORKER_SWEEP_INTERVAL_MS = Math.min(WORKER_TIMEOUT_MS, 60 * 1000);
const ITEM_PREVIEW_LIMIT = 240;
const MAX_ITEM_RESULTS_STORED = 200;
const workerHeartbeats = new Map();
const SESSION_COOKIE = "rt_session";
const DEV_DEFAULT_WALLET = Number.isFinite(Number(process.env.DEV_INITIAL_WALLET))
  ? Number(process.env.DEV_INITIAL_WALLET)
  : 2_000_000_000;
const PLATFORM_FEE_PERCENT = Number.isFinite(Number(process.env.PLATFORM_FEE_PERCENT))
  ? Number(process.env.PLATFORM_FEE_PERCENT)
  : 10;
const WORKER_FEE_PERCENT = 100 - PLATFORM_FEE_PERCENT;
const DISABLE_BUDGET_CHECKS = String(process.env.DISABLE_BUDGET_CHECKS ?? "true").toLowerCase() === "true";

const SANDBOX_WALLET_ENABLED = String(process.env.WALLET_SANDBOX_ENABLED || "").toLowerCase() === "true";

function normalizeCurrencyAmount(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  const rounded = Math.round(numeric * 100) / 100;
  return Number(rounded.toFixed(2));
}

function parseCookies(cookieHeader = "") {
  return cookieHeader.split(";").reduce((acc, part) => {
    const trimmed = part.trim();
    if (!trimmed) return acc;
    const [name, ...rest] = trimmed.split("=");
    if (!name) return acc;
    acc[name] = decodeURIComponent(rest.join("="));
    return acc;
  }, /** @type {Record<string, string>} */ ({}));
}

function serializeCookie(name, value, options = {}) {
  const segments = [`${name}=${encodeURIComponent(value)}`];
  if (options.path) segments.push(`Path=${options.path}`);
  if (options.httpOnly) segments.push("HttpOnly");
  if (options.sameSite) segments.push(`SameSite=${options.sameSite}`);
  if (options.maxAge !== undefined) segments.push(`Max-Age=${options.maxAge}`);
  if (options.secure) segments.push("Secure");
  return segments.join("; ");
}

function ensureCollection(db, key, fallback) {
  if (!db.data[key]) {
    db.data[key] = fallback;
  }
  return db.data[key];
}

function findUserBySessionId(db, sessionId) {
  if (!sessionId) return null;
  const users = ensureCollection(db, "users", []);
  return users.find((user) => user.sessionId === sessionId) || null;
}

function findUserById(db, userId) {
  if (!userId) return null;
  const users = ensureCollection(db, "users", []);
  return users.find((user) => user.id === userId) || null;
}

function createUser(db, { sessionId, initialBalance = 0, roles = [] }) {
  const users = ensureCollection(db, "users", []);
  const now = new Date().toISOString();
  const user = {
    id: nanoid(),
    sessionId: sessionId || nanoid(),
    walletBalance: Number(initialBalance) || 0,
    roles,
    createdAt: now,
    updatedAt: now,
  };
  users.push(user);
  return user;
}

function ensureWorkerUser(db, workerId) {
  if (!workerId) return null;
  let user = findUserBySessionId(db, workerId);
  if (!user) {
    user = createUser(db, { sessionId: workerId, initialBalance: 0, roles: ["worker"] });
  }
  return user;
}

function pushWalletTransaction(db, payload) {
  const transactions = ensureCollection(db, "walletTransactions", []);
  transactions.push(payload);
  return payload;
}

function adjustUserBalance(db, user, delta, type, meta = {}) {
  if (!user) return null;
  const amount = Number(delta) || 0;
  user.walletBalance = Number(user.walletBalance || 0) + amount;
  user.updatedAt = new Date().toISOString();
  const transaction = pushWalletTransaction(db, {
    id: nanoid(),
    userId: user.id,
    sessionId: user.sessionId,
    type,
    amount,
    balanceAfter: user.walletBalance,
    meta,
    createdAt: user.updatedAt,
  });
  return transaction;
}

function recordPlatformEarning(db, amount, meta = {}) {
  const ledger = ensureCollection(db, "platformLedger", { totalEarnings: 0 });
  ledger.totalEarnings = Number(ledger.totalEarnings || 0) + amount;
  pushWalletTransaction(db, {
    id: nanoid(),
    userId: "platform",
    sessionId: "platform",
    type: "platform-fee",
    amount,
    balanceAfter: ledger.totalEarnings,
    meta,
    createdAt: new Date().toISOString(),
  });
}

function resolveTaskBudget(task) {
  const costPerChunk = Number(task?.costPerChunk) > 0 ? Number(task.costPerChunk) : Number(task?.creditCost) || 1;
  const maxBillableChunks = Number.isFinite(Number(task?.maxBillableChunks))
    ? Number(task.maxBillableChunks)
    : Number.isFinite(Number(task?.totalChunks))
    ? Number(task.totalChunks)
    : 1;
  const budgetTotal = Number.isFinite(Number(task?.budgetTotal))
    ? Number(task.budgetTotal)
    : costPerChunk * maxBillableChunks;
  const chunksPaid = Number.isFinite(Number(task?.chunksPaid)) ? Number(task.chunksPaid) : 0;
  const budgetSpent = Number.isFinite(Number(task?.budgetSpent)) ? Number(task.budgetSpent) : costPerChunk * chunksPaid;
  return {
    costPerChunk,
    maxBillableChunks,
    budgetTotal,
    chunksPaid,
    budgetSpent,
  };
}

function issueChunkPayout(db, task, chunkResult, workerId) {
  if (!task || !chunkResult) return false;
  if (chunkResult.payoutIssued) return false;
  if (chunkResult.status !== "completed") return false;
  const { costPerChunk, maxBillableChunks, chunksPaid } = resolveTaskBudget(task);
  if (!DISABLE_BUDGET_CHECKS && chunksPaid >= maxBillableChunks) return false;

  const creatorSessionId = task.creatorId || task.creatorSessionId || null;
  const customerAccount = findUserBySessionId(db, creatorSessionId);
  if (!customerAccount) return false;
  const worker = workerId ? ensureWorkerUser(db, workerId) : null;

  const total = costPerChunk;
  const feePercent = Number.isFinite(Number(task.platformFeePercent))
    ? Number(task.platformFeePercent)
    : PLATFORM_FEE_PERCENT;
  const platformShare = Number(((total * feePercent) / 100).toFixed(6));
  const workerShare = Number((total - platformShare).toFixed(6));

  adjustUserBalance(db, customerAccount, -total, "chunk-debit", {
    taskId: task.id,
    chunkIndex: chunkResult.chunkIndex,
  });
  if (worker) {
    adjustUserBalance(db, worker, workerShare, "chunk-credit", {
      taskId: task.id,
      chunkIndex: chunkResult.chunkIndex,
    });
  }
  if (platformShare !== 0) {
    recordPlatformEarning(db, platformShare, {
      taskId: task.id,
      chunkIndex: chunkResult.chunkIndex,
    });
  }

  task.chunksPaid = (task.chunksPaid || 0) + 1;
  task.budgetSpent = Number(task.budgetSpent || 0) + total;
  chunkResult.payoutIssued = true;
  chunkResult.payoutAt = new Date().toISOString();
  chunkResult.workerId = workerId || chunkResult.workerId || null;
  return true;
}

setInterval(() => {
  const cutoff = Date.now() - WORKER_TIMEOUT_MS * 2;
  for (const [workerId, ts] of workerHeartbeats.entries()) {
    if (ts < cutoff) {
      workerHeartbeats.delete(workerId);
    }
  }
}, WORKER_SWEEP_INTERVAL_MS);

app.use(cors());
app.use("/api/stripe/webhook", express.raw({ type: "application/json" }));
app.use(express.json({ limit: "10mb" }));

const storageDir = path.resolve(process.cwd(), "backend", "storage");
if (!fs.existsSync(storageDir)) {
  fs.mkdirSync(storageDir, { recursive: true });
}

await initDb();

// Ensure DB shape defaults exist at startup to avoid undefined access during requests
try {
  const _db = getDb();
  let mutated = false;
  if (!_db.data.tasks) {
    _db.data.tasks = [];
    mutated = true;
  }
  if (!_db.data.chunkResults) {
    _db.data.chunkResults = [];
    mutated = true;
  }
  if (!_db.data.chunkAssignments) {
    _db.data.chunkAssignments = [];
    mutated = true;
  }
  if (!_db.data.users) {
    _db.data.users = [];
    mutated = true;
  }
  if (!_db.data.walletTransactions) {
    _db.data.walletTransactions = [];
    mutated = true;
  }
  if (!_db.data.stripeSessions) {
    _db.data.stripeSessions = [];
    mutated = true;
  }
  if (!_db.data.platformLedger) {
    _db.data.platformLedger = { totalEarnings: 0 };
    mutated = true;
  }
  const storageSubdirs = fs.existsSync(storageDir)
    ? fs
        .readdirSync(storageDir, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
    : [];
  const availableStorage = new Set(storageSubdirs);
  const chunkResults = _db.data.chunkResults;
  const chunkAssignments = _db.data.chunkAssignments;
  const legacyToCanonicalId = new Map();

  _db.data.tasks.forEach((task) => {
    if (!task.id && task._id) {
      task.id = String(task._id);
      mutated = true;
    } else if (task.id) {
      task.id = String(task.id);
    }
    if (!task.baseUrl) {
      task.baseUrl = `http://localhost:${PORT}`;
      mutated = true;
    }
    if (!task.name) {
      const suffix = task.id ? String(task.id).slice(-6) : nanoid(6);
      task.name = `Task ${suffix}`;
      mutated = true;
    }
    if (!Number.isFinite(Number(task.costPerChunk)) || Number(task.costPerChunk) <= 0) {
      const fallbackCost = Number(task.creditCost) > 0 ? Number(task.creditCost) : 1;
      task.costPerChunk = fallbackCost;
      mutated = true;
    } else {
      task.costPerChunk = Number(task.costPerChunk);
    }
    if (!Number.isFinite(Number(task.platformFeePercent))) {
      task.platformFeePercent = PLATFORM_FEE_PERCENT;
      mutated = true;
    }
    if (!Number.isFinite(Number(task.maxBillableChunks))) {
      const fallbackChunks = Number.isFinite(Number(task.totalChunks)) ? Number(task.totalChunks) : DEFAULT_MAX_BUCKETS;
      task.maxBillableChunks = Math.max(1, Math.floor(fallbackChunks));
      mutated = true;
    } else {
      task.maxBillableChunks = Math.max(1, Math.floor(Number(task.maxBillableChunks)));
    }
    if (!Number.isFinite(Number(task.budgetTotal))) {
      task.budgetTotal = Number(task.costPerChunk) * Number(task.maxBillableChunks);
      mutated = true;
    } else {
      task.budgetTotal = Number(task.budgetTotal);
    }
    if (!Number.isFinite(Number(task.budgetSpent))) {
      task.budgetSpent = 0;
      mutated = true;
    }
    if (!Number.isFinite(Number(task.chunksPaid))) {
      task.chunksPaid = 0;
      mutated = true;
    }

    const candidateIds = [];
    if (task.storageId) candidateIds.push(String(task.storageId));
    if (task.id) candidateIds.push(String(task.id));
    if (task._id) candidateIds.push(String(task._id));

    let resolvedStorageId = null;
    for (const candidate of candidateIds) {
      const candidatePath = path.join(storageDir, candidate);
      if (fs.existsSync(candidatePath)) {
        resolvedStorageId = candidate;
        availableStorage.delete(candidate);
        break;
      }
    }

    if (!resolvedStorageId) {
      const candidates = [];
      for (const dirName of Array.from(availableStorage)) {
        const folder = path.join(storageDir, dirName);
        const codeMatches = !task.codeFileName || fs.existsSync(path.join(folder, task.codeFileName));
        const dataMatches = !task.dataFileName || fs.existsSync(path.join(folder, task.dataFileName));
        if (!codeMatches || !dataMatches) continue;

        let score = Number.POSITIVE_INFINITY;
        if (task.createdAt && task.codeFileName) {
          const created = new Date(task.createdAt).getTime();
          if (Number.isFinite(created)) {
            try {
              const stats = fs.statSync(path.join(folder, task.codeFileName));
              const diff = Math.abs(stats.mtime.getTime() - created);
              score = diff;
            } catch (e) {
              // ignore stat errors; keep infinity score
            }
          }
        }
        candidates.push({ dirName, score });
      }
      if (candidates.length === 1) {
        resolvedStorageId = candidates[0].dirName;
      } else if (candidates.length > 1) {
        const finite = candidates.filter((c) => Number.isFinite(c.score));
        const ordered = (finite.length > 0 ? finite : candidates).sort((a, b) => a.score - b.score);
        if (ordered.length > 0) {
          resolvedStorageId = ordered[0].dirName;
        }
      }
      if (resolvedStorageId) {
        availableStorage.delete(resolvedStorageId);
      }
    }

    if (resolvedStorageId && task.storageId !== resolvedStorageId) {
      task.storageId = resolvedStorageId;
      mutated = true;
    }

    if (task.id && resolvedStorageId && resolvedStorageId !== task.id) {
      legacyToCanonicalId.set(resolvedStorageId, task.id);
    }

    task.bucketConfig = task.bucketConfig || {};
    if (typeof task.bucketConfig.maxBuckets !== "number" && typeof task.totalChunks === "number") {
      task.bucketConfig.maxBuckets = task.totalChunks;
      mutated = true;
    }
    if (typeof task.bucketConfig.maxBucketBytes !== "number" && typeof task.maxBucketBytes === "number") {
      task.bucketConfig.maxBucketBytes = task.maxBucketBytes;
      mutated = true;
    }
    if (!task.hasOwnProperty("totalItems")) {
      task.totalItems = null;
      mutated = true;
    }
    if (!task.hasOwnProperty("processedItems")) {
      task.processedItems = 0;
      mutated = true;
    }
    if (!task.hasOwnProperty("nextChunkIndex")) {
      task.nextChunkIndex = 0;
      mutated = true;
    }
  });

  if (legacyToCanonicalId.size) {
    chunkResults.forEach((entry) => {
      const mapped = legacyToCanonicalId.get(entry.taskId);
      if (mapped && entry.taskId !== mapped) {
        entry.taskId = mapped;
        mutated = true;
      }
    });
    chunkAssignments.forEach((entry) => {
      const mapped = legacyToCanonicalId.get(entry.taskId);
      if (mapped && entry.taskId !== mapped) {
        entry.taskId = mapped;
        mutated = true;
      }
    });
  }
  if (mutated) await saveDb();
} catch (e) {
  // ignore initialization errors
}

app.use(async (req, res, next) => {
  try {
    const db = getDb();
    const cookies = parseCookies(req.headers.cookie || "");
    let sessionId = cookies[SESSION_COOKIE] || req.headers["x-session-id"];
    let shouldPersist = false;
    if (!sessionId) {
      sessionId = nanoid();
      shouldPersist = true;
    }
    let user = findUserBySessionId(db, sessionId);
    if (!user) {
      user = createUser(db, {
        sessionId,
        initialBalance: DEV_DEFAULT_WALLET,
        roles: ["customer", "worker"],
      });
      pushWalletTransaction(db, {
        id: nanoid(),
        userId: user.id,
        sessionId: user.sessionId,
        type: "seed-credit",
        amount: user.walletBalance,
        balanceAfter: user.walletBalance,
        meta: { reason: "dev-auto-fund" },
        createdAt: user.createdAt,
      });
      shouldPersist = true;
    } else if (!Array.isArray(user.roles) || user.roles.length === 0) {
      user.roles = ["customer", "worker"];
      user.updatedAt = new Date().toISOString();
      shouldPersist = true;
    }

    req.sessionId = sessionId;
    req.currentUser = user;

    if (!cookies[SESSION_COOKIE]) {
      res.setHeader(
        "Set-Cookie",
        serializeCookie(SESSION_COOKIE, sessionId, {
          path: "/",
          httpOnly: true,
          sameSite: "Lax",
        })
      );
    }

    if (shouldPersist) {
      await saveDb();
    }
    next();
  } catch (error) {
    console.error("session init error", error);
    res.status(500).json({ error: "Failed to initialize session" });
  }
});

app.use("/storage", express.static(storageDir));

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const taskId = req.taskId || nanoid();
      const taskDir = path.join(storageDir, taskId);
      fs.mkdirSync(taskDir, { recursive: true });
      req.taskId = taskId;
      cb(null, taskDir);
    },
    filename: (req, file, cb) => {
      const original = file.originalname?.toLowerCase() || "file";
      cb(null, original.includes(".zip") ? "code.zip" : original.includes(".json") ? "data.json" : original);
    },
  }),
});

function getTaskStorageId(task) {
  if (!task || typeof task !== "object") return null;
  const candidate = task.storageId || task.id || task._id;
  return candidate ? String(candidate) : null;
}

function getTaskStoragePath(task, fileName = null) {
  const storageId = getTaskStorageId(task);
  if (!storageId) return null;
  const dir = path.join(storageDir, storageId);
  return fileName ? path.join(dir, fileName) : dir;
}

function removeTaskStorage(task) {
  const dir = getTaskStoragePath(task);
  if (!dir) return false;
  try {
    if (fs.existsSync(dir)) {
      fs.rmSync(dir, { recursive: true, force: true });
      return true;
    }
  } catch (error) {
    console.warn("failed to remove storage for task", task?.id || task?._id, error.message);
  }
  return false;
}

function buildTaskResponse(task) {
  if (!task) return null;
  const taskId = task.id || task._id;
  if (!taskId) return null;
  const storageId = getTaskStorageId(task);
  const base = task.baseUrl && storageId ? `${task.baseUrl}/storage/${storageId}` : null;
  const { _id, ...safeTask } = typeof task === 'object' && task !== null ? task : {};
  return {
    ...safeTask,
    id: taskId,
    name: task.name || `Task ${taskId}`,
    creatorId: task.creatorId || null,
    workerId: task.workerId || null,
    assignedWorkers: task.assignedWorkers || [],
    totalItems: typeof task.totalItems === "number" ? task.totalItems : null,
    processedItems: typeof task.processedItems === "number" ? task.processedItems : null,
    bucketConfig: {
      maxBuckets: task.bucketConfig?.maxBuckets ?? null,
      maxBucketBytes: task.bucketConfig?.maxBucketBytes ?? task.maxBucketBytes ?? null,
    },
    maxBucketBytes: task.maxBucketBytes ?? null,
    codeUrl: base && task.codeFileName ? `${base}/${task.codeFileName}` : undefined,
    dataUrl: base && task.dataFileName ? `${base}/${task.dataFileName}` : undefined,
    costPerChunk: Number.isFinite(Number(task.costPerChunk)) ? Number(task.costPerChunk) : null,
    budgetTotal: Number.isFinite(Number(task.budgetTotal)) ? Number(task.budgetTotal) : null,
    budgetSpent: Number.isFinite(Number(task.budgetSpent)) ? Number(task.budgetSpent) : 0,
    maxBillableChunks: Number.isFinite(Number(task.maxBillableChunks)) ? Number(task.maxBillableChunks) : null,
    chunksPaid: Number.isFinite(Number(task.chunksPaid)) ? Number(task.chunksPaid) : 0,
    platformFeePercent: Number.isFinite(Number(task.platformFeePercent)) ? Number(task.platformFeePercent) : PLATFORM_FEE_PERCENT,
    revoked: Boolean(task.revoked),
  };
}

function buildUserResponse(user) {
  if (!user) return null;
  return {
    id: user.id || user._id || null,
    sessionId: user.sessionId || null,
    walletBalance: Number.isFinite(Number(user.walletBalance)) ? Number(user.walletBalance) : 0,
    roles: Array.isArray(user.roles) ? [...user.roles] : [],
    createdAt: user.createdAt || null,
    updatedAt: user.updatedAt || null,
  };
}

function buildWalletTransactionResponse(entry) {
  if (!entry) return null;
  const txId = entry.id || entry._id || nanoid();
  const amount = Number(entry.amount);
  const balanceAfter = Number(entry.balanceAfter);
  return {
    id: txId,
    type: entry.type || "unknown",
    amount: Number.isFinite(amount) ? amount : 0,
    balanceAfter: Number.isFinite(balanceAfter) ? balanceAfter : null,
    createdAt: entry.createdAt || null,
    meta: entry.meta || null,
  };
}

function readTaskItems(task) {
  if (!task || !task.dataFileName) return [];
  try {
    const dataPath = getTaskStoragePath(task, task.dataFileName);
    if (!dataPath || !fs.existsSync(dataPath)) return [];
    const raw = fs.readFileSync(dataPath, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    console.error("failed to read task data", getTaskStorageId(task) || task.id, error.message);
    return [];
  }
}

function ensureBucketConfig(task, items) {
  let mutated = false;
  task.bucketConfig = task.bucketConfig || {};
  const cfg = task.bucketConfig;
  const existingMaxBuckets = Number.isFinite(cfg.maxBuckets) && cfg.maxBuckets > 0
    ? Math.max(1, Math.floor(cfg.maxBuckets))
    : null;
  const configuredTotalChunks = Number.isFinite(task.totalChunks) && task.totalChunks > 0
    ? Math.max(1, Math.floor(task.totalChunks))
    : null;
  const desiredMaxBuckets = existingMaxBuckets
    ?? configuredTotalChunks
    ?? DEFAULT_MAX_BUCKETS;
  if (!Number.isFinite(cfg.maxBuckets) || cfg.maxBuckets <= 0 || Math.floor(cfg.maxBuckets) !== desiredMaxBuckets) {
    cfg.maxBuckets = desiredMaxBuckets;
    mutated = true;
  }
  if (!Number.isFinite(cfg.maxBucketBytes) || cfg.maxBucketBytes <= 0) {
    const fallbackBytes = task.maxBucketBytes && task.maxBucketBytes > 0 ? task.maxBucketBytes : DEFAULT_BUCKET_BYTES;
    cfg.maxBucketBytes = Math.max(1024, fallbackBytes);
    mutated = true;
  }
  let largestItemBytes = 0;
  for (const item of items) {
    try {
      const len = Buffer.byteLength(JSON.stringify(item));
      if (len > largestItemBytes) largestItemBytes = len;
    } catch (e) {
      // ignore serialization issue, treat as small item
    }
  }
  while (cfg.maxBuckets > 1 && largestItemBytes > cfg.maxBucketBytes) {
    cfg.maxBuckets = Math.max(1, Math.floor(cfg.maxBuckets / 2));
    cfg.maxBucketBytes *= 2;
    mutated = true;
  }
  if (largestItemBytes > cfg.maxBucketBytes) {
    cfg.maxBucketBytes = Math.max(cfg.maxBucketBytes, largestItemBytes * 2);
    mutated = true;
  }
  if (task.totalChunks !== cfg.maxBuckets) {
    task.totalChunks = cfg.maxBuckets;
    mutated = true;
  }
  if (task.maxBucketBytes !== cfg.maxBucketBytes) {
    task.maxBucketBytes = cfg.maxBucketBytes;
    mutated = true;
  }
  return mutated;
}

function sweepExpiredAssignments(db, taskId) {
  const assignments = db.data.chunkAssignments || [];
  const now = Date.now();
  let removed = false;
  for (let i = assignments.length - 1; i >= 0; i--) {
    const entry = assignments[i];
    if (entry.taskId !== taskId) continue;
    const assignedAt = entry.assignedAt ? new Date(entry.assignedAt).getTime() : 0;
    const expiresAt = entry.expiresAt ? new Date(entry.expiresAt).getTime() : assignedAt + BUCKET_TIMEOUT_MS;
    if (Number.isFinite(expiresAt) && now > expiresAt) {
      assignments.splice(i, 1);
      removed = true;
    }
  }
  return removed;
}

function clearTaskAssignments(db, taskId) {
  const assignments = db.data.chunkAssignments || [];
  let removed = false;
  for (let i = assignments.length - 1; i >= 0; i--) {
    if (assignments[i]?.taskId === taskId) {
      assignments.splice(i, 1);
      removed = true;
    }
  }
  return removed;
}

function normalizeRange(entry, fallbackIndex = null) {
  if (!entry) return null;
  const start = Number.isFinite(entry.rangeStart) ? entry.rangeStart : null;
  const end = Number.isFinite(entry.rangeEnd) ? entry.rangeEnd : null;
  if (start !== null && end !== null && end > start) {
    return { start, end };
  }
  if (fallbackIndex !== null) {
    const itemsCount = Number.isFinite(entry.itemsCount) ? Math.max(1, entry.itemsCount) : 1;
    return { start: fallbackIndex, end: fallbackIndex + itemsCount };
  }
  return null;
}

function collectRanges(entries) {
  const ranges = [];
  for (const entry of entries) {
    const fallbackIndex = Number.isFinite(entry.chunkIndex) ? entry.chunkIndex : null;
    const range = normalizeRange(entry, fallbackIndex);
    if (range) {
      ranges.push(range);
    }
  }
  return ranges;
}

function truncateText(value, limit = ITEM_PREVIEW_LIMIT) {
  if (typeof value !== "string") return "";
  if (value.length <= limit) return value;
  return `${value.slice(0, limit)}... (+${value.length - limit} chars)`;
}

function safeStringify(input) {
  try {
    if (typeof input === "string") return input;
    return JSON.stringify(input);
  } catch (error) {
    return String(input);
  }
}

function sanitizeItemResults(raw) {
  if (!Array.isArray(raw) || raw.length === 0) {
    return { items: [], total: 0, truncated: false };
  }
  const limited = raw.slice(0, MAX_ITEM_RESULTS_STORED);
  const items = limited.map((item, idx) => {
    const globalIndex = Number.isFinite(item?.globalIndex) ? item.globalIndex : null;
    const localIndex = Number.isFinite(item?.localIndex) ? item.localIndex : idx;
    const status = item?.status === "failed" ? "failed" : item?.status === "skipped" ? "skipped" : "completed";
    const inputPreviewSource = typeof item?.inputPreview === "string" && item.inputPreview.trim().length > 0 ? item.inputPreview : safeStringify(item?.input ?? item?.inputValue ?? null);
    const outputSource = typeof item?.output === "string" ? item.output : safeStringify(item?.output ?? null);
    const errorSource = typeof item?.error === "string" ? item.error : null;
    return {
      globalIndex,
      localIndex,
      status,
      inputPreview: truncateText(inputPreviewSource || ""),
      output: truncateText(outputSource || ""),
      error: errorSource ? truncateText(errorSource) : null,
    };
  });
  return {
    items,
    total: raw.length,
    truncated: raw.length > items.length,
  };
}

function sanitizeSingleItem(raw, fallbacks = {}) {
  if (!raw || typeof raw !== "object") return null;
  const safe = sanitizeItemResults([
    {
      globalIndex: Number.isFinite(raw.globalIndex) ? raw.globalIndex : fallbacks.globalIndex ?? null,
      localIndex: Number.isFinite(raw.localIndex) ? raw.localIndex : fallbacks.localIndex ?? null,
      status: raw.status || fallbacks.status || "completed",
      inputPreview: raw.inputPreview ?? raw.input ?? raw.inputValue ?? null,
      output: raw.output ?? null,
      error: raw.error ?? null,
    },
  ]);
  return safe.items[0] || null;
}

function buildResultTextFromItems(items, status) {
  if (!Array.isArray(items) || items.length === 0) {
    return `[input]\nnone\n[output]\nstatus: ${status}`;
  }
  const previewLimit = 20;
  const limited = items.slice(0, previewLimit);
  const inputLines = limited.map((item) => {
    const label = Number.isFinite(item.globalIndex) ? `#${item.globalIndex}` : `item ${item.localIndex}`;
    return `${label}: ${item.inputPreview || ""}`.trim();
  });
  const outputLines = limited.map((item) => {
    const label = Number.isFinite(item.globalIndex) ? `#${item.globalIndex}` : `item ${item.localIndex}`;
    const base = item.status === "failed" ? (item.error || item.output || "failed") : (item.output || "completed");
    return `${label}: ${base}`.trim();
  });
  if (items.length > previewLimit) {
    const remaining = items.length - previewLimit;
    inputLines.push(`...(and ${remaining} more)`);
    outputLines.push(`...(and ${remaining} more)`);
  }
  return `[input]\n${inputLines.join("\n")}\n[output]\n${outputLines.join("\n")}`;
}

function buildAssignmentSummary(entry) {
  return {
    chunkIndex: Number.isFinite(entry?.chunkIndex) ? entry.chunkIndex : null,
    workerId: entry?.workerId || null,
    assignedAt: entry?.assignedAt || null,
    expiresAt: entry?.expiresAt || null,
    rangeStart: Number.isFinite(entry?.rangeStart) ? entry.rangeStart : null,
    rangeEnd: Number.isFinite(entry?.rangeEnd) ? entry.rangeEnd : null,
    itemsCount: Number.isFinite(entry?.itemsCount)
      ? entry.itemsCount
      : (Number.isFinite(entry?.rangeStart) && Number.isFinite(entry?.rangeEnd))
      ? entry.rangeEnd - entry.rangeStart
      : null,
    bytesUsed: Number.isFinite(entry?.bytesUsed) ? entry.bytesUsed : null,
    processedCount: Number.isFinite(entry?.processedCount) ? entry.processedCount : null,
    progressRangeEnd: Number.isFinite(entry?.progressRangeEnd) ? entry.progressRangeEnd : null,
    lastBatchOffset: Number.isFinite(entry?.lastBatchOffset) ? entry.lastBatchOffset : null,
    lastBatchSize: Number.isFinite(entry?.lastBatchSize) ? entry.lastBatchSize : null,
    updatedAt: entry?.updatedAt || null,
  };
}

function indexCovered(index, ranges) {
  for (const range of ranges) {
    if (index >= range.start && index < range.end) return true;
  }
  return false;
}

function calculateBucket(task, items, finishedRanges, assignedRanges) {
  const cfg = task.bucketConfig;
  const total = items.length;
  const itemSizes = items.map((item) => {
    try {
      return Buffer.byteLength(JSON.stringify(item));
    } catch (e) {
      return 0;
    }
  });
  const ensureItemFits = (size) => {
    let changed = false;
    while (cfg.maxBuckets > 1 && size > cfg.maxBucketBytes) {
      cfg.maxBuckets = Math.max(1, Math.floor(cfg.maxBuckets / 2));
      cfg.maxBucketBytes *= 2;
      changed = true;
    }
    if (size > cfg.maxBucketBytes) {
      cfg.maxBucketBytes = Math.max(cfg.maxBucketBytes, size * 2);
      changed = true;
    }
    return changed;
  };

  let start = null;
  for (let i = 0; i < total; i++) {
    if (!indexCovered(i, finishedRanges) && !indexCovered(i, assignedRanges)) {
      start = i;
      break;
    }
  }
  if (start === null) return null;

  const firstSize = itemSizes[start] || 0;
  if (ensureItemFits(firstSize)) {
    // first item forced config change; make sure totals reflect update
    task.totalChunks = cfg.maxBuckets;
    task.maxBucketBytes = cfg.maxBucketBytes;
  }

  let limit = cfg.maxBucketBytes;
  let bytesUsed = 0;
  let end = start;

  while (end < total) {
    if (indexCovered(end, finishedRanges) || indexCovered(end, assignedRanges)) {
      break;
    }
    const size = itemSizes[end] || 0;
    if (size > limit) {
      const changed = ensureItemFits(size);
      if (changed) {
        limit = cfg.maxBucketBytes;
        task.totalChunks = cfg.maxBuckets;
        task.maxBucketBytes = cfg.maxBucketBytes;
      }
    }
    if (size > limit) {
      // still too large, assign as single item bucket
      if (end === start) {
        bytesUsed = size;
        end = start + 1;
      }
      break;
    }
    if (bytesUsed > 0 && bytesUsed + size > limit) {
      break;
    }
    bytesUsed += size;
    end++;
    if (bytesUsed >= limit) break;
  }

  if (end === start) {
    // ensure we advance by at least one item
    const size = itemSizes[start] || 0;
    bytesUsed = size;
    end = start + 1;
  }

  return {
    rangeStart: start,
    rangeEnd: Math.min(end, total),
    bytesUsed: bytesUsed || 0,
  };
}

function computeProgress(task, db) {
  const resultsArr = db.data.chunkResults || [];
  const finishedStatuses = new Set(["completed", "skipped", "failed"]);
  let processedChunks = 0;
  let processedItems = 0;

  for (const result of resultsArr) {
    if (result.taskId !== task.id) continue;

    const range = normalizeRange(result, Number.isFinite(result.chunkIndex) ? result.chunkIndex : null);
    const totalFromRange = range ? Math.max(0, range.end - range.start) : null;
    const totalFromCount = Number.isFinite(result.itemsCount) ? Math.max(0, result.itemsCount) : null;
    const processedHint = Number.isFinite(result.processedItems) ? Math.max(0, result.processedItems) : null;
    const totalFallback = Number.isFinite(result.itemResultsTotal) ? Math.max(0, result.itemResultsTotal) : null;

    let processedForResult = 0;
    if (finishedStatuses.has(result.status)) {
      processedChunks += 1;
      processedForResult = totalFromRange ?? totalFromCount ?? processedHint ?? totalFallback ?? 0;
    } else if (result.status === "processing") {
      processedForResult = processedHint ?? totalFromRange ?? totalFallback ?? 0;
    }

    processedItems += processedForResult;
  }

  task.processedChunks = processedChunks;
  task.processedItems = processedItems;

  const totalItems = Number.isFinite(task.totalItems) ? task.totalItems : null;
  const chunkGoal = Number.isFinite(task.totalChunks)
    ? task.totalChunks
    : Number.isFinite(task.maxBillableChunks)
    ? task.maxBillableChunks
    : null;
  let progress = task.progress || 0;
  if (totalItems && totalItems > 0) {
    progress = Math.min(100, Math.round((processedItems / totalItems) * 100));
  } else if (chunkGoal && chunkGoal > 0) {
    progress = Math.min(100, Math.round((processedChunks / chunkGoal) * 100));
  }
  task.progress = progress;
  if (progress === 100 && task.status !== "completed") {
    task.status = "completed";
  }
}

app.get("/api/me", (req, res) => {
  try {
    const db = getDb();
    const sessionUser = req.currentUser || findUserBySessionId(db, req.sessionId);
    if (!sessionUser) {
      return res.status(404).json({ error: "User session not found" });
    }
    const transactions = ensureCollection(db, "walletTransactions", []).filter(
      (entry) => entry?.userId === sessionUser.id
    );
    transactions.sort((a, b) => {
      const left = a?.createdAt ? new Date(a.createdAt).getTime() : 0;
      const right = b?.createdAt ? new Date(b.createdAt).getTime() : 0;
      return right - left;
    });
    const limited = transactions.slice(0, 25).map(buildWalletTransactionResponse).filter(Boolean);
    res.json({
      user: buildUserResponse(sessionUser),
      walletTransactions: limited,
      walletTransactionsTotal: transactions.length,
    });
  } catch (error) {
    console.error("wallet profile error", error);
    res.status(500).json({ error: "Failed to load profile" });
  }
});

app.post("/api/wallet/deposit", async (req, res) => {
  try {
    const { amount, reason } = req.body || {};
    const normalized = normalizeCurrencyAmount(amount);
    if (!Number.isFinite(normalized) || normalized <= 0) {
      return res.status(400).json({ error: "Deposit amount must be greater than zero" });
    }
    if (normalized > 1_000_000) {
      return res.status(400).json({ error: "Deposit amount exceeds limit" });
    }
    if (!SANDBOX_WALLET_ENABLED) {
      return res.status(403).json({ error: "Manual wallet deposits are disabled. Configure Stripe to add funds." });
    }
    const db = getDb();
    const sessionUser = req.currentUser || findUserBySessionId(db, req.sessionId);
    if (!sessionUser) {
      return res.status(500).json({ error: "User session not found" });
    }
    const meta = {
      reason: typeof reason === "string" && reason.trim().length > 0 ? reason.trim() : "manual-deposit",
    };
    const transaction = adjustUserBalance(db, sessionUser, normalized, "wallet-deposit", meta);
    await saveDb();
    res.json({
      ok: true,
      user: buildUserResponse(sessionUser),
      transaction: buildWalletTransactionResponse(transaction),
    });
  } catch (error) {
    console.error("wallet deposit error", error);
    res.status(500).json({ error: "Failed to deposit funds" });
  }
});

app.post("/api/wallet/withdraw", async (req, res) => {
  try {
    const { amount, reason } = req.body || {};
    const normalized = normalizeCurrencyAmount(amount);
    if (!Number.isFinite(normalized) || normalized <= 0) {
      return res.status(400).json({ error: "Withdrawal amount must be greater than zero" });
    }
    if (normalized > 1_000_000) {
      return res.status(400).json({ error: "Withdrawal amount exceeds limit" });
    }
    if (!SANDBOX_WALLET_ENABLED) {
      return res.status(403).json({ error: "Manual withdrawals are disabled. Contact support." });
    }
    const db = getDb();
    const sessionUser = req.currentUser || findUserBySessionId(db, req.sessionId);
    if (!sessionUser) {
      return res.status(500).json({ error: "User session not found" });
    }
    const currentBalance = Number(sessionUser.walletBalance || 0);
    if (normalized > currentBalance) {
      return res.status(400).json({ error: "Insufficient wallet balance" });
    }
    const meta = {
      reason: typeof reason === "string" && reason.trim().length > 0 ? reason.trim() : "manual-withdrawal",
    };
    const transaction = adjustUserBalance(db, sessionUser, -normalized, "wallet-withdrawal", meta);
    await saveDb();
    res.json({
      ok: true,
      user: buildUserResponse(sessionUser),
      transaction: buildWalletTransactionResponse(transaction),
    });
  } catch (error) {
    console.error("wallet withdraw error", error);
    res.status(500).json({ error: "Failed to withdraw funds" });
  }
});

// --- Stripe: create checkout session and webhook processing ---
const stripeSecret = process.env.STRIPE_SECRET_KEY || null;
const stripeWebhookSecret = process.env.STRIPE_WEBHOOK_SECRET || null;
const stripeClient = stripeSecret ? new Stripe(String(stripeSecret), { apiVersion: '2023-08-16' }) : null;

function persistStripeSession(db, record) {
  const sessions = ensureCollection(db, 'stripeSessions', []);
  const idx = sessions.findIndex((s) => s.id === record.id);
  if (idx === -1) {
    sessions.push(record);
  } else {
    sessions[idx] = Object.assign({}, sessions[idx], record);
  }
  return record;
}

app.post('/api/stripe/create-checkout-session', async (req, res) => {
  try {
    if (!stripeClient) return res.status(501).json({ error: 'Stripe not configured on server' });
    const { amount } = req.body || {};
    const normalized = normalizeCurrencyAmount(amount);
    if (!Number.isFinite(normalized) || normalized <= 0) {
      return res.status(400).json({ error: 'Amount must be a positive number' });
    }
    if (normalized > 1_000_000) {
      return res.status(400).json({ error: 'Amount exceeds allowed limit' });
    }
    const db = getDb();
    const sessionUser = req.currentUser || findUserBySessionId(db, req.sessionId);

    const origin = req.get('origin') || `${req.protocol}://${req.get('host')}` || 'http://localhost:5173';
    const success_url = `${origin}/?checkout=success&session_id={CHECKOUT_SESSION_ID}`;
    const cancel_url = `${origin}/?checkout=cancel`;

    const checkout = await stripeClient.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'payment',
      line_items: [
        {
          price_data: {
            currency: 'usd',
            product_data: { name: 'Wallet top-up' },
            unit_amount: Math.round(normalized * 100),
          },
          quantity: 1,
        },
      ],
      success_url,
      cancel_url,
      metadata: {
        sessionId: sessionUser?.sessionId || '',
      },
      client_reference_id: sessionUser?.sessionId || undefined,
    });

    // persist a lightweight session record so we can reconcile later
    persistStripeSession(db, {
      id: checkout.id,
      url: checkout.url || null,
      amount: checkout.amount_total ? checkout.amount_total / 100 : normalized,
      currency: checkout.currency || 'usd',
      sessionId: sessionUser?.sessionId || null,
      createdAt: new Date().toISOString(),
      status: 'created',
      raw: checkout,
    });
    await saveDb();

    res.json({ id: checkout.id, url: checkout.url });
  } catch (error) {
    console.error('create-checkout-session error', error);
    res.status(500).json({ error: 'Failed to create checkout session' });
  }
});

// stripe webhook - needs raw body for signature verification
app.post("/api/stripe/webhook", async (req, res) => {
  try {
    if (!stripeClient || !stripeWebhookSecret) {
      return res.status(501).send('Stripe webhook not configured');
    }
    const sig = req.headers['stripe-signature'];
    let event;
    try {
      event = stripeClient.webhooks.constructEvent(req.body, sig, String(stripeWebhookSecret));
    } catch (err) {
      console.error('stripe webhook signature verification failed', err && err.message ? err.message : err);
      return res.status(400).send(`Webhook error: ${err && err.message ? err.message : 'invalid signature'}`);
    }

    if (event.type === 'checkout.session.completed') {
      const session = event.data.object;
      const sessionId = session.id;
      const amountTotal = typeof session.amount_total === 'number' ? session.amount_total : session.amount_total || null;
      const metadata = session.metadata || {};
      const userSessionId = metadata.sessionId || session.client_reference_id || null;
      const db = getDb();

      persistStripeSession(db, {
        id: sessionId,
        status: 'completed',
        amount: amountTotal != null ? amountTotal / 100 : null,
        currency: session.currency || 'usd',
        sessionId: userSessionId || null,
        raw: session,
        updatedAt: new Date().toISOString(),
      });

      // credit user wallet if we can find them
      if (userSessionId && amountTotal) {
        const user = findUserBySessionId(db, userSessionId);
        if (user) {
          const usd = amountTotal / 100.0;
          adjustUserBalance(db, user, usd, 'wallet-deposit', { reason: 'stripe', stripeSessionId: sessionId });
        }
      }

      await saveDb();
    }

    res.json({ received: true });
  } catch (error) {
    console.error('stripe webhook processing error', error);
    res.status(500).send('Webhook handler failed');
  }
});

app.post(
  "/api/tasks",
  upload.fields([
    { name: "code", maxCount: 1 },
    { name: "data", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const db = getDb();
      const taskId = req.taskId || nanoid();
        const {
          name,
          capabilityRequired,
          creditCost,
          inputType,
          metadataJson,
          totalChunks,
          creatorId,
          maxBucketBytes,
          costPerChunk: costPerChunkRaw,
          budgetTotal: budgetTotalRaw,
          maxBillableChunks: maxBillableChunksRaw,
        } = req.body;
      const trimmedName = typeof name === "string" ? name.trim() : "";
      if (!trimmedName) {
        return res.status(400).json({ error: "name is required" });
      }
      if (!capabilityRequired) {
        return res.status(400).json({ error: "capabilityRequired is required" });
      }
      const currentUser = req.currentUser || findUserBySessionId(db, req.sessionId);
      if (!currentUser) {
        return res.status(500).json({ error: "Unable to resolve submitting user" });
      }
      const parsedCost = Number(costPerChunkRaw || creditCost || 0);
      if (!Number.isFinite(parsedCost) || parsedCost <= 0) {
        return res.status(400).json({ error: "costPerChunk must be greater than 0" });
      }
      const parsedMaxChunks = Number(maxBillableChunksRaw || totalChunks || 0);
      const maxBillableChunks = Number.isFinite(parsedMaxChunks) && parsedMaxChunks > 0 ? Math.floor(parsedMaxChunks) : 1;
      const parsedBudget = Number(budgetTotalRaw || 0);
      const budgetTotal = Number.isFinite(parsedBudget) && parsedBudget > 0 ? parsedBudget : parsedCost * maxBillableChunks;
      if (currentUser.walletBalance < budgetTotal) {
        return res.status(400).json({ error: "Insufficient wallet balance for selected budget" });
      }
      if (!req.files || !req.files.code) {
        return res.status(400).json({ error: "code.zip upload is required" });
      }
      const codeFile = req.files.code[0];
      const dataFile = req.files.data ? req.files.data[0] : null;
      const taskDir = path.join(storageDir, taskId);
      const record = {
        id: taskId,
        name: trimmedName,
        status: "queued",
        creatorId: currentUser.sessionId || creatorId || null,
        creatorUserId: currentUser.id,
        capabilityRequired,
        creditCost: parsedCost,
        inputType: inputType || "file",
        metadataJson: metadataJson || null,
        totalChunks: maxBillableChunks,
        processedChunks: 0,
        processedItems: 0,
        progress: 0,
        createdAt: new Date().toISOString(),
        result: null,
        codeFileName: codeFile ? path.basename(codeFile.filename) : null,
        dataFileName: dataFile ? path.basename(dataFile.filename) : null,
        baseUrl: `http://${req.headers.host}`,
        maxBucketBytes: maxBucketBytes ? Number(maxBucketBytes) : null,
        bucketConfig: {
          maxBuckets: maxBillableChunks,
          maxBucketBytes: maxBucketBytes ? Number(maxBucketBytes) : null,
        },
        totalItems: null,
        nextChunkIndex: 0,
        storageId: taskId,
        costPerChunk: parsedCost,
        budgetTotal,
        maxBillableChunks,
        platformFeePercent: Number.isFinite(Number(req.body?.platformFeePercent))
          ? Number(req.body.platformFeePercent)
          : PLATFORM_FEE_PERCENT,
        budgetSpent: 0,
        chunksPaid: 0,
      };
      const codeDest = path.join(taskDir, record.codeFileName);
      if (codeFile.path !== codeDest) {
        fs.renameSync(codeFile.path, codeDest);
      }
      if (dataFile) {
        const dataDest = path.join(taskDir, record.dataFileName);
        if (dataFile.path !== dataDest) {
          fs.renameSync(dataFile.path, dataDest);
        }
      }
      db.data.tasks.push(record);
      await saveDb();
      res.status(201).json({ task: buildTaskResponse(record) });
    } catch (error) {
      console.error("submit task error", error);
      res.status(500).json({ error: "Failed to create task" });
    }
  }
);

app.get("/api/tasks", (req, res) => {
  const db = getDb();
  const { status } = req.query;
  let tasks = [...db.data.tasks];
  if (status) {
    tasks = tasks.filter((t) => t.status === status);
  }
  // compute progress for the response (don't persist on every poll)
  tasks.forEach((task) => computeProgress(task, db));
  const payload = tasks.map(buildTaskResponse).filter(Boolean);
  res.json({ tasks: payload });
});

app.post("/api/tasks/:taskId/claim", async (req, res) => {
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === req.params.taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  if (task.revoked) {
    return res.json({ ok: false, message: 'revoked' });
  }
  // allow worker to pass their workerId so we can track which worker claimed the task
  const { workerId } = req.body || {};
  // Convert single-worker model into multi-worker subscription: add worker to assignedWorkers
  task.assignedWorkers = task.assignedWorkers || [];
  if (workerId && !task.assignedWorkers.includes(workerId)) {
    task.assignedWorkers.push(workerId);
    ensureWorkerUser(db, workerId);
  }
  // mark processing so the task isn't considered queued
  if (task.status === 'queued') task.status = 'processing';
  await saveDb();
  res.json({ task: buildTaskResponse(task) });
});

// Worker drops participation in a task (stop doing any more operations for this worker)
app.post('/api/tasks/:taskId/drop', async (req, res) => {
  try {
    const { workerId } = req.body || {};
    if (!workerId) return res.status(400).json({ error: 'workerId required' });
    const db = getDb();
    const task = db.data.tasks.find((t) => t.id === req.params.taskId);
    if (!task) return res.status(404).json({ error: 'Task not found' });

    // Remove worker from assignedWorkers
    task.assignedWorkers = Array.isArray(task.assignedWorkers) ? task.assignedWorkers.filter((w) => w !== workerId) : [];

    // Remove any active chunk assignments that belong to this worker for this task
    const assignments = db.data.chunkAssignments || [];
    for (let i = assignments.length - 1; i >= 0; i--) {
      const a = assignments[i];
      if (a.taskId === task.id && a.workerId === workerId) {
        // drop assignment so other workers can pick up these chunks
        assignments.splice(i, 1);
      }
    }

    // If no workers remain and there are unfinished chunks, keep task.status as processing so customers can receive results as they arrive
    if (!task.assignedWorkers || task.assignedWorkers.length === 0) {
      // leave status as-is (queued/processing/completed) - do not mark complete here
    }

    await saveDb();
    res.json({ ok: true, task: buildTaskResponse(task) });
  } catch (err) {
    console.error('drop task error', err);
    res.status(500).json({ error: 'Failed to drop task' });
  }
});

// Customer revokes a task: stop all workers and free buckets
app.post('/api/tasks/:taskId/revoke', async (req, res) => {
  try {
    const db = getDb();
    const task = db.data.tasks.find((t) => t.id === req.params.taskId);
    if (!task) return res.status(404).json({ error: 'Task not found' });

    // Mark revoked so workers cannot claim or fetch chunks
    task.revoked = true;

    // Remove assigned workers
    task.assignedWorkers = [];

    // Remove chunk assignments so buckets become available again
    const assignments = db.data.chunkAssignments || [];
    for (let i = assignments.length - 1; i >= 0; i--) {
      const a = assignments[i];
      if (a.taskId === task.id) assignments.splice(i, 1);
    }

    await saveDb();
    res.json({ ok: true, task: buildTaskResponse(task) });
  } catch (err) {
    console.error('revoke task error', err);
    res.status(500).json({ error: 'Failed to revoke task' });
  }
});

// Customer reinvokes a previously revoked task so workers can claim again
app.post('/api/tasks/:taskId/reinvoke', async (req, res) => {
  try {
    const db = getDb();
    const task = db.data.tasks.find((t) => t.id === req.params.taskId);
    if (!task) return res.status(404).json({ error: 'Task not found' });
    task.revoked = false;
    // keep status as queued/processing as-is; workers must claim again
    await saveDb();
    res.json({ ok: true, task: buildTaskResponse(task) });
  } catch (err) {
    console.error('reinvoke task error', err);
    res.status(500).json({ error: 'Failed to reinvoke task' });
  }
});

// Worker asks for the next available chunk to process for a task
app.post('/api/worker/next-chunk', async (req, res) => {
  const { taskId, workerId } = req.body || {};
  if (!taskId || !workerId) return res.status(400).json({ error: 'taskId and workerId required' });
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  const assignedWorkers = Array.isArray(task.assignedWorkers) ? task.assignedWorkers : [];
  if (task.revoked) {
    // Task temporarily revoked by customer
    return res.json({ ok: false, message: 'revoked' });
  }
  if (!assignedWorkers.includes(workerId)) {
    return res.json({ ok: false, message: 'not-assigned' });
  }
  const budgetInfo = resolveTaskBudget(task);
  const assignmentsCollection = db.data.chunkAssignments || [];
  sweepExpiredAssignments(db, task.id);

  const finishedResults = db.data.chunkResults || [];
  const finishedStatuses = new Set(["completed", "failed", "skipped"]);

  let existingResumeAssignment = null;
  for (let i = assignmentsCollection.length - 1; i >= 0; i--) {
    const entry = assignmentsCollection[i];
    if (entry.taskId !== taskId) continue;
    if (entry.workerId !== workerId) continue;
    const relatedResult = finishedResults.find(
      (result) => result.taskId === taskId && result.chunkIndex === entry.chunkIndex
    );
    if (relatedResult && finishedStatuses.has(relatedResult.status)) {
      assignmentsCollection.splice(i, 1);
      continue;
    }
    if (!existingResumeAssignment) {
      existingResumeAssignment = entry;
    } else {
      const existingTime = existingResumeAssignment.assignedAt ? Date.parse(existingResumeAssignment.assignedAt) : 0;
      const candidateTime = entry.assignedAt ? Date.parse(entry.assignedAt) : 0;
      if (candidateTime < existingTime) {
        existingResumeAssignment = entry;
      }
    }
  }

  const activeAssignments = assignmentsCollection.filter((a) => a.taskId === taskId);
  const dataItems = readTaskItems(task);
  if (!Array.isArray(dataItems) || dataItems.length === 0) {
    return res.status(400).json({ error: 'No data items available for task' });
  }
  task.totalItems = dataItems.length;
  const mutatedConfig = ensureBucketConfig(task, dataItems);

  if (existingResumeAssignment) {
    const resumeRange = normalizeRange(existingResumeAssignment, existingResumeAssignment.chunkIndex ?? null);
    const resumeStart = resumeRange?.start ?? existingResumeAssignment.rangeStart ?? existingResumeAssignment.chunkIndex ?? 0;
    const resumeEnd = resumeRange?.end ?? existingResumeAssignment.rangeEnd ?? resumeStart;
    const safeStart = Math.max(0, Math.min(resumeStart, dataItems.length));
    let safeEnd = Math.max(safeStart, Math.min(resumeEnd, dataItems.length));
    if (safeEnd <= safeStart) {
      const fallbackCount = Number.isFinite(existingResumeAssignment.itemsCount)
        ? Math.max(1, existingResumeAssignment.itemsCount)
        : 1;
      safeEnd = Math.min(dataItems.length, safeStart + fallbackCount);
    }
    const chunkData = dataItems.slice(safeStart, safeEnd);

    existingResumeAssignment.expiresAt = new Date(Date.now() + BUCKET_TIMEOUT_MS).toISOString();
    existingResumeAssignment.updatedAt = new Date().toISOString();
    if (!existingResumeAssignment.workerId) existingResumeAssignment.workerId = workerId;

    await saveDb();

    const taskResp = buildTaskResponse(task);
    return res.json({
      ok: true,
      task: taskResp,
      chunkIndex: Number.isFinite(existingResumeAssignment.chunkIndex)
        ? existingResumeAssignment.chunkIndex
        : safeStart,
      chunkData,
      rangeStart: Number.isFinite(safeStart) ? safeStart : null,
      rangeEnd: Number.isFinite(safeEnd) ? safeEnd : null,
      totalItems: dataItems.length,
      bucketBytes: existingResumeAssignment.bytesUsed || null,
      maxBucketBytes: task.bucketConfig.maxBucketBytes,
      resume: true,
      processedCount: Number.isFinite(existingResumeAssignment.processedCount)
        ? existingResumeAssignment.processedCount
        : null,
    });
  }

    const taskCustomer = findUserBySessionId(db, task.creatorId || null);
    if (!DISABLE_BUDGET_CHECKS && budgetInfo.maxBillableChunks > 0 && budgetInfo.chunksPaid + activeAssignments.length >= budgetInfo.maxBillableChunks) {
      return res.json({ ok: false, message: 'budget-exhausted' });
    }
    if (!DISABLE_BUDGET_CHECKS && taskCustomer && taskCustomer.walletBalance < budgetInfo.costPerChunk) {
      return res.json({ ok: false, message: 'insufficient-funds' });
    }

  const finishedRanges = collectRanges((db.data.chunkResults || []).filter((r) => r.taskId === taskId));
  const assignedRanges = collectRanges(activeAssignments);

  const bucket = calculateBucket(task, dataItems, finishedRanges, assignedRanges);
  if (!bucket) {
    if (mutatedConfig) await saveDb();
    return res.json({ ok: false, message: 'no-chunk' });
  }

  const chunkIndex = task.nextChunkIndex || 0;
  task.nextChunkIndex = chunkIndex + 1;
  const expiresAt = new Date(Date.now() + BUCKET_TIMEOUT_MS).toISOString();
  db.data.chunkAssignments.push({
    id: nanoid(),
    taskId,
    chunkIndex,
    workerId,
    assignedAt: new Date().toISOString(),
    expiresAt,
    rangeStart: bucket.rangeStart,
    rangeEnd: bucket.rangeEnd,
    itemsCount: bucket.rangeEnd - bucket.rangeStart,
    processedCount: 0,
    progressRangeEnd: bucket.rangeStart,
    bytesUsed: bucket.bytesUsed,
    lastBatchOffset: 0,
    lastBatchSize: 0,
    updatedAt: new Date().toISOString(),
  });

  task.status = 'processing';
  task.assignedWorkers = task.assignedWorkers || [];
  if (!task.assignedWorkers.includes(workerId)) task.assignedWorkers.push(workerId);

  const chunkData = dataItems.slice(bucket.rangeStart, bucket.rangeEnd);

  await saveDb();

  const taskResp = buildTaskResponse(task);
  res.json({
    ok: true,
    task: taskResp,
    chunkIndex,
    chunkData,
    rangeStart: bucket.rangeStart,
    rangeEnd: bucket.rangeEnd,
    totalItems: dataItems.length,
    bucketBytes: bucket.bytesUsed,
    maxBucketBytes: task.bucketConfig.maxBucketBytes,
  });
});

app.post('/api/worker/heartbeat', (req, res) => {
  const { workerId } = req.body || {};
  if (!workerId) return res.status(400).json({ error: 'workerId required' });
  const now = Date.now();
  if (!workerHeartbeats.has(workerId)) {
    workerHeartbeats.set(workerId, now);
    console.log('registered worker heartbeat', workerId);
  } else {
    workerHeartbeats.set(workerId, now);
  }
  res.json({ ok: true, serverTime: new Date(now).toISOString() });
});

app.get('/api/worker/online/:workerId', (req, res) => {
  const workerId = req.params.workerId;
  if (!workerId) return res.status(400).json({ error: 'workerId required' });
  const ts = workerHeartbeats.get(workerId);
  if (!ts) return res.json({ online: false });
  const delta = Date.now() - ts;
  if (delta > WORKER_TIMEOUT_MS) {
    workerHeartbeats.delete(workerId);
    return res.json({ online: false });
  }
  res.json({ online: true, lastHeartbeat: new Date(ts).toISOString(), ageMs: delta });
});

app.post("/api/worker/set-total-chunks", async (req, res) => {
  const { taskId, totalChunks } = req.body || {};
  if (!taskId || typeof totalChunks !== "number") {
    return res.status(400).json({ error: "taskId and totalChunks are required" });
  }
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  task.totalChunks = totalChunks;
  computeProgress(task, db);
  await saveDb();
  res.json({ ok: true });
});

app.post("/api/worker/record-chunk", async (req, res) => {
  const {
    taskId,
    chunkIndex,
    status,
    resultText,
    rangeStart,
    rangeEnd,
    itemsCount,
    bytesUsed,
    output,
    error,
    itemResults,
  } = req.body || {};
  if (!taskId || typeof chunkIndex !== "number" || !status) {
    return res.status(400).json({ error: "Missing fields" });
  }
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  if (task.status === "queued") {
    task.status = "processing";
  }
  // remove any existing assignment for this chunk (it is completed now)
  const assignments = db.data.chunkAssignments || [];
  const matchedAssignment = assignments.find((entry) => entry.taskId === taskId && entry.chunkIndex === chunkIndex);
  for (let i = assignments.length - 1; i >= 0; i--) {
    const entry = assignments[i];
    if (entry.taskId !== taskId) continue;
    if (entry.chunkIndex === chunkIndex) {
      assignments.splice(i, 1);
      continue;
    }
    if (Number.isFinite(rangeStart) && Number.isFinite(rangeEnd)) {
      const entryRange = normalizeRange(entry, entry.chunkIndex ?? null);
      if (entryRange && entryRange.start === rangeStart && entryRange.end === rangeEnd) {
        assignments.splice(i, 1);
      }
    }
  }

  const range = normalizeRange({ rangeStart, rangeEnd, itemsCount, chunkIndex }, chunkIndex);
  const sanitizedItemPayload = sanitizeItemResults(itemResults);
  const sanitizedItems = sanitizedItemPayload.items;
  const totalItemResults = sanitizedItemPayload.total;
  const itemsTruncated = sanitizedItemPayload.truncated;
  const resolvedItemsCount = Number.isFinite(itemsCount)
    ? itemsCount
    : (Number.isFinite(rangeStart) && Number.isFinite(rangeEnd))
    ? rangeEnd - rangeStart
    : sanitizedItems.length
    ? sanitizedItems.length
    : null;
  const safeResultText = typeof resultText === "string" && resultText.trim().length > 0
    ? resultText
    : buildResultTextFromItems(sanitizedItems, status);
  const rawOutput = typeof output === "string" ? output : safeStringify(output ?? "");
  const safeOutput = truncateText(rawOutput || "");
  const safeError = typeof error === "string" ? error : error ? safeStringify(error) : null;

  const resultsArr = db.data.chunkResults || [];
  for (let i = resultsArr.length - 1; i >= 0; i--) {
    const r = resultsArr[i];
    if (r.taskId !== taskId) continue;
    if (r.chunkIndex === chunkIndex) continue;
    const rRange = normalizeRange(r, Number.isFinite(r.chunkIndex) ? r.chunkIndex : null);
    if (range && rRange) {
      const overlap = Math.max(0, Math.min(range.end, rRange.end) - Math.max(range.start, rRange.start));
      if (overlap > 0) {
        resultsArr.splice(i, 1);
      }
    }
  }

  const existing = resultsArr.find(
    (r) => r.taskId === taskId && r.chunkIndex === chunkIndex
  );
  const resolvedWorkerId = req.body?.workerId || matchedAssignment?.workerId || existing?.workerId || null;
  if (existing) {
    existing.status = status;
    existing.resultText = safeResultText || existing.resultText;
    existing.updatedAt = new Date().toISOString();
    if (Number.isFinite(rangeStart)) existing.rangeStart = rangeStart;
    if (Number.isFinite(rangeEnd)) existing.rangeEnd = rangeEnd;
    if (resolvedItemsCount !== null) existing.itemsCount = resolvedItemsCount;
    if (resolvedItemsCount !== null) {
      existing.processedItems = resolvedItemsCount;
    } else if (Array.isArray(sanitizedItems)) {
      existing.processedItems = sanitizedItems.length;
    }
    if (Number.isFinite(bytesUsed)) existing.bytesUsed = bytesUsed;
    if (output !== undefined) {
      existing.output = safeOutput;
    } else if (safeOutput && safeOutput !== existing.output) {
      existing.output = safeOutput;
    }
    if (safeError !== null || error === null) existing.error = safeError;
    existing.itemResults = sanitizedItems;
    existing.itemResultsTotal = totalItemResults;
    existing.itemResultsTruncated = itemsTruncated;
    if (resolvedWorkerId) existing.workerId = resolvedWorkerId;
  } else {
    const newResult = {
      id: nanoid(),
      taskId,
      chunkIndex,
      status,
      resultText: safeResultText || null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      rangeStart: Number.isFinite(rangeStart) ? rangeStart : null,
      rangeEnd: Number.isFinite(rangeEnd) ? rangeEnd : null,
      itemsCount: resolvedItemsCount,
      bytesUsed: Number.isFinite(bytesUsed) ? bytesUsed : null,
      output: safeOutput || null,
      error: safeError,
      itemResults: sanitizedItems,
      itemResultsTotal,
      itemResultsTruncated: itemsTruncated,
      processedItems: resolvedItemsCount,
      workerId: resolvedWorkerId || null,
    };
    db.data.chunkResults.push(newResult);
  }

  const targetResult = existing || db.data.chunkResults.find((r) => r.taskId === taskId && r.chunkIndex === chunkIndex);
  if (targetResult) {
    const payoutApplied = issueChunkPayout(db, task, targetResult, resolvedWorkerId);
    if (payoutApplied) {
      computeProgress(task, db);
      await saveDb();
      return res.json({ ok: true, payout: true });
    }
  }
  computeProgress(task, db);
  await saveDb();
  res.json({ ok: true });
});

// Delete a task and all related files/data (customer action)
app.delete('/api/tasks/:taskId', async (req, res) => {
  try {
    const db = getDb();
    const taskIndex = (db.data.tasks || []).findIndex((t) => t.id === req.params.taskId);
    if (taskIndex === -1) return res.status(404).json({ error: 'Task not found' });
    const task = db.data.tasks[taskIndex];

    // Remove storage folder if present
    try {
      const storageId = getTaskStorageId(task);
      if (storageId) {
        const folder = path.join(process.cwd(), 'backend', 'storage', String(storageId));
        if (fs.existsSync(folder)) {
          fs.rmSync(folder, { recursive: true, force: true });
        }
      }
    } catch (err) {
      console.warn('failed to remove task storage', err?.message || err);
    }

    // Remove task record
    db.data.tasks.splice(taskIndex, 1);

    // Remove chunk results and assignments tied to task
    db.data.chunkResults = (db.data.chunkResults || []).filter((r) => r.taskId !== req.params.taskId);
    db.data.chunkAssignments = (db.data.chunkAssignments || []).filter((a) => a.taskId !== req.params.taskId);

    await saveDb();
    res.json({ ok: true });
  } catch (err) {
    console.error('delete task error', err);
    res.status(500).json({ error: 'Failed to delete task' });
  }
});

app.post("/api/worker/record-progress", async (req, res) => {
  const {
    taskId,
    chunkIndex,
    workerId,
    rangeStart,
    itemsProcessed,
    totalItems,
    bytesUsed,
    item,
    items,
    batchOffset,
    batchSize,
  } = req.body || {};
  if (!taskId || typeof chunkIndex !== "number" || !Number.isFinite(itemsProcessed)) {
    return res.status(400).json({ error: "taskId, chunkIndex, and itemsProcessed are required" });
  }

  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });

  const assignments = db.data.chunkAssignments || [];
  const assignment = assignments.find((entry) => entry.taskId === taskId && entry.chunkIndex === chunkIndex);
  const total = Number.isFinite(totalItems) ? Math.max(0, Math.floor(totalItems)) : null;
  const processedRaw = Number.isFinite(itemsProcessed) ? Math.max(0, Math.floor(itemsProcessed)) : null;
  const processed = processedRaw !== null && total !== null ? Math.min(processedRaw, total) : processedRaw;
  const normalizedBatchOffset = Number.isFinite(batchOffset) ? Math.max(0, Math.floor(batchOffset)) : null;
  const normalizedBatchSize = Number.isFinite(batchSize) ? Math.max(0, Math.floor(batchSize)) : null;
  if (assignment) {
    assignment.processedCount = processed ?? assignment.processedCount ?? 0;
    if (Number.isFinite(bytesUsed)) assignment.bytesUsed = bytesUsed;
    if (Number.isFinite(rangeStart) && processed !== null) {
      assignment.progressRangeEnd = rangeStart + processed;
    }
    if (normalizedBatchOffset !== null) {
      assignment.lastBatchOffset = normalizedBatchOffset;
    }
    if (normalizedBatchSize !== null) {
      assignment.lastBatchSize = normalizedBatchSize;
    }
    assignment.expiresAt = new Date(Date.now() + BUCKET_TIMEOUT_MS).toISOString();
    assignment.updatedAt = new Date().toISOString();
    if (workerId) assignment.workerId = workerId;
  }

  db.data.chunkResults = db.data.chunkResults || [];
  const resultsArr = db.data.chunkResults;
  let resultEntry = resultsArr.find((r) => r.taskId === taskId && r.chunkIndex === chunkIndex);
  if (!resultEntry) {
    resultEntry = {
      id: nanoid(),
      taskId,
      chunkIndex,
      status: "processing",
      resultText: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      rangeStart: Number.isFinite(rangeStart) ? rangeStart : null,
      rangeEnd: Number.isFinite(rangeStart) ? rangeStart : null,
      itemsCount: total,
      bytesUsed: Number.isFinite(bytesUsed) ? bytesUsed : null,
      output: null,
      error: null,
      itemResults: [],
      itemResultsTotal: 0,
      itemResultsTruncated: false,
      processedItems: processed,
    };
    resultsArr.push(resultEntry);
  }

  if (resultEntry.status !== "completed" && resultEntry.status !== "failed") {
    resultEntry.status = "processing";
  }
  if (Number.isFinite(rangeStart)) {
    if (!Number.isFinite(resultEntry.rangeStart)) {
      resultEntry.rangeStart = rangeStart;
    } else {
      resultEntry.rangeStart = Math.min(resultEntry.rangeStart, rangeStart);
    }
  }
  if (Number.isFinite(rangeStart) && processed !== null) {
    const candidateEnd = rangeStart + processed;
    if (!Number.isFinite(resultEntry.rangeEnd) || candidateEnd > resultEntry.rangeEnd) {
      resultEntry.rangeEnd = candidateEnd;
    }
  }
  if (total !== null) {
    const currentTotal = Number.isFinite(resultEntry.itemsCount) ? resultEntry.itemsCount : 0;
    if (total > currentTotal) {
      resultEntry.itemsCount = total;
    }
  }
  if (processed !== null) {
    const currentProcessed = Number.isFinite(resultEntry.processedItems) ? resultEntry.processedItems : 0;
    if (processed > currentProcessed) {
      resultEntry.processedItems = processed;
    }
  }
  if (Number.isFinite(bytesUsed)) {
    resultEntry.bytesUsed = bytesUsed;
  }

  const rawBatch = Array.isArray(items) && items.length > 0 ? items : item ? [item] : [];
  if (rawBatch.length > 0) {
    resultEntry.itemResults = Array.isArray(resultEntry.itemResults) ? resultEntry.itemResults : [];
    const baseLocal = normalizedBatchOffset !== null ? normalizedBatchOffset : null;
    rawBatch.forEach((rawItem, idx) => {
      const fallbackLocal = baseLocal !== null
        ? baseLocal + idx
        : processed !== null
        ? Math.max(0, processed - rawBatch.length + idx)
        : idx;
      const fallbackGlobal = Number.isFinite(rangeStart) && fallbackLocal !== null
        ? rangeStart + fallbackLocal
        : null;
      const sanitized = sanitizeSingleItem(rawItem, {
        localIndex: Number.isFinite(rawItem?.localIndex) ? rawItem.localIndex : fallbackLocal,
        globalIndex: Number.isFinite(rawItem?.globalIndex) ? rawItem.globalIndex : fallbackGlobal,
        status: rawItem?.status || "completed",
      });
      if (!sanitized) {
        return;
      }

      if (Number.isFinite(sanitized.localIndex)) {
        const localIdx = sanitized.localIndex;
        resultEntry.itemResults = resultEntry.itemResults.filter((existing) => existing && Number.isFinite(existing.localIndex) ? existing.localIndex !== localIdx : true);
      }
      resultEntry.itemResults.push(sanitized);
    });

    resultEntry.itemResults.sort((a, b) => {
      const aIndex = Number.isFinite(a?.localIndex) ? a.localIndex : Number.MAX_SAFE_INTEGER;
      const bIndex = Number.isFinite(b?.localIndex) ? b.localIndex : Number.MAX_SAFE_INTEGER;
      return aIndex - bIndex;
    });

    if (resultEntry.itemResults.length > MAX_ITEM_RESULTS_STORED) {
      resultEntry.itemResults.splice(0, resultEntry.itemResults.length - MAX_ITEM_RESULTS_STORED);
    }
  }

  if (total !== null && processed !== null) {
    resultEntry.output = `Processing ${Math.min(processed, total)} / ${total} item(s)`;
  } else if (processed !== null) {
    resultEntry.output = `Processing ${processed} item(s)`;
  }
  if (processed !== null) {
    const currentTotal = Number.isFinite(resultEntry.itemResultsTotal) ? resultEntry.itemResultsTotal : 0;
    if (processed > currentTotal) {
      resultEntry.itemResultsTotal = processed;
    }
    resultEntry.itemResultsTruncated = (resultEntry.itemResults?.length || 0) < (resultEntry.itemResultsTotal || 0);
  }
  resultEntry.updatedAt = new Date().toISOString();

  computeProgress(task, db);
  await saveDb();
  res.json({
    ok: true,
    processed,
    total,
  });
});

app.get("/api/worker/task-info", (req, res) => {
  const { taskId } = req.query;
  if (!taskId) return res.status(400).json({ error: "taskId required" });
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  computeProgress(task, db);
  const hostBase = `http://${req.headers.host}`;
  const storageId = getTaskStorageId(task);
  const taskBase = storageId ? `${hostBase}/storage/${storageId}` : null;
  res.json({
    taskId: task.id,
    status: task.status,
    totalChunks: task.totalChunks,
    processedChunks: task.processedChunks,
    capabilityRequired: task.capabilityRequired,
    creditCost: task.creditCost,
    inputType: task.inputType,
    metadataJson: task.metadataJson,
    codeUrl: taskBase && task.codeFileName ? `${taskBase}/${task.codeFileName}` : null,
    dataUrl: taskBase && task.dataFileName ? `${taskBase}/${task.dataFileName}` : null,
  });
});

app.get("/api/tasks/:taskId/results", (req, res) => {
  const db = getDb();
  const rawResults = (db.data.chunkResults || [])
    .filter((r) => r.taskId === req.params.taskId)
    .sort((a, b) => a.chunkIndex - b.chunkIndex);
  const results = rawResults.map((r) => {
    const itemResults = Array.isArray(r.itemResults) ? r.itemResults : [];
    const totalItems = Number.isFinite(r.itemResultsTotal)
      ? r.itemResultsTotal
      : itemResults.length;
    return {
      ...r,
      itemResults,
      itemResultsTotal: totalItems,
      itemResultsTruncated: Boolean(r.itemResultsTruncated && totalItems > itemResults.length),
      processedItems: Number.isFinite(r.processedItems) ? Math.max(0, r.processedItems) : null,
    };
  });
  const assignments = (db.data.chunkAssignments || [])
    .filter((a) => a.taskId === req.params.taskId)
    .map(buildAssignmentSummary)
    .sort((a, b) => {
      const aIndex = Number.isFinite(a.chunkIndex) ? a.chunkIndex : Number.MAX_SAFE_INTEGER;
      const bIndex = Number.isFinite(b.chunkIndex) ? b.chunkIndex : Number.MAX_SAFE_INTEGER;
      return aIndex - bIndex;
    });
  res.json({ results, assignments });
});

app.listen(PORT, () => {
  console.log(`Offline task backend ready on http://localhost:${PORT}`);
});
