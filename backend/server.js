import express from "express";
import cors from "cors";
import multer from "multer";
import path from "path";
import fs from "fs";
import { getDb, saveDb } from "./db.js";
import { nanoid } from "nanoid";

const app = express();
const PORT = process.env.PORT || 4000;
const WORKER_TIMEOUT_MS = 15000;
const workerHeartbeats = new Map();

setInterval(() => {
  const cutoff = Date.now() - WORKER_TIMEOUT_MS * 2;
  for (const [workerId, ts] of workerHeartbeats.entries()) {
    if (ts < cutoff) {
      workerHeartbeats.delete(workerId);
    }
  }
}, WORKER_TIMEOUT_MS);

app.use(cors());
app.use(express.json({ limit: "10mb" }));

const storageDir = path.resolve(process.cwd(), "backend", "storage");
if (!fs.existsSync(storageDir)) {
  fs.mkdirSync(storageDir, { recursive: true });
}

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
  _db.data.tasks.forEach((task) => {
    if (!task.name) {
      const suffix = task.id ? String(task.id).slice(-6) : nanoid(6);
      task.name = `Task ${suffix}`;
      mutated = true;
    }
  });
  if (mutated) saveDb();
} catch (e) {
  // ignore initialization errors
}

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

function buildTaskResponse(task) {
  if (!task) return null;
  const base = `${task.baseUrl}/storage/${task.id}`;
  return {
    ...task,
    name: task.name || `Task ${task.id}`,
    creatorId: task.creatorId || null,
    workerId: task.workerId || null,
    assignedWorkers: task.assignedWorkers || [],
    codeUrl: task.codeFileName ? `${base}/${task.codeFileName}` : undefined,
    dataUrl: task.dataFileName ? `${base}/${task.dataFileName}` : undefined,
  };
}

function computeProgress(task, db) {
  if (!task.totalChunks || task.totalChunks <= 0) return task.progress || 0;
  const resultsArr = db.data.chunkResults || [];
  // treat completed, skipped and failed as processed for progress
  const finishedStatuses = new Set(["completed", "skipped", "failed"]);
  const processed = resultsArr.filter((r) => r.taskId === task.id && finishedStatuses.has(r.status)).length;
  const progress = Math.min(100, Math.round((processed / task.totalChunks) * 100));
  task.processedChunks = processed;
  task.progress = progress;
  if (progress === 100 && task.status !== "completed") {
    task.status = "completed";
  }
}

app.post(
  "/api/tasks",
  upload.fields([
    { name: "code", maxCount: 1 },
    { name: "data", maxCount: 1 },
  ]),
  (req, res) => {
    try {
      const db = getDb();
      const taskId = req.taskId || nanoid();
      const { name, capabilityRequired, creditCost, inputType, metadataJson, totalChunks, creatorId } = req.body;
      const trimmedName = typeof name === "string" ? name.trim() : "";
      if (!trimmedName) {
        return res.status(400).json({ error: "name is required" });
      }
      if (!capabilityRequired) {
        return res.status(400).json({ error: "capabilityRequired is required" });
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
        creatorId: creatorId || null,
        capabilityRequired,
        creditCost: Number(creditCost) || 0,
        inputType: inputType || "file",
        metadataJson: metadataJson || null,
        totalChunks: totalChunks ? Number(totalChunks) : null,
        processedChunks: 0,
        progress: 0,
        createdAt: new Date().toISOString(),
        result: null,
        codeFileName: codeFile ? path.basename(codeFile.filename) : null,
        dataFileName: dataFile ? path.basename(dataFile.filename) : null,
        baseUrl: `http://${req.headers.host}`,
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
      saveDb();
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
  res.json({ tasks: tasks.map(buildTaskResponse) });
});

app.post("/api/tasks/:taskId/claim", (req, res) => {
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === req.params.taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  // allow worker to pass their workerId so we can track which worker claimed the task
  const { workerId } = req.body || {};
  // Convert single-worker model into multi-worker subscription: add worker to assignedWorkers
  task.assignedWorkers = task.assignedWorkers || [];
  if (workerId && !task.assignedWorkers.includes(workerId)) {
    task.assignedWorkers.push(workerId);
  }
  // mark processing so the task isn't considered queued
  if (task.status === 'queued') task.status = 'processing';
  saveDb();
  res.json({ task: buildTaskResponse(task) });
});

// Worker asks for the next available chunk to process for a task
app.post('/api/worker/next-chunk', (req, res) => {
  const { taskId, workerId } = req.body || {};
  if (!taskId || !workerId) return res.status(400).json({ error: 'taskId and workerId required' });
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  // ensure totalChunks known; try to read data.json if present
  if (!task.totalChunks && task.dataFileName) {
    try {
      const dataPath = path.join(process.cwd(), 'backend', 'storage', task.id, task.dataFileName);
      if (fs.existsSync(dataPath)) {
        const content = fs.readFileSync(dataPath, 'utf8');
        const parsed = JSON.parse(content);
        if (Array.isArray(parsed)) {
          task.totalChunks = parsed.length;
        }
      }
    } catch (e) {
      // ignore
    }
  }
  if (!task.totalChunks || task.totalChunks <= 0) {
    return res.status(400).json({ error: 'totalChunks not available for task' });
  }

  // compute already completed (or otherwise finished) and currently assigned indices
  const finishedStatuses = new Set(["completed", "skipped", "failed"]);
  const completed = new Set((db.data.chunkResults || []).filter(r => r.taskId === taskId && finishedStatuses.has(r.status)).map(r => r.chunkIndex));
  const assigned = new Set((db.data.chunkAssignments || []).filter(a => a.taskId === taskId).map(a => a.chunkIndex));
  // pick first available index
  let chosen = null;
  for (let i = 0; i < task.totalChunks; i++) {
    if (!completed.has(i) && !assigned.has(i)) { chosen = i; break; }
  }
  if (chosen === null) return res.json({ ok: false, message: 'no-chunk' });

  // record assignment
  db.data.chunkAssignments.push({ taskId, chunkIndex: chosen, workerId, assignedAt: new Date().toISOString() });
  task.status = 'processing';
  task.assignedWorkers = task.assignedWorkers || [];
  if (!task.assignedWorkers.includes(workerId)) task.assignedWorkers.push(workerId);
  saveDb();

  // prepare chunk payload from data.json if available
  let chunkData = null;
  if (task.dataFileName) {
    try {
      const dataPath = path.join(process.cwd(), 'backend', 'storage', task.id, task.dataFileName);
      if (fs.existsSync(dataPath)) {
        const parsed = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
        if (Array.isArray(parsed)) {
          const total = parsed.length;
          const start = Math.floor((chosen * total) / task.totalChunks);
          const end = Math.floor(((chosen + 1) * total) / task.totalChunks);
          chunkData = parsed.slice(start, end);
        }
      }
    } catch (e) {
      // ignore
    }
  }

  const taskResp = buildTaskResponse(task);
  res.json({ ok: true, task: taskResp, chunkIndex: chosen, chunkData });
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

app.post("/api/worker/set-total-chunks", (req, res) => {
  const { taskId, totalChunks } = req.body || {};
  if (!taskId || typeof totalChunks !== "number") {
    return res.status(400).json({ error: "taskId and totalChunks are required" });
  }
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  task.totalChunks = totalChunks;
  computeProgress(task, db);
  saveDb();
  res.json({ ok: true });
});

app.post("/api/worker/record-chunk", (req, res) => {
  const { taskId, chunkIndex, status, resultText } = req.body || {};
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
  const assignIdx = db.data.chunkAssignments ? db.data.chunkAssignments.findIndex(a => a.taskId === taskId && a.chunkIndex === chunkIndex) : -1;
  if (assignIdx !== -1) db.data.chunkAssignments.splice(assignIdx, 1);

  const existing = db.data.chunkResults.find(
    (r) => r.taskId === taskId && r.chunkIndex === chunkIndex
  );
  if (existing) {
    existing.status = status;
    existing.resultText = resultText || existing.resultText;
    existing.updatedAt = new Date().toISOString();
  } else {
    db.data.chunkResults.push({
      id: nanoid(),
      taskId,
      chunkIndex,
      status,
      resultText: resultText || null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
  }
  computeProgress(task, db);
  saveDb();
  res.json({ ok: true });
});

app.get("/api/worker/task-info", (req, res) => {
  const { taskId } = req.query;
  if (!taskId) return res.status(400).json({ error: "taskId required" });
  const db = getDb();
  const task = db.data.tasks.find((t) => t.id === taskId);
  if (!task) return res.status(404).json({ error: "Task not found" });
  computeProgress(task, db);
  const hostBase = `http://${req.headers.host}`;
  const taskBase = `${hostBase}/storage/${task.id}`;
  res.json({
    taskId: task.id,
    status: task.status,
    totalChunks: task.totalChunks,
    processedChunks: task.processedChunks,
    capabilityRequired: task.capabilityRequired,
    creditCost: task.creditCost,
    inputType: task.inputType,
    metadataJson: task.metadataJson,
    codeUrl: task.codeFileName ? `${taskBase}/${task.codeFileName}` : null,
    dataUrl: task.dataFileName ? `${taskBase}/${task.dataFileName}` : null,
  });
});

app.get("/api/tasks/:taskId/results", (req, res) => {
  const db = getDb();
  const results = (db.data.chunkResults || [])
    .filter((r) => r.taskId === req.params.taskId)
    .sort((a, b) => a.chunkIndex - b.chunkIndex);
  res.json({ results });
});

app.delete("/api/tasks/:taskId", (req, res) => {
  const db = getDb();
  const idx = db.data.tasks.findIndex((t) => t.id === req.params.taskId);
  if (idx === -1) return res.status(404).json({ error: "Task not found" });
  const [task] = db.data.tasks.splice(idx, 1);
  db.data.chunkResults = (db.data.chunkResults || []).filter((r) => r.taskId !== task.id);
  saveDb();
  const dir = path.join(storageDir, task.id);
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`Offline task backend ready on http://localhost:${PORT}`);
});
