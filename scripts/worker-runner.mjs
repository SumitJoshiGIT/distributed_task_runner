#!/usr/bin/env node
/*
  Minimal worker runner service.
  - Polls the backend for queued tasks
  - Claims a task with WORKER_ID
  - Downloads code.zip and data.json if present
  - Extracts code.zip and runs `node main.js` inside the extracted folder
  - If data.json is an array, posts one bucket result per item batch to /api/worker/record-chunk

  Usage:
    WORKER_ID=my-worker-1 API_BASE=http://localhost:4000 node scripts/worker-runner.mjs

  Optionally set POLL_INTERVAL (ms) and CHUNK_SIZE (number)
*/

import fs from 'fs';
import path from 'path';
import os from 'os';
import fetch from 'node-fetch';
import { pipeline } from 'stream/promises';
import unzipper from 'unzipper';
import { spawn } from 'child_process';

const WORKER_ID = process.env.WORKER_ID || process.env.WORKER_TOKEN || 'dev-worker';
const API_BASE = process.env.API_BASE || 'http://localhost:4000';
const POLL_INTERVAL = Number(process.env.POLL_INTERVAL) || 4000;
const HEARTBEAT_INTERVAL = Number(process.env.HEARTBEAT_INTERVAL) || 5000;
const ITEM_PREVIEW_LIMIT = Number(process.env.ITEM_PREVIEW_LIMIT) || 200;
const SUMMARY_PREVIEW_LIMIT = Number(process.env.ITEM_SUMMARY_LIMIT) || 20;
const PROGRESS_BATCH_SIZE = Number(process.env.PROGRESS_BATCH_SIZE) || 5;
const NO_CHUNK_BACKOFF_MS = Number(process.env.NO_CHUNK_BACKOFF_MS) || Math.min(POLL_INTERVAL, 1500);
const NO_CHUNK_MAX_RETRIES = Number.isFinite(Number(process.env.NO_CHUNK_MAX_RETRIES))
  ? Math.max(1, Number(process.env.NO_CHUNK_MAX_RETRIES))
  : 12;
const VERBOSE_WORKER_LOGS = process.env.VERBOSE_WORKER_LOGS === 'true';

function log(...args) {
  if (!VERBOSE_WORKER_LOGS) return;
  console.log(new Date().toISOString(), '[worker]', ...args);
}

function safeStringify(value) {
  try {
    if (typeof value === 'string') return value;
    return JSON.stringify(value);
  } catch (error) {
    return String(value);
  }
}

let completedChunksCount = 0;
let lastStatusLine = '';

function updateStatusLine({ chunkIndex = '-', itemsInChunk = '-', itemsProcessed = 0 } = {}) {
  const chunkLabel = Number.isFinite(chunkIndex) ? chunkIndex : (chunkIndex ?? '-');
  const itemsLabel = Number.isFinite(itemsInChunk) ? itemsInChunk : (itemsInChunk ?? '-');
  const processedLabel = Number.isFinite(itemsProcessed) ? itemsProcessed : (itemsProcessed ?? '-');
  const line = `${WORKER_ID} chunk:${chunkLabel} items:${itemsLabel} processed:${processedLabel} completed_chunks:${completedChunksCount}`;
  if (line === lastStatusLine) return;
  process.stdout.write(`\r\x1b[K${line}`);
  lastStatusLine = line;
}

function setIdleStatus() {
  updateStatusLine({ chunkIndex: '-', itemsInChunk: 0, itemsProcessed: 0 });
}

setIdleStatus();

process.once('exit', () => {
  if (lastStatusLine) process.stdout.write('\n');
});

function previewValue(value, limit = ITEM_PREVIEW_LIMIT) {
  const raw = safeStringify(value) || '';
  if (raw.length <= limit) return raw;
  return `${raw.slice(0, limit)}... (+${raw.length - limit} chars)`;
}

function summarizeItemResults(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return {
      inputLines: ['(no chunk items)'],
      outputLines: ['(no output)'],
    };
  }
  const limited = items.slice(0, SUMMARY_PREVIEW_LIMIT);
  const buildLabel = (item, idx) => {
    if (Number.isFinite(item.globalIndex)) {
      return `#${item.globalIndex}`;
    }
    return `item ${Number.isFinite(item.localIndex) ? item.localIndex : idx}`;
  };
  const inputLines = limited.map((item, idx) => `${buildLabel(item, idx)}: ${item.inputPreview || ''}`.trim());
  const outputLines = limited.map((item, idx) => {
    const base = item.status === 'failed' ? (item.error || item.output || 'failed') : (item.output || 'completed');
    return `${buildLabel(item, idx)}: ${base}`.trim();
  });
  if (items.length > SUMMARY_PREVIEW_LIMIT) {
    const remaining = items.length - SUMMARY_PREVIEW_LIMIT;
    inputLines.push(`...(and ${remaining} more)`);
    outputLines.push(`...(and ${remaining} more)`);
  }
  return { inputLines, outputLines };
}

async function recordProgressUpdate(payload) {
  try {
    const res = await fetch(`${API_BASE}/api/worker/record-progress`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (res.status === 404) {
      const body = await res.text();
      log('record-progress failed', res.status, body.slice(0, 200));
      return { ok: false, fatal: 'task-not-found' };
    }
    if (!res.ok) {
      const body = await res.text();
      log('record-progress failed', res.status, body.slice(0, 200));
      return { ok: false };
    }
    return { ok: true };
  } catch (error) {
    log('record-progress error', error.message);
    return { ok: false };
  }
}

async function sendHeartbeat() {
  try {
    await fetch(`${API_BASE}/api/worker/heartbeat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ workerId: WORKER_ID }),
    });
  } catch (error) {
    log('heartbeat failed', error.message);
  }
}

let heartbeatTimer = null;

function findMainScript(rootDir) {
  const stack = [rootDir];
  while (stack.length) {
    const current = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch (e) {
      continue;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isFile() && entry.name.toLowerCase() === 'main.js') {
        return full;
      }
      if (entry.isDirectory()) {
        stack.push(full);
      }
    }
  }
  return null;
}

async function listAssignedTasks() {
  // fetch all tasks and filter for those assigned to this worker and not completed
  const res = await fetch(`${API_BASE}/api/tasks`);
  if (!res.ok) throw new Error(await res.text());
  const json = await res.json();
  const tasks = (json.tasks || []).map((task) => {
    const fallbackCost = Number(task.creditCost) || Number(task.costPerChunk) || 1;
    const costPerChunk = Number.isFinite(Number(task.costPerChunk)) ? Number(task.costPerChunk) : fallbackCost;
    const budgetTotal = Number.isFinite(Number(task.budgetTotal)) ? Number(task.budgetTotal) : costPerChunk;
    const budgetSpent = Number.isFinite(Number(task.budgetSpent)) ? Number(task.budgetSpent) : 0;
    const maxBillableChunks = Number.isFinite(Number(task.maxBillableChunks))
      ? Number(task.maxBillableChunks)
      : Number(task.totalChunks) || 1;
    const chunksPaid = Number.isFinite(Number(task.chunksPaid)) ? Number(task.chunksPaid) : 0;
    const remainingBudget = Math.max(0, budgetTotal - budgetSpent);
    const remainingChunks = Math.max(0, maxBillableChunks - chunksPaid);
    return {
      ...task,
      costPerChunk,
      budgetTotal,
      budgetSpent,
      maxBillableChunks,
      chunksPaid,
      remainingBudget,
      remainingChunks,
    };
  });
  return tasks.filter((t) => (t.assignedWorkers || []).includes(WORKER_ID) && t.status !== 'completed' && t.remainingChunks > 0 && t.remainingBudget >= t.costPerChunk);
}

async function downloadFile(url, dest) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  const destStream = fs.createWriteStream(dest);
  await pipeline(res.body, destStream);
}

async function processTask(task) {
  log('processing', task.id);
  const storageId = task.storageId || task.id;
  const taskBase = storageId ? `${API_BASE}/storage/${storageId}` : null;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `worker-${task.id}-`));
  let downloadedDataPath = null;
  try {
    // download code.zip if present
    if (task.codeFileName) {
      const url = task.codeUrl || (taskBase ? `${taskBase}/${task.codeFileName}` : null);
      if (!url) {
        throw new Error('code.zip URL unavailable for task');
      }
      const codeZip = path.join(tmp, 'code.zip');
      log('download code', url);
      await downloadFile(url, codeZip);
      log('extracting code');
      await fs.createReadStream(codeZip).pipe(unzipper.Extract({ path: tmp })).promise();
    }

    // download data.json if present (worker may still request chunked data from server)
    if (task.dataFileName) {
      const url = task.dataUrl || (taskBase ? `${taskBase}/${task.dataFileName}` : null);
      const dataDest = path.join(tmp, 'data.json');
      if (url) {
        log('download data (for reference)', url);
      }
      try {
        if (!url) throw new Error('data.json URL unavailable for task');
        await downloadFile(url, dataDest);
        downloadedDataPath = dataDest;
      } catch (e) {
        log('warning: failed to download data.json for reference', e.message);
      }
    }

    // run main.js if exists
    const main = findMainScript(tmp);
    const hasMain = Boolean(main);
    const mainCwd = hasMain ? path.dirname(main) : tmp;
    if (hasMain && downloadedDataPath) {
      const targetPath = path.join(mainCwd, 'data.json');
      try {
        if (!fs.existsSync(targetPath)) {
          fs.copyFileSync(downloadedDataPath, targetPath);
        }
      } catch (e) {
        log('warning: failed to place data.json next to main.js', e.message);
      }
    }
    if (hasMain) {
      log('running main.js (bootstrap)', main);
      try {
        await new Promise((resolve, reject) => {
          const cp = spawn('node', [main], {
            cwd: mainCwd,
            env: { ...process.env, TASK_ID: task.id, API_BASE }
          });
          cp.stdout.on('data', (d) => {
            if (VERBOSE_WORKER_LOGS) process.stdout.write(`[task ${task.id}] ` + d);
          });
          cp.stderr.on('data', (d) => {
            if (VERBOSE_WORKER_LOGS) process.stderr.write(`[task ${task.id}] ERR ` + d);
          });
          cp.on('close', code => {
            if (code === 0) resolve(); else reject(new Error(`main.js exited ${code}`));
          });
        });
      } catch (e) {
        log('main.js bootstrap failed', e.message);
      }
    } else {
      log('no main.js found, skipping execution');
    }

    // repeatedly request next chunk assignment from the server and process it
    let idleIterations = 0;
    while (true) {
      let abortTask = false;
      let abortReason = null;
      try {
        const nextRes = await fetch(`${API_BASE}/api/worker/next-chunk`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ taskId: task.id, workerId: WORKER_ID }),
        });
        if (!nextRes.ok) {
          const t = await nextRes.text();
          log('next-chunk error', t);
          break;
        }
        const nextJson = await nextRes.json();
        if (!nextJson.ok) {
          const rawMessage = typeof nextJson.message === 'string' ? nextJson.message : '';
          const normalizedMessage = rawMessage.toLowerCase();
          if (normalizedMessage === 'no-chunk' || normalizedMessage === 'no-chunks') {
            idleIterations += 1;
            if (idleIterations >= NO_CHUNK_MAX_RETRIES) {
              log('no chunks available after multiple retries', task.id, '- pausing');
              break;
            }
            await new Promise((resolve) => setTimeout(resolve, NO_CHUNK_BACKOFF_MS));
            setIdleStatus();
            continue;
          }
          if (normalizedMessage === 'not-assigned') {
            log('worker no longer assigned to task', task.id, '- stopping processing');
            break;
          }
          if (normalizedMessage === 'revoked') {
            log('task temporarily revoked by customer', task.id, '- pausing');
            break;
          }
          if (normalizedMessage === 'budget-exhausted' || normalizedMessage === 'insufficient-funds') {
            log('task cannot allocate more chunks', task.id, '-', rawMessage);
            break;
          }
          log('next-chunk unavailable', rawMessage || 'unknown reason');
          break;
        }
        idleIterations = 0;
        const idx = nextJson.chunkIndex;
        const rawChunkData = nextJson.chunkData;
        const rangeStart = typeof nextJson.rangeStart === 'number' ? nextJson.rangeStart : null;
        const rangeEnd = typeof nextJson.rangeEnd === 'number' ? nextJson.rangeEnd : null;
        const bucketBytes = typeof nextJson.bucketBytes === 'number' ? nextJson.bucketBytes : null;
        const chunkItems = Array.isArray(rawChunkData)
          ? rawChunkData
          : (rawChunkData !== undefined && rawChunkData !== null ? [rawChunkData] : []);
        const rangeItemCount = (rangeStart !== null && rangeEnd !== null) ? Math.max(0, rangeEnd - rangeStart) : null;
        const itemsCount = Number.isFinite(nextJson.itemsCount)
          ? nextJson.itemsCount
          : rangeItemCount !== null
          ? rangeItemCount
          : chunkItems.length;
        const totalItems = Number.isFinite(itemsCount) ? itemsCount : chunkItems.length;

        updateStatusLine({ chunkIndex: idx, itemsInChunk: totalItems, itemsProcessed: 0 });

  const itemResults = [];
  let completedCount = 0;
  let failedCount = 0;
  let skippedCount = 0;

        const progressBuffer = [];
        const flushProgress = async (force = false) => {
          if (abortTask) {
            progressBuffer.length = 0;
            return;
          }
          if (progressBuffer.length === 0) return;
          if (!force && progressBuffer.length < PROGRESS_BATCH_SIZE) return;
          const first = progressBuffer[0];
          const fallbackOffset = itemResults.length - progressBuffer.length;
          const batchOffset = Number.isFinite(first?.localIndex)
            ? first.localIndex
            : Math.max(0, fallbackOffset);
          const safeProcessedCount = Number.isFinite(totalItems)
            ? Math.min(itemResults.length, totalItems)
            : itemResults.length;
          const progressResult = await recordProgressUpdate({
            taskId: task.id,
            chunkIndex: idx,
            workerId: WORKER_ID,
            rangeStart,
            itemsProcessed: safeProcessedCount,
            totalItems,
            bytesUsed: bucketBytes,
            batchOffset,
            batchSize: progressBuffer.length,
            items: progressBuffer.map((entry) => ({ ...entry })),
          });
          if (progressResult?.fatal === 'task-not-found') {
            abortTask = true;
            abortReason = 'task-not-found';
          }
          progressBuffer.length = 0;
        };

        if (chunkItems.length === 0) {
          itemResults.push({
            localIndex: 0,
            globalIndex: rangeStart,
            status: 'skipped',
            inputPreview: '(empty bucket)',
            output: 'No items to process',
            error: null,
          });
          skippedCount = 1;
          updateStatusLine({ chunkIndex: idx, itemsInChunk: totalItems, itemsProcessed: 0 });
        } else {
          for (let itemOffset = 0; itemOffset < chunkItems.length; itemOffset++) {
            const chunkItem = chunkItems[itemOffset];
            const globalIndex = rangeStart !== null ? rangeStart + itemOffset : null;
            const inputPreview = previewValue(chunkItem);
            let itemStatus = hasMain ? 'completed' : 'skipped';
            let outputText = hasMain ? '' : 'Skipped (no main.js)';
            let errorText = null;

            if (hasMain) {
              const chunkPath = path.join(mainCwd, 'chunk.json');
              try {
                fs.writeFileSync(chunkPath, JSON.stringify(chunkItem));
              } catch (error) {
                log('failed to write chunk.json', error.message);
              }
              let stdoutBuf = '';
              let stderrBuf = '';
              try {
                log('running main.js for bucket item', `${idx}:${itemOffset}`);
                await new Promise((resolve, reject) => {
                  const cp = spawn('node', [main], {
                    cwd: mainCwd,
                    env: {
                      ...process.env,
                      TASK_ID: task.id,
                      API_BASE,
                      CHUNK_INDEX: String(idx),
                      CHUNK_ITEM_INDEX: String(itemOffset),
                      CHUNK_GLOBAL_INDEX: globalIndex !== null ? String(globalIndex) : '',
                      CHUNK_ITEMS_TOTAL: String(chunkItems.length),
                    },
                  });
                  cp.stdout.on('data', (d) => {
                    stdoutBuf += d.toString();
                    if (VERBOSE_WORKER_LOGS) process.stdout.write(`[task ${task.id}] ` + d);
                  });
                  cp.stderr.on('data', (d) => {
                    stderrBuf += d.toString();
                    if (VERBOSE_WORKER_LOGS) process.stderr.write(`[task ${task.id}] ERR ` + d);
                  });
                  cp.on('close', (code) => {
                    if (code === 0) resolve(); else reject(new Error(`main.js exited ${code}`));
                  });
                });
                outputText = stdoutBuf.trim() || `Processed bucket item ${globalIndex ?? itemOffset}`;
              } catch (error) {
                itemStatus = 'failed';
                const stderrText = stderrBuf.trim();
                errorText = (stderrText && stderrText.length > 0) ? stderrText : (error.message || 'Unknown error');
                outputText = `Failed: ${errorText}`;
                log('main.js bucket item run failed', error.message);
              }
            }

            if (itemStatus === 'completed') completedCount += 1;
            else if (itemStatus === 'failed') failedCount += 1;
            else skippedCount += 1;

            const itemResult = {
              localIndex: itemOffset,
              globalIndex,
              status: itemStatus,
              inputPreview,
              output: outputText,
              error: errorText,
            };

            itemResults.push(itemResult);
            progressBuffer.push({ ...itemResult });

            const shouldForceFlush = progressBuffer.length >= PROGRESS_BATCH_SIZE || itemOffset === chunkItems.length - 1;
            await flushProgress(shouldForceFlush);
            const processedSoFar = Number.isFinite(totalItems)
              ? Math.min(itemResults.length, totalItems)
              : itemResults.length;
            updateStatusLine({ chunkIndex: idx, itemsInChunk: totalItems, itemsProcessed: processedSoFar });
            if (abortTask) {
              log('aborting chunk due to task removal', task.id);
              break;
            }
          }
        }

        await flushProgress(true);
        if (abortTask) {
          log('task no longer available while processing chunk', task.id);
          break;
        }

        const statusToSend = failedCount > 0
          ? 'failed'
          : completedCount > 0
          ? 'completed'
          : 'skipped';

        const summaryText = hasMain
          ? `Processed ${completedCount}/${totalItems} item(s). Failures: ${failedCount}.`
          : `Skipped ${totalItems} item(s). Worker missing main.js.`;

        const { inputLines, outputLines } = summarizeItemResults(itemResults);
        const resultText = `[input]\n${inputLines.join('\n')}\n[output]\n${outputLines.join('\n')}`;

        const resultPayload = {
          taskId: task.id,
          chunkIndex: idx,
          rangeStart,
          rangeEnd,
          itemsCount: Number.isFinite(itemsCount) ? itemsCount : chunkItems.length,
          bytesUsed: bucketBytes,
          status: statusToSend,
          resultText,
          output: summaryText,
          itemResults,
          itemResultsTotal: Number.isFinite(itemsCount) ? itemsCount : chunkItems.length,
          processedItems: Number.isFinite(totalItems) ? Math.min(itemResults.length, totalItems) : itemResults.length,
          workerId: WORKER_ID,
        };
        if (failedCount > 0) {
          const firstError = itemResults.find((item) => item.status === 'failed' && item.error);
          if (firstError?.error) resultPayload.error = firstError.error;
        }
        const recordRes = await fetch(`${API_BASE}/api/worker/record-chunk`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(resultPayload),
        });
        if (recordRes.status === 404) {
          const body = await recordRes.text();
          log('record-bucket failed', recordRes.status, body);
          abortTask = true;
          abortReason = 'task-not-found';
          break;
        }
        if (!recordRes.ok) {
          const body = await recordRes.text();
          log('record-bucket failed', recordRes.status, body);
          break;
        }
        log('posted bucket', idx, statusToSend, `(${completedCount} completed, ${failedCount} failed, ${skippedCount} skipped)`);
        if (!abortTask) {
          completedChunksCount += 1;
          updateStatusLine({ chunkIndex: idx, itemsInChunk: totalItems, itemsProcessed: totalItems });
        }
        // small pause to avoid tight loop
        await new Promise(r => setTimeout(r, 200));

        if (abortTask) {
          break;
        }
      } catch (e) {
        log('error requesting/processing chunk', e.message);
        break;
      }

      if (abortTask && abortReason === 'task-not-found') {
        log('stopping task processing because backend no longer knows task', task.id);
        break;
      }
    }

  } finally {
    // cleanup
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch (e) {}
    setIdleStatus();
  }
}

async function loop() {
  while (true) {
    try {
      const assigned = await listAssignedTasks();
      if (assigned.length > 0) {
        for (const t of assigned) {
          try {
            log('processing assigned task', t.id);
            await processTask(t);
          } catch (e) {
            log('process error', e.message);
          }
        }
      } else {
        setIdleStatus();
      }
    } catch (e) {
      log('poll error', e.message);
      setIdleStatus();
    }
    if (!heartbeatTimer) {
      heartbeatTimer = setInterval(() => {
        sendHeartbeat();
      }, HEARTBEAT_INTERVAL);
      sendHeartbeat();
      const cleanup = () => {
        if (heartbeatTimer) {
          clearInterval(heartbeatTimer);
          heartbeatTimer = null;
        }
      };
      process.once('exit', cleanup);
      process.once('SIGINT', () => {
        cleanup();
        process.exit(0);
      });
      process.once('SIGTERM', () => {
        cleanup();
        process.exit(0);
      });
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL));
  }
}

// start the loop
log('worker runner starting', { WORKER_ID, API_BASE, POLL_INTERVAL });
loop();
