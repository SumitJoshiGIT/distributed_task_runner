#!/usr/bin/env node
/*
  Minimal worker runner service.
  - Polls the backend for queued tasks
  - Claims a task with WORKER_ID
  - Downloads code.zip and data.json if present
  - Extracts code.zip and runs `node main.js` inside the extracted folder
  - If data.json is an array, posts one chunk result per item to /api/worker/record-chunk

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

function log(...args) { console.log(new Date().toISOString(), '[worker]', ...args); }

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
  const tasks = json.tasks || [];
  return tasks.filter((t) => (t.assignedWorkers || []).includes(WORKER_ID) && t.status !== 'completed');
}

async function downloadFile(url, dest) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  const destStream = fs.createWriteStream(dest);
  await pipeline(res.body, destStream);
}

async function processTask(task) {
  log('processing', task.id);
  const taskBase = `${API_BASE}/storage/${task.id}`;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `worker-${task.id}-`));
  let downloadedDataPath = null;
  try {
    // download code.zip if present
    if (task.codeFileName) {
      const url = `${taskBase}/${task.codeFileName}`;
      const codeZip = path.join(tmp, 'code.zip');
      log('download code', url);
      await downloadFile(url, codeZip);
      log('extracting code');
      await fs.createReadStream(codeZip).pipe(unzipper.Extract({ path: tmp })).promise();
    }

    // download data.json if present (worker may still request chunked data from server)
    if (task.dataFileName) {
      const url = `${taskBase}/${task.dataFileName}`;
      const dataDest = path.join(tmp, 'data.json');
      log('download data (for reference)', url);
      try {
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
          cp.stdout.on('data', d => process.stdout.write(`[task ${task.id}] ` + d));
          cp.stderr.on('data', d => process.stderr.write(`[task ${task.id}] ERR ` + d));
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
    while (true) {
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
          // no chunk available right now
          break;
        }
        const idx = nextJson.chunkIndex;
        const chunkData = nextJson.chunkData;
        // run main.js per-chunk if present (optional)
        let statusToSend = 'skipped';
        let resultText = chunkData ? `[input] ${JSON.stringify(chunkData)}\n[output] Skipped (no main.js)` : `[input] -\n[output] Skipped (no main.js)`;
        if (hasMain) {
          // write chunk to chunk.json for the main to consume
          try { fs.writeFileSync(path.join(mainCwd, 'chunk.json'), JSON.stringify(chunkData)); } catch (e) {}
          let stdoutBuf = '';
          let stderrBuf = '';
          try {
            log('running main.js for chunk', idx);
            await new Promise((resolve, reject) => {
              const cp = spawn('node', [main], {
                cwd: mainCwd,
                env: { ...process.env, TASK_ID: task.id, API_BASE, CHUNK_INDEX: String(idx) }
              });
              cp.stdout.on('data', d => {
                stdoutBuf += d.toString();
                process.stdout.write(`[task ${task.id}] ` + d);
              });
              cp.stderr.on('data', d => {
                stderrBuf += d.toString();
                process.stderr.write(`[task ${task.id}] ERR ` + d);
              });
              cp.on('close', code => {
                if (code === 0) resolve(); else reject(new Error(`main.js exited ${code}`));
              });
            });
            statusToSend = 'completed';
            const outputText = stdoutBuf.trim() || `Processed chunk ${idx}`;
            resultText = chunkData ? `[input] ${JSON.stringify(chunkData)}\n[output] ${outputText}` : `[input] -\n[output] ${outputText}`;
          } catch (e) {
            statusToSend = 'failed';
            const errorText = (stderrBuf.trim() || e.message || 'Unknown error');
            resultText = chunkData ? `[input] ${JSON.stringify(chunkData)}\n[output] Failed: ${errorText}` : `[input] -\n[output] Failed: ${errorText}`;
            log('main.js chunk run failed', e.message);
          }
        }
        const recordRes = await fetch(`${API_BASE}/api/worker/record-chunk`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ taskId: task.id, chunkIndex: idx, status: statusToSend, resultText }),
        });
        if (!recordRes.ok) {
          const body = await recordRes.text();
          log('record-chunk failed', recordRes.status, body);
          break;
        }
        log('posted chunk', idx, statusToSend);
        // small pause to avoid tight loop
        await new Promise(r => setTimeout(r, 200));
      } catch (e) {
        log('error requesting/processing chunk', e.message);
        break;
      }
    }

  } finally {
    // cleanup
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch (e) {}
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
      }
    } catch (e) {
      log('poll error', e.message);
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
