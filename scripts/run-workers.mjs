#!/usr/bin/env node
import { spawn } from 'child_process';
import path from 'path';
import process from 'process';

function printUsage() {
  console.log('Usage: node scripts/run-workers.mjs workerA workerB ...');
  console.log('Set API_BASE to point at the backend (defaults to http://localhost:4000).');
}

const workerIds = process.argv.slice(2).filter(Boolean);
if (workerIds.length === 0) {
  printUsage();
  process.exit(1);
}

const API_BASE = process.env.API_BASE || 'http://localhost:4000';
const workerScript = path.resolve(path.dirname(new URL(import.meta.url).pathname), 'worker-runner.mjs');

const children = new Map();

function launchWorker(workerId) {
  const child = spawn('node', [workerScript], {
    env: {
      ...process.env,
      WORKER_ID: workerId,
      API_BASE,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  children.set(child.pid, child);

  const prefix = `[worker:${workerId}]`;
  const log = (stream, data) => {
    const lines = data.toString().split(/\r?\n/);
    for (const line of lines) {
      if (!line) continue;
      stream.write(`${prefix} ${line}\n`);
    }
  };

  child.stdout.on('data', (data) => log(process.stdout, data));
  child.stderr.on('data', (data) => log(process.stderr, data));

  child.on('exit', (code, signal) => {
    children.delete(child.pid);
    console.log(`${prefix} exited with ${signal ?? code}`);
  });
}

workerIds.forEach(launchWorker);

function shutdown() {
  console.log('\nStopping all workers...');
  for (const child of children.values()) {
    try {
      child.kill('SIGINT');
    } catch (err) {
      // ignore
    }
  }
  setTimeout(() => process.exit(0), 500);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
