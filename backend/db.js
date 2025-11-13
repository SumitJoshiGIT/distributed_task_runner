import { Low } from 'lowdb';
import { JSONFileSync } from 'lowdb/node';
import path from 'path';
import fs from 'fs';

const dataDir = path.resolve(process.cwd(), 'backend', 'data');
const dbFile = path.join(dataDir, 'database.json');

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const defaultData = { tasks: [], chunkResults: [], chunkAssignments: [] };
const adapter = new JSONFileSync(dbFile);
const db = new Low(adapter, defaultData);

db.read();
if (!db.data) {
  db.data = { ...defaultData };
  db.write();
}

export function getDb() {
  return db;
}

export function saveDb() {
  db.write();
}
