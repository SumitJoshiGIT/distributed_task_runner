#!/usr/bin/env node
import mongoose from 'mongoose';
import { initDb, getDb, syncToMongo, isMongoReady } from '../backend/db.js';

async function run() {
  await initDb();
  if (!isMongoReady()) {
    console.error('MONGO_URI is not configured or MongoDB connection failed. Aborting migration.');
    process.exitCode = 1;
    return;
  }

  const db = getDb();
  const tasksCount = Array.isArray(db.data?.tasks) ? db.data.tasks.length : 0;
  const resultsCount = Array.isArray(db.data?.chunkResults) ? db.data.chunkResults.length : 0;
  const assignmentsCount = Array.isArray(db.data?.chunkAssignments) ? db.data.chunkAssignments.length : 0;

  console.log(`Migrating data to MongoDB...`);
  console.log(`  Tasks:         ${tasksCount}`);
  console.log(`  Chunk results: ${resultsCount}`);
  console.log(`  Assignments:   ${assignmentsCount}`);

  await syncToMongo();
  await mongoose.disconnect();

  console.log('Migration complete.');
}

run().catch((error) => {
  console.error('Migration failed:', error?.message || error);
  process.exitCode = 1;
});
