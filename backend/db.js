import { Low } from 'lowdb';
import { JSONFileSync } from 'lowdb/node';
import path from 'path';
import fs from 'fs';
import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

const dataDir = path.resolve(process.cwd(), 'backend', 'data');
const dbFile = path.join(dataDir, 'database.json');

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const defaultData = {
  tasks: [],
  chunkResults: [],
  chunkAssignments: [],
  users: [],
  walletTransactions: [],
  stripeSessions: [],
  platformLedger: { totalEarnings: 0 },
};
const adapter = new JSONFileSync(dbFile);
const db = new Low(adapter, defaultData);

db.read();
if (!db.data) {
  db.data = { ...defaultData };
  db.write();
}

mongoose.set('strictQuery', false);

const schemaOptions = { strict: false, minimize: false, versionKey: false };

const TaskSchema = new mongoose.Schema({}, schemaOptions);
TaskSchema.index({ id: 1 }, { unique: true, sparse: true });

const ChunkResultSchema = new mongoose.Schema({}, schemaOptions);
ChunkResultSchema.index({ taskId: 1, chunkIndex: 1 }, { unique: true, sparse: true });

const ChunkAssignmentSchema = new mongoose.Schema({}, schemaOptions);
ChunkAssignmentSchema.index({ taskId: 1, chunkIndex: 1 }, { sparse: true });

const UserSchema = new mongoose.Schema({}, schemaOptions);
UserSchema.index({ id: 1 }, { unique: true, sparse: true });
UserSchema.index({ sessionId: 1 }, { unique: true, sparse: true });

const WalletTransactionSchema = new mongoose.Schema({}, schemaOptions);
WalletTransactionSchema.index({ userId: 1, createdAt: -1 });

const StripeSessionSchema = new mongoose.Schema({}, schemaOptions);
StripeSessionSchema.index({ sessionId: 1 }, { unique: true, sparse: true });

const TaskModel = mongoose.models.Task || mongoose.model('Task', TaskSchema, 'tasks');
const ChunkResultModel =
  mongoose.models.ChunkResult || mongoose.model('ChunkResult', ChunkResultSchema, 'chunkResults');
const ChunkAssignmentModel =
  mongoose.models.ChunkAssignment || mongoose.model('ChunkAssignment', ChunkAssignmentSchema, 'chunkAssignments');
const UserModel = mongoose.models.User || mongoose.model('User', UserSchema, 'users');
const WalletTransactionModel =
  mongoose.models.WalletTransaction || mongoose.model('WalletTransaction', WalletTransactionSchema, 'walletTransactions');
const StripeSessionModel =
  mongoose.models.StripeSession || mongoose.model('StripeSession', StripeSessionSchema, 'stripeSessions');

let mongoReady = false;
let syncQueue = Promise.resolve();

function getArray(source) {
  return Array.isArray(source) ? source : [];
}

function normalizeTaskRecord(raw) {
  if (!raw || typeof raw !== 'object') return raw;
  const record = { ...raw };
  if (!record.id && record._id) {
    record.id = String(record._id);
  } else if (record.id) {
    record.id = String(record.id);
  }
  if (record.storageId) {
    record.storageId = String(record.storageId);
  }
  delete record._id;
  return record;
}

function normalizeGenericRecord(raw) {
  if (!raw || typeof raw !== 'object') return raw;
  const record = { ...raw };
  if (record.id) {
    record.id = String(record.id);
  }
  if (record.taskId) {
    record.taskId = String(record.taskId);
  }
  if (record.userId) {
    record.userId = String(record.userId);
  }
  delete record._id;
  return record;
}

function normalizeUserRecord(raw) {
  if (!raw || typeof raw !== 'object') return raw;
  const record = { ...raw };
  if (record.id) record.id = String(record.id);
  if (record.sessionId) record.sessionId = String(record.sessionId);
  delete record._id;
  return record;
}

export async function syncToMongo() {
  if (!mongoReady) return;
  syncQueue = syncQueue.then(async () => {
    const tasks = getArray(db.data?.tasks);
    const chunkResults = getArray(db.data?.chunkResults);
    const chunkAssignments = getArray(db.data?.chunkAssignments);
    const users = getArray(db.data?.users);
    const walletTransactions = getArray(db.data?.walletTransactions);
    const stripeSessions = getArray(db.data?.stripeSessions);
    try {
      const normalizedTasks = tasks.map(normalizeTaskRecord);
      const normalizedResults = chunkResults.map(normalizeGenericRecord);
      const normalizedAssignments = chunkAssignments.map(normalizeGenericRecord);
      const normalizedUsers = users.map(normalizeUserRecord);
      const normalizedWalletTransactions = walletTransactions.map(normalizeGenericRecord);
      const normalizedStripeSessions = stripeSessions.map(normalizeGenericRecord);
      await Promise.all([
        TaskModel.deleteMany({}),
        ChunkResultModel.deleteMany({}),
        ChunkAssignmentModel.deleteMany({}),
        UserModel.deleteMany({}),
        WalletTransactionModel.deleteMany({}),
        StripeSessionModel.deleteMany({}),
      ]);
      const operations = [];
      if (normalizedTasks.length) operations.push(TaskModel.insertMany(normalizedTasks, { ordered: false }));
      if (normalizedResults.length) operations.push(ChunkResultModel.insertMany(normalizedResults, { ordered: false }));
      if (normalizedAssignments.length) operations.push(ChunkAssignmentModel.insertMany(normalizedAssignments, { ordered: false }));
      if (normalizedUsers.length) operations.push(UserModel.insertMany(normalizedUsers, { ordered: false }));
      if (normalizedWalletTransactions.length)
        operations.push(WalletTransactionModel.insertMany(normalizedWalletTransactions, { ordered: false }));
      if (normalizedStripeSessions.length)
        operations.push(StripeSessionModel.insertMany(normalizedStripeSessions, { ordered: false }));
      if (operations.length) {
        await Promise.all(operations);
      }
    } catch (error) {
      console.error('Failed to sync data to MongoDB', error?.message || error);
    }
  });
  try {
    await syncQueue;
  } catch (error) {
    console.error('Mongo sync queue error', error?.message || error);
  }
}

export async function initDb() {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    console.warn('MONGO_URI not set. Continuing with file-based storage only.');
    return;
  }
  try {
    await mongoose.connect(uri, { serverSelectionTimeoutMS: 5000 });
    mongoReady = true;
    console.log('MongoDB connection established');
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error?.message || error);
    return;
  }

  try {
    const [tasks, chunkResults, chunkAssignments] = await Promise.all([
      TaskModel.find().lean().exec(),
      ChunkResultModel.find().lean().exec(),
      ChunkAssignmentModel.find().lean().exec(),
    ]);
    const [users, walletTransactions, stripeSessions] = await Promise.all([
      UserModel.find().lean().exec(),
      WalletTransactionModel.find().lean().exec(),
      StripeSessionModel.find().lean().exec(),
    ]);
    const hasRemoteData = tasks.length > 0 || chunkResults.length > 0 || chunkAssignments.length > 0;
    if (hasRemoteData) {
      db.data.tasks = tasks.map(normalizeTaskRecord);
      db.data.chunkResults = chunkResults.map(normalizeGenericRecord);
      db.data.chunkAssignments = chunkAssignments.map(normalizeGenericRecord);
      db.data.users = users.map(normalizeUserRecord);
      db.data.walletTransactions = walletTransactions.map(normalizeGenericRecord);
      db.data.stripeSessions = stripeSessions.map(normalizeGenericRecord);
      db.write();
    } else {
      await syncToMongo();
    }
  } catch (error) {
    console.error('Failed to hydrate LowDB from MongoDB:', error?.message || error);
  }
}

export function getDb() {
  return db;
}

export async function saveDb() {
  db.write();
  await syncToMongo();
}

export function isMongoReady() {
  return mongoReady;
}

export function getMongoModels() {
  return {
    TaskModel,
    ChunkResultModel,
    ChunkAssignmentModel,
    UserModel,
    WalletTransactionModel,
    StripeSessionModel,
  };
}
