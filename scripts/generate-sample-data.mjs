#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');

const OUTPUT_PATH = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(PROJECT_ROOT, 'samples', 'data.json');
const RECORD_COUNT = Number.isFinite(Number(process.env.SAMPLE_COUNT))
  ? Math.max(1, Number(process.env.SAMPLE_COUNT))
  : 3000;

const customers = [
  'Orbit Ventures',
  'Blue Meadow Foods',
  'Vector Labs',
  'Radiant Retail',
  'Summit Analytics',
  'Northwind Logistics',
  'Silverline Finance',
  'Brightside Education',
  'Granite Manufacturing',
  'Cloud Harbor',
  'Lumen Media',
  'Terra Healthcare',
  'Apex Dynamics',
  'Cobalt Energy',
  'Nova Travel'
];

const priorities = ['Low', 'Medium', 'High', 'Urgent'];
const channels = ['Email', 'Chat', 'Phone', 'Slack', 'Webhook'];
const subjects = [
  'Payment failure for invoice',
  'Slow data export job',
  'Production outage in region',
  'Feature request: export weekly summary',
  'Incorrect timezone on reports',
  'Webhook retries exhausted',
  'Critical vulnerability disclosed',
  'Onboarding checklist',
  'API throttle warnings',
  'Multi-factor auth failing for admins',
  'Brand assets update',
  'Audit assistance request'
];

const descriptions = [
  'Finance reports recurring failures when attempting to pay invoice through the customer portal.',
  'Operations noticed scheduled export tasks taking longer than usual and needs confirmation.',
  'Customers in the specified region cannot authenticate and are experiencing an outage.',
  'Customer is requesting a new dashboard export feature to automate weekly summaries.',
  'Dashboards display an incorrect timezone for the tenant configuration.',
  'Webhook delivery stopped retrying even though the endpoint responded with 503.',
  'Security disclosed a new CVE affecting dependencies and needs mitigation guidance.',
  'Customer success needs an onboarding checklist for their new team.',
  'Nightly batch exceeded API rate limits during processing and needs a higher burst capacity.',
  'MFA push notifications are failing with invalid token errors.',
  'Marketing updated assets and needs instructions for rolling them out.',
  'Compliance team needs access logs exported ASAP.'
];

const tagsPool = [
  ['billing', 'payment', 'portal'],
  ['performance', 'export'],
  ['outage', 'auth'],
  ['feature', 'dashboard'],
  ['bug', 'reports', 'timezone'],
  ['webhooks', 'reliability'],
  ['security', 'cve'],
  ['documentation', 'onboarding'],
  ['api', 'rate-limit'],
  ['security', 'mfa'],
  ['branding', 'support'],
  ['compliance', 'audit', 'logs']
];

function pick(list, index) {
  return list[index % list.length];
}

const baseTimestamp = Date.parse('2025-03-15T09:20:00Z');
const hourMs = 60 * 60 * 1000;

const data = Array.from({ length: RECORD_COUNT }, (_, idx) => {
  const ticketNumber = 1001 + idx;
  const customer = pick(customers, idx);
  const priority = pick(priorities, idx);
  const channel = pick(channels, idx);
  const subjectBase = pick(subjects, idx);
  const description = pick(descriptions, idx);
  const tags = pick(tagsPool, idx);
  const invoiceId = 4000 + (idx % 500);
  const region = ['US', 'EU', 'APAC'][idx % 3];
  const createdAt = new Date(baseTimestamp + idx * hourMs).toISOString();

  return {
    ticketId: `SUP-${ticketNumber.toString().padStart(4, '0')}`,
    customer,
    priority,
    channel,
    subject: `${subjectBase} ${invoiceId}`,
    description,
    tags,
    createdAt
  };
});

fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
fs.writeFileSync(OUTPUT_PATH, JSON.stringify(data, null, 2));
console.log(`Wrote ${data.length} tickets to ${OUTPUT_PATH}`);
