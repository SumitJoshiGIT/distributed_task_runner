#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

const chunkPath = path.join(__dirname, 'chunk.json');

function logBootstrap() {
  const scriptName = path.basename(__filename);
  console.log(`[${scriptName}] Support triage worker online. Waiting for chunk assignments...`);
}

function loadTicket() {
  try {
    const raw = fs.readFileSync(chunkPath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    console.error(`Unable to read chunk payload: ${error.message}`);
    process.exit(1);
  }
}

function scorePriority(priority = '') {
  const normalized = String(priority).toLowerCase();
  switch (normalized) {
    case 'urgent':
      return 4;
    case 'high':
      return 3;
    case 'medium':
      return 2;
    case 'low':
      return 1;
    default:
      return 1;
  }
}

function extractSignals(ticket, text) {
  const signals = [];
  const entries = [
    { pattern: /(outage|down|failure|unavailable)/, label: 'service-disruption', weight: 3 },
    { pattern: /(security|cve|vulnerab|breach|hipaa|mfa)/, label: 'security', weight: 4 },
    { pattern: /(payment|invoice|billing|refund)/, label: 'billing', weight: 2 },
    { pattern: /(performance|slow|latency|timeout)/, label: 'performance', weight: 2 },
    { pattern: /(export|report|dashboard)/, label: 'analytics', weight: 1 },
    { pattern: /(webhook|integration|api|rate)/, label: 'integration', weight: 2 },
    { pattern: /(feature|request|roadmap)/, label: 'feature-request', weight: 1 }
  ];
  entries.forEach((entry) => {
    if (entry.pattern.test(text)) {
      signals.push({ label: entry.label, weight: entry.weight });
    }
  });
  if (Array.isArray(ticket.tags)) {
    ticket.tags.forEach((tag) => {
      const normalized = String(tag).toLowerCase();
      if (normalized === 'security') signals.push({ label: 'security-tag', weight: 3 });
      if (normalized === 'outage') signals.push({ label: 'service-disruption-tag', weight: 3 });
      if (normalized === 'billing') signals.push({ label: 'billing-tag', weight: 2 });
    });
  }
  return signals;
}

function determineCategory(signals) {
  if (signals.some((s) => s.label.includes('security'))) return 'security';
  if (signals.some((s) => s.label.includes('service-disruption'))) return 'incident-response';
  if (signals.some((s) => s.label.includes('billing'))) return 'billing';
  if (signals.some((s) => s.label.includes('integration'))) return 'integrations';
  if (signals.some((s) => s.label.includes('analytics'))) return 'analytics';
  return 'general-support';
}

function recommendTeam(category) {
  switch (category) {
    case 'security':
      return 'Security Response';
    case 'incident-response':
      return 'SRE On-Call';
    case 'billing':
      return 'Billing Specialists';
    case 'integrations':
      return 'Platform Integrations';
    case 'analytics':
      return 'Analytics Success Team';
    default:
      return 'Customer Success';
  }
}

function computeUrgency(ticket, signals) {
  let score = scorePriority(ticket.priority);
  signals.forEach((signal) => {
    score += signal.weight;
  });
  if (String(ticket.channel).toLowerCase() === 'phone') score += 1;
  if (String(ticket.channel).toLowerCase() === 'slack') score += 0.5;
  return Math.min(10, Math.round(score * 10) / 10);
}

function buildSummary(ticket, category, team, urgency, signals) {
  const signalLabels = signals.map((s) => s.label).join(', ') || 'no special signals';
  return [
    `Ticket ${ticket.ticketId} (${ticket.priority})`,
    `Category: ${category}`,
    `Route to: ${team}`,
    `Urgency score: ${urgency}/10`,
    `Signals: ${signalLabels}`
  ].join(' | ');
}

if (!fs.existsSync(chunkPath)) {
  logBootstrap();
  process.exit(0);
}

const ticket = loadTicket();
const text = `${ticket.subject || ''} ${ticket.description || ''}`.toLowerCase();
const signals = extractSignals(ticket, text);
const category = determineCategory(signals);
const team = recommendTeam(category);
const urgency = computeUrgency(ticket, signals);
const summary = buildSummary(ticket, category, team, urgency, signals);

try {
  const outputPath = path.join(__dirname, 'analysis.json');
  const payload = {
    ticketId: ticket.ticketId,
    category,
    recommendedTeam: team,
    urgency,
    signals,
    generatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
} catch (error) {
  console.error(`Failed to write analysis: ${error.message}`);
}

console.log(summary);
