#!/home/kali/.bun/bin/bun

import { Database } from 'bun:sqlite';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { basename, join } from 'node:path';
import { homedir } from 'node:os';
import { extractLastAssistantTurn } from '../src/shared/transcript-parser.ts';

type SessionRow = {
  id: number;
  content_session_id: string;
  platform_source: string;
  cwd: string | null;
  started_at: string;
};

const args = new Set(process.argv.slice(2));
const valueAfter = (flag: string, fallback: string): string => {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
};

const since = valueAfter('--since', '2026-08-28T00:00:00Z');
const limit = Number.parseInt(valueAfter('--limit', '0'), 10);
const concurrency = Math.max(1, Number.parseInt(valueAfter('--concurrency', '2'), 10));
const timeoutMs = Math.max(30_000, Number.parseInt(valueAfter('--timeout-ms', '180000'), 10));
const dryRun = args.has('--dry-run');
const sinceEpoch = Date.parse(since);

if (!Number.isFinite(sinceEpoch)) {
  throw new Error(`Invalid --since value: ${since}`);
}

const dataDir = process.env.CLAUDE_MEM_DATA_DIR || join(homedir(), '.claude-mem');
const dbPath = join(dataDir, 'claude-mem.db');
const projectsRoot = join(homedir(), '.claude', 'projects');
const workerUrl = process.env.CLAUDE_MEM_WORKER_URL || 'http://localhost:37700';

if (!existsSync(dbPath)) throw new Error(`Database not found: ${dbPath}`);
if (!existsSync(projectsRoot)) throw new Error(`Claude transcript directory not found: ${projectsRoot}`);

const db = new Database(dbPath);
const rows = db.query<SessionRow, [number]>(`
  SELECT s.id, s.content_session_id, s.platform_source, s.cwd, s.started_at
  FROM sdk_sessions s
  WHERE s.platform_source = 'claude'
    AND s.started_at_epoch >= ?
    AND NOT EXISTS (
      SELECT 1 FROM session_summaries ss
      WHERE ss.memory_session_id = s.memory_session_id
    )
  ORDER BY s.started_at_epoch ASC
`).all(sinceEpoch);

const candidates = limit > 0 ? rows.slice(0, limit) : rows;
const wanted = new Set(candidates.map((row) => `${row.content_session_id}.jsonl`));
const transcripts = new Map<string, string>();

function scan(directory: string): void {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const fullPath = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'subagents' || entry.name.includes('claude-mem-observer-sessions')) continue;
      scan(fullPath);
      continue;
    }
    if (!entry.isFile() || !wanted.has(entry.name)) continue;
    const sessionId = basename(entry.name, '.jsonl');
    const previous = transcripts.get(sessionId);
    if (!previous || statSync(fullPath).mtimeMs > statSync(previous).mtimeMs) {
      transcripts.set(sessionId, fullPath);
    }
  }
}

scan(projectsRoot);

const prepared = candidates.flatMap((row) => {
  const transcript = transcripts.get(row.content_session_id);
  if (!transcript) return [];
  const turn = extractLastAssistantTurn(transcript, true);
  if (!turn.text.trim()) return [];
  return [{ row, transcript, lastAssistantMessage: turn.text, observedModel: turn.model }];
});

console.log(JSON.stringify({
  since,
  candidates: candidates.length,
  recoverable: prepared.length,
  missingTranscriptOrAssistantTurn: candidates.length - prepared.length,
  concurrency,
  dryRun,
}));

if (dryRun) {
  db.close();
  process.exit(0);
}

const hasSummary = db.query<{ count: number }, [number]>(`
  SELECT COUNT(*) AS count
  FROM sdk_sessions s
  JOIN session_summaries ss ON ss.memory_session_id = s.memory_session_id
  WHERE s.id = ?
`);

async function waitForSummary(sessionDbId: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((hasSummary.get(sessionDbId)?.count ?? 0) > 0) return;
    await Bun.sleep(1_000);
  }
  throw new Error(`summary generation timed out for database session ${sessionDbId}`);
}

let cursor = 0;
let recovered = 0;
let failed = 0;

async function worker(): Promise<void> {
  while (true) {
    const index = cursor++;
    const item = prepared[index];
    if (!item) return;
    const response = await fetch(`${workerUrl}/api/sessions/summarize`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contentSessionId: item.row.content_session_id,
        last_assistant_message: item.lastAssistantMessage,
        platformSource: item.row.platform_source,
        cwd: item.row.cwd || undefined,
        observedModel: item.observedModel,
      }),
    });
    const result = await response.json() as { status?: string; reason?: string };
    if (!response.ok || result.status !== 'queued') {
      failed += 1;
      console.error(`[${index + 1}/${prepared.length}] skipped db=${item.row.id}: ${result.reason || result.status || response.status}`);
      continue;
    }
    try {
      await waitForSummary(item.row.id);
      recovered += 1;
      console.log(`[${index + 1}/${prepared.length}] recovered db=${item.row.id} (${recovered} complete, ${failed} failed)`);
    } catch (error) {
      failed += 1;
      console.error(`[${index + 1}/${prepared.length}] failed db=${item.row.id}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

await Promise.all(Array.from({ length: Math.min(concurrency, prepared.length) }, () => worker()));
console.log(JSON.stringify({ recovered, failed, attempted: prepared.length }));
db.close();
process.exitCode = failed === 0 ? 0 : 1;
