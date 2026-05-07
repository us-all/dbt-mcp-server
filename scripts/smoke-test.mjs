#!/usr/bin/env node
// Lightweight smoke-test against a real env. Spawns the built server, runs
// initialize + tools/list, optionally calls a couple of read-only tools when
// the relevant env vars are set, then exits.

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const serverPath = join(here, "..", "dist", "index.js");

if (!process.env.DBT_PROJECT_DIR) {
  console.error("DBT_PROJECT_DIR is required for the smoke test");
  process.exit(1);
}

const proc = spawn(process.execPath, [serverPath], {
  stdio: ["pipe", "pipe", "inherit"],
  env: process.env,
});

let buffer = "";
const pending = new Map();

proc.stdout.on("data", (chunk) => {
  buffer += chunk.toString();
  let nl;
  while ((nl = buffer.indexOf("\n")) !== -1) {
    const line = buffer.slice(0, nl).trim();
    buffer = buffer.slice(nl + 1);
    if (!line) continue;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      continue;
    }
    const cb = pending.get(msg.id);
    if (cb) {
      pending.delete(msg.id);
      cb(msg);
    }
  }
});

let nextId = 1;
function send(method, params) {
  const id = nextId++;
  const payload = JSON.stringify({ jsonrpc: "2.0", id, method, params });
  return new Promise((resolve, reject) => {
    pending.set(id, (msg) => (msg.error ? reject(new Error(JSON.stringify(msg.error))) : resolve(msg.result)));
    proc.stdin.write(payload + "\n");
  });
}

function notify(method, params) {
  proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");
}

async function main() {
  await send("initialize", {
    protocolVersion: "2024-11-05",
    capabilities: {},
    clientInfo: { name: "smoke-test", version: "0.0.0" },
  });
  notify("notifications/initialized", {});

  const tools = await send("tools/list", {});
  console.log(`tools/list returned ${tools.tools.length} tools`);
  const expected = 32; // 15 dbt + 5 quality + 6 airflow + 4 agg (in quality) + 1 search + 1 = 32
  if (tools.tools.length < 25) {
    console.error("Smoke test failed: too few tools registered");
    process.exit(1);
  }

  // Always-safe call: dbt-list-models.
  try {
    const r = await send("tools/call", {
      name: "dbt-list-models",
      arguments: { limit: 5 },
    });
    const text = r.content?.[0]?.text ?? "";
    console.log("dbt-list-models OK:", text.slice(0, 120) + "...");
  } catch (err) {
    console.error("dbt-list-models failed:", err.message);
  }

  if (process.env.DQ_RESULTS_TABLE) {
    try {
      const r = await send("tools/call", {
        name: "dq-list-checks",
        arguments: { sinceHours: 24, limit: 5 },
      });
      console.log("dq-list-checks OK:", r.content?.[0]?.text?.slice(0, 120) + "...");
    } catch (err) {
      console.warn("dq-list-checks failed (acceptable if backend not ready):", err.message);
    }
  }

  if (process.env.AIRFLOW_API_URL) {
    try {
      const r = await send("tools/call", {
        name: "airflow-list-dags",
        arguments: { onlyActive: true, limit: 5 },
      });
      console.log("airflow-list-dags OK:", r.content?.[0]?.text?.slice(0, 120) + "...");
    } catch (err) {
      console.warn("airflow-list-dags failed (acceptable if no tunnel):", err.message);
    }
  }

  proc.kill();
  process.exit(0);
}

main().catch((err) => {
  console.error("Smoke test failed:", err);
  proc.kill();
  process.exit(1);
});
