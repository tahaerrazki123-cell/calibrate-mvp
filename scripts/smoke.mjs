import { spawn } from "child_process";
import { createServer } from "http";
import { randomUUID } from "crypto";

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on("error", reject);
    server.listen(0, () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForHealth(url, timeoutMs = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(url);
      const json = await res.json().catch(() => ({}));
      if (res.ok && json.ok) return;
    } catch {}
    await sleep(150);
  }
  throw new Error("Health check failed");
}

function buildMultipart(boundary, fields, fileField, filename, fileBuf) {
  const parts = [];
  for (const [key, value] of Object.entries(fields)) {
    parts.push(
      Buffer.from(
        `--${boundary}\r\n` +
          `Content-Disposition: form-data; name="${key}"\r\n\r\n` +
          `${value}\r\n`
      )
    );
  }
  parts.push(
    Buffer.from(
      `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="${fileField}"; filename="${filename}"\r\n` +
        `Content-Type: application/octet-stream\r\n\r\n`
    )
  );
  parts.push(fileBuf);
  parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
  return Buffer.concat(parts);
}

async function run() {
  const port = await getFreePort();
  const env = {
    ...process.env,
    NODE_ENV: "test",
    SMOKE_BYPASS_USER_ID: "00000000-0000-0000-0000-000000000001",
    SMOKE_BYPASS_TOKEN: "smoke-token",
    MAX_UPLOAD_BYTES: "1024",
    PORT: String(port),
  };

  const child = spawn(process.execPath, ["web/server.mjs"], {
    env,
    stdio: "inherit",
  });

  let failed = false;
  try {
    await waitForHealth(`http://127.0.0.1:${port}/api/health`);
    const notFoundId = randomUUID();
    const res1 = await fetch(`http://127.0.0.1:${port}/api/runs/${notFoundId}`, {
      headers: { "x-smoke-bypass": "smoke-token" },
    });
    const json1 = await res1.json().catch(() => ({}));
    if (!(res1.status === 404 && json1.code === "NOT_FOUND")) {
      throw new Error(`Expected 404 NOT_FOUND, got ${res1.status} ${JSON.stringify(json1)}`);
    }
    console.log("PASS open-run 404");

    const boundary = `----smoke${Date.now()}`;
    const body = buildMultipart(
      boundary,
      { scenario: "smoke", context: "smoke" },
      "file",
      "oversize.bin",
      Buffer.alloc(2048, 1)
    );
    const res2 = await fetch(`http://127.0.0.1:${port}/api/run`, {
      method: "POST",
      headers: {
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "x-smoke-bypass": "smoke-token",
      },
      body,
    });
    const json2 = await res2.json().catch(() => ({}));
    if (!(res2.status === 413 && json2.code === "FILE_TOO_LARGE" && json2.maxBytes === 1024)) {
      throw new Error(`Expected 413 FILE_TOO_LARGE, got ${res2.status} ${JSON.stringify(json2)}`);
    }
    console.log("PASS upload size limit");

    const boundary2 = `----smoke${Date.now()}b`;
    const body2 = buildMultipart(
      boundary2,
      { scenario: "smoke", context: "smoke" },
      "file",
      "tiny.bin",
      Buffer.alloc(8, 1)
    );
    const res3 = await fetch(`http://127.0.0.1:${port}/api/run_async`, {
      method: "POST",
      headers: {
        "Content-Type": `multipart/form-data; boundary=${boundary2}`,
        "x-smoke-bypass": "smoke-token",
      },
      body: body2,
    });
    const json3 = await res3.json().catch(() => ({}));
    if (!(res3.status === 202 && json3.run_id)) {
      throw new Error(`Expected 202 run_async, got ${res3.status} ${JSON.stringify(json3)}`);
    }
    const runId = json3.run_id;
    const res4 = await fetch(`http://127.0.0.1:${port}/api/runs`, {
      headers: { "x-smoke-bypass": "smoke-token" },
    });
    const json4 = await res4.json().catch(() => ({}));
    const runRow = (json4.runs || []).find((r) => r.id === runId);
    if (!runRow?.entity_id) {
      throw new Error(`Expected run entity_id set, got ${JSON.stringify(runRow)}`);
    }
    console.log("PASS run auto-entity");

    const res5 = await fetch(`http://127.0.0.1:${port}/api/entities`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-smoke-bypass": "smoke-token",
      },
      body: JSON.stringify({ name: "Smoke Entity" }),
    });
    const json5 = await res5.json().catch(() => ({}));
    if (!json5?.entity?.id) {
      throw new Error(`Expected entity created, got ${res5.status} ${JSON.stringify(json5)}`);
    }
    const res6 = await fetch(`http://127.0.0.1:${port}/api/runs/${runId}/reassign_entity`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-smoke-bypass": "smoke-token",
      },
      body: JSON.stringify({ entity_id: json5.entity.id }),
    });
    const json6 = await res6.json().catch(() => ({}));
    if (!(res6.status === 200 && json6?.run?.entity_id === json5.entity.id)) {
      throw new Error(`Expected reassign_entity ok, got ${res6.status} ${JSON.stringify(json6)}`);
    }
    console.log("PASS reassign_entity");
  } catch (err) {
    failed = true;
    console.error("FAIL", err?.message || err);
  } finally {
    child.kill();
    await sleep(250);
    if (!child.killed) {
      child.kill("SIGKILL");
    }
  }

  if (failed) process.exit(1);
}

run();
