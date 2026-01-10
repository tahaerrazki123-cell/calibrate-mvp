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
