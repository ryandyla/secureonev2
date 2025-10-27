// server.js
import express from "express";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import path from "node:path";

// Import your Cloudflare Worker module.
// It should export either `default.fetch` or `fetch`.
import workerModule from "./_worker.js";

const app = express();

// Ensure we can read raw bodies when needed
app.use(express.raw({ type: "*/*", limit: "10mb" }));

// Helper: Express req → WHATWG Request
function toWebRequest(req) {
  const proto = req.headers["x-forwarded-proto"] || "http";
  const host = req.headers["host"];
  const url = `${proto}://${host}${req.originalUrl}`;

  // Body: use Buffer if present; otherwise null (GET/HEAD)
  const method = req.method || "GET";
  const hasBody = !["GET", "HEAD"].includes(method);
  const body = hasBody ? req.body : undefined;

  // Headers: from Express to Web
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (Array.isArray(v)) {
      for (const vv of v) headers.append(k, vv);
    } else if (v != null) {
      headers.set(k, String(v));
    }
  }

  // Construct Request
  return new Request(url, {
    method,
    headers,
    body: hasBody && body && body.length ? body : undefined,
    // Node 18+ global fetch/request is fine
    // duplex not required unless streaming uploads
  });
}

// Minimal ctx shim for CF Worker
const ctx = {
  waitUntil: (p) => {
    // Let the promise run but don't block the response
    Promise.resolve(p).catch((e) => console.error("waitUntil error:", e));
  },
  passThroughOnException: () => {}
};

// Build env from process.env.
// If your Worker used `env.MY_SECRET`, define it in Cloud Run as an env var.
const env = {
  ...process.env
};

// Main handler: delegate everything to Worker’s fetch
app.all("*", async (req, res) => {
  try {
    const webReq = toWebRequest(req);

    // Support both `export default { fetch }` and `export async function fetch`
    const fetchFn =
      (workerModule && workerModule.fetch) ||
      (workerModule && workerModule.default && workerModule.default.fetch);

    if (typeof fetchFn !== "function") {
      res.status(500).send("Worker fetch handler not found.");
      return;
    }

    const webRes = await fetchFn(webReq, env, ctx);

    // Copy status & headers back to Express
    res.status(webRes.status);
    webRes.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });

    // Stream or buffer the body back
    if (webRes.body) {
      // Node >=18 supports web streams on Response
      const reader = webRes.body.getReader();
      res.on("close", () => reader.cancel().catch(() => {}));
      const pump = async () => {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          res.write(Buffer.from(value));
        }
        res.end();
      };
      pump().catch((e) => {
        console.error("Streaming error:", e);
        if (!res.headersSent) res.status(500);
        res.end();
      });
    } else {
      res.end();
    }
  } catch (err) {
    console.error(err);
    res.status(500).send("Internal error");
  }
});

// Cloud Run provides PORT env var
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`SecureOne adapter listening on ${PORT}`);
});
