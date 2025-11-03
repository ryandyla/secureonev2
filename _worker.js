// Cloudflare Worker: ZVA <-> WinTeam / Monday bridge (PASTE-READY)
// Endpoints:
//  - POST /auth/employee
//  - POST /winteam/shifts
//  - POST /monday/write
//  - POST /zva/shift-write
//  - POST /zva/shift-write-by-cell
//
// Required secrets: WINTEAM_TENANT_ID, WINTEAM_API_KEY, MONDAY_API_KEY
// Optional secrets: MONDAY_BOARD_ID, MONDAY_DEFAULT_BOARD_ID, FLOW_GUARD_TTL_SECONDS
// Optional KV binding: FLOW_GUARD
//
// wrangler.toml
// name = "secureonev2"
// main = "_worker.js"
// compatibility_date = "2025-10-18"

///////////////////////////////
// Constants
///////////////////////////////
const EMPLOYEE_BASE = "http://apim.myteamsoftware.com/wtnextgen/employees/v1/api/employees";
const SHIFTS_BASE_EXACT = "http://apim.myteamsoftware.com/wtnextgen/schedules/v1/api/shiftDetails";
const MONDAY_API_URL = "https://api.monday.com/v2";

const MONDAY_COLUMN_MAP = {
  division: "color_mktd81zp",     // "Division:" (Status)
  department: "color_mktsk31h",   // "Department" (Status)
  site: "text_mktj4gmt",          // "Account/Site:"
  email: "email_mktdyt3z",        // "Email Address:"
  phone: "phone_mktdphra",        // "Phone Number:"
  callerId: "phone_mkv0p9q3",     // "Caller ID:"
  reason: "text_mktdb8pg",        // "Call Issue/Reason:"
  timeInOut: "text_mktsvsns",     // "Time In/Out:"
  startTime: "text_mkv0t29z",     // "Start Time (if applicable):"
  endTime: "text_mkv0nmq1",       // "End Time (if applicable)"
  dateTime: "date4",              // "Date/Time:"
  deptEmail: "text_mkv07gad",     // "Department Email:"
  emailStatus: "color_mkv0cpxc",
  itemIdEcho: "pulse_id_mkv6rhgy",
  zoomGuid: "text_mkv7j2fq",      // "Zoom Call GUID"
  shift: "text_mkwn6bzw"
};

///////////////////////////////
// Tiny utils
///////////////////////////////
const json = (obj, { status = 200, cors = true } = {}) =>
  new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(cors ? {
        "access-control-allow-origin": "*",
        "access-control-allow-headers": "content-type, authorization",
        "access-control-allow-methods": "GET,POST,OPTIONS"
      } : {})
    }
  });

const toStr = (v) => (v == null ? "" : String(v));
const trim = (v) => toStr(v).trim();
const pad2 = (n) => String(n).padStart(2, "0");

function ymd(date) {
  const y = date.getUTCFullYear();
  const m = pad2(date.getUTCMonth() + 1);
  const d = pad2(date.getUTCDate());
  return `${y}-${m}-${d}`;
}
function addDaysUTC(date, days) {
  const d = new Date(date.getTime());
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}
function fmtYmdHmUTC(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return "";
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth()+1)}-${pad2(d.getUTCDate())} ${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
}
function normalizeUsPhone(raw) {
  if (!raw) return "";
  const s = String(raw);
  const d = s.replace(/\D+/g, "");
  if (d.length === 11 && d[0] === "1") return "+" + d;
  if (d.length === 10) return "+1" + d;
  if (/^\+\d{8,15}$/.test(s.trim())) return s.trim();
  return s.trim();
}
function parseNaiveAsLocalWall(isoNoTZ) {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(String(isoNoTZ));
  if (!m) return null;
  const [ , Y, M, D, h, min, s ] = m;
  return new Date(Date.UTC(+Y, +M - 1, +D, +h, +min, +(s || 0)));
}
function nowAnchor() { return new Date(); }

function ymdFromISODate(isoOrYmd) {
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(isoOrYmd))) return String(isoOrYmd);
  const d = new Date(String(isoOrYmd));
  return isNaN(d) ? "" : `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,"0")}-${String(d.getUTCDate()).padStart(2,"0")}`;
}
function ymdAdd(ymdStr, days) {
  const [y,m,d] = ymdStr.split("-").map(n=>+n);
  const t = Date.UTC(y, m-1, d) + days*24*60*60*1000;
  const D = new Date(t);
  return `${D.getUTCFullYear()}-${String(D.getUTCMonth()+1).padStart(2,"0")}-${String(D.getUTCDate()).padStart(2,"0")}`;
}
function windowExactDay(ymdStr) {
  const from = ymdStr;
  const to   = ymdAdd(ymdStr, 1); // next day
  return { from, to };
}
function windowAround(ymdStr, beforeDays = 10, afterDays = 10) {
  const from = ymdAdd(ymdStr, -Math.abs(beforeDays));
  const to   = ymdAdd(ymdStr,  Math.abs(afterDays));
  return { from, to };
}

// --- resignation helpers ---
const QUIT_WORDS = [
  "quit","resign","resignation","two weeks","2 weeks","notice",
  "separation","terminate","termination","leaving","last day",
  "effective immediately"
];
function isResignationish(s) {
  if (!s) return false;
  const t = String(s).toLowerCase();
  return QUIT_WORDS.some(w => t.includes(w));
}

function normalizeDepartmentLabel(v = "") {
  const t = String(v).trim().toLowerCase();
  if (!t) return "";
  if (["hr","human resources","human-resources"].includes(t)) return "Human Resources";
  if (["ops","operations"].includes(t)) return "Operations";
  if (["payroll"].includes(t)) return "Payroll";
  if (["training","train"].includes(t)) return "Training";
  if (["sales"].includes(t)) return "Sales";
  if (["fingerprint","fingerprinting"].includes(t)) return "Fingerprint";
  if (["other"].includes(t)) return "Other";
  return ""; // unknown -> skip setting to avoid Monday error
}

async function mondayFindItemByText(env, { boardId, columnId, value }) {
  if (!value) return null;
  const q = `
    query($boardId:[ID!], $columnId:String!, $value:String!) {
      items_page(query_params:{ rules:[
        { column_id:$columnId, operator:contains_text, compare_value:$value }
      ], board_ids:$boardId }, limit:1) {
        items { id name }
      }
    }`;
  const d = await mondayGraphQL(env, q, { boardId:[String(boardId)], columnId, value });
  return d?.items_page?.items?.[0] || null;
}

///////////////////////////////
// Logging (PII-safe)
///////////////////////////////
const MAX_LOG_BODY = 2048;
function headerObj(headers) {
  const safe = ["content-type","user-agent","cf-ray","x-forwarded-for","x-real-ip","accept","accept-encoding","accept-language","host","origin","referer"];
  const out = {};
  for (const [k,v] of headers.entries()) {
    if (/authorization|api|key|token|cookie|set-cookie|ocp-apim/i.test(k)) continue;
    if (safe.includes(k.toLowerCase())) out[k] = v;
  }
  return out;
}
function maskPII(str) {
  if (!str) return str;
  let s = String(str);
  s = s.replace(/([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})/g, "$1***@$2");
  s = s.replace(/\+?\d[\d\s().-]{6,}\d/g, (m) => m.slice(0, Math.max(0, m.length - 2)).replace(/\d/g, "x") + m.slice(-2));
  s = s.replace(/("ssnLast4"\s*:\s*")(\d{0,4})(")/gi, (_, a, b, c) => a + (b ? "***" + b.slice(-1) : "***") + c);
  return s;
}

function summarizePayload(payload, { maxArray = 5, maxString = 400 } = {}) {
  // Returns a JSON-serializable copy with large arrays/strings truncated for readability
  const seen = new WeakSet();

  function walk(v) {
    if (v == null) return v;
    if (typeof v === "string") {
      const s = maskPII(v);
      return s.length > maxString ? s.slice(0, maxString) + "…(+)" : s;
    }
    if (typeof v !== "object") return v;

    if (seen.has(v)) return "[Circular]";
    seen.add(v);

    if (Array.isArray(v)) {
      const out = v.slice(0, maxArray).map(walk);
      if (v.length > maxArray) out.push(`…and ${v.length - maxArray} more`);
      return out;
    }

    const out = {};
    for (const k of Object.keys(v)) {
      out[k] = walk(v[k]);
    }
    return out;
  }

  return walk(payload);
}

async function cloneBodyPreview(reqOrResp) {
  // Try to parse JSON and return an OBJECT (not a string)
  try {
    const ct = reqOrResp.headers?.get?.("content-type") || "";
    if (/json/i.test(ct)) {
      const data = await reqOrResp.clone().json();
      return data;
    }
  } catch {}

  // Fallback to plain text for non-JSON or parse errors
  try {
    const text = await reqOrResp.clone().text();
    return maskPII(text).slice(0, MAX_LOG_BODY);
  } catch {
    return "";
  }
}

function shortId() { return Math.random().toString(36).slice(2, 8); }
function pretty(obj) { try { return JSON.stringify(obj, null, 2); } catch { return String(obj); } }
function compact(obj) { try { return JSON.stringify(obj); } catch { return String(obj); } }

function withLogging(handler) {
  return async (req, env, ctx) => {
    const start = Date.now();
    const url = new URL(req.url);
    const reqId = req.headers.get("cf-ray") || shortId();
    const wantPretty =
      (env && String(env.PRETTY_LOGS).toLowerCase() === "true") ||
      url.searchParams.get("pretty") === "1";

    const reqPreviewRaw = await cloneBodyPreview(req);
    const reqPreview = summarizePayload(reqPreviewRaw);

    let res;
    try {
      res = await handler(req, env, ctx);
    } catch (e) {
      res = json({ success:false, message:"Unhandled error.", detail:String(e && e.message || e) }, { status: 500 });
    }

    const ms = Date.now() - start;
    const resPreviewRaw = await cloneBodyPreview(res);
    const resPreview = summarizePayload(resPreviewRaw);

    const record = {
      at: new Date().toISOString(),
      reqId,
      method: req.method,
      path: url.pathname,
      query: Object.fromEntries(url.searchParams.entries()),
      headers: headerObj(req.headers),
      reqBody: reqPreview,
      status: res.status,
      resHeaders: headerObj(res.headers || new Headers()),
      resBody: resPreview,   // <— now an OBJECT, summarized
      ms
    };

    if (wantPretty) {
      console.log(`\n──── HTTP (${record.method} ${record.path}) #${reqId} ────\n${pretty(record)}\n`);
    } else {
      console.log("HTTP", compact(record));
    }
    return res;
  };
}

///////////////////////////////
// HTTP helpers
///////////////////////////////
async function readJson(req) {
  try {
    const j = await req.json();
    if (j && typeof j === "object" && typeof j.body === "string") { try { return JSON.parse(j.body); } catch { return j; } }
    return j;
  } catch {}
  try {
    const t = await req.text();
    if (!t) return null;
    const j = JSON.parse(t);
    if (j && typeof j === "object" && typeof j.body === "string") { try { return JSON.parse(j.body); } catch { return j; } }
    return j;
  } catch { return null; }
}
function buildWTHeaders(TENANT_ID, API_KEY) {
  return { tenantId: TENANT_ID, "Ocp-Apim-Subscription-Key": API_KEY, accept: "application/json" };
}

async function callWinTeamJSON(url, env) {
  const TENANT_ID = env.WINTEAM_TENANT_ID || "";
  const API_KEY = env.WINTEAM_API_KEY || "";
  if (!TENANT_ID || !API_KEY) return { ok:false, status:500, error:"Missing WINTEAM_TENANT_ID or WINTEAM_API_KEY." };

  try {
    const r = await fetch(url, { headers: buildWTHeaders(TENANT_ID, API_KEY) });

    const ct = (r.headers.get("content-type") || "").toLowerCase();
    const raw = await r.text(); // read once

    // Non-2xx → bubble up body (HTML/text) for debugging
    if (!r.ok) {
      return { ok:false, status:r.status, error: (raw || "").slice(0, 1000) || `HTTP ${r.status}` };
    }

    // 204 or empty → treat as empty page, not a JSON parse error
    if (!raw || r.status === 204) {
      return { ok:true, data: { data: [] } };
    }

    // Try JSON parse; if it fails, surface a helpful error
    try {
      const data = JSON.parse(raw);
      return { ok:true, data };
    } catch (e) {
      return {
        ok:false,
        status:r.status,
        error:`Non-JSON response (${r.status}) ct=${ct || "n/a"} body=${raw.slice(0, 240)}`
      };
    }
  } catch (e) {
    return { ok:false, status:502, error:String(e?.message || e) };
  }
}

function buildEmployeeURL(employeeNumber) {
  const url = new URL(EMPLOYEE_BASE);
  url.searchParams.set("searchFieldName", "employeeNumber");
  url.searchParams.set("searchText", String(employeeNumber));
  url.searchParams.set("exactMatch", "true");
  return url.toString();
}
async function mondayGraphQL(env, query, variables) {
  const token = env.MONDAY_API_KEY;
  if (!token) throw new Error("MONDAY_API_KEY not configured.");
  const r = await fetch(MONDAY_API_URL, {
    method: "POST",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables })
  });
  const j = await r.json();
  if (!r.ok || j.errors) throw new Error(`Monday API error: ${r.status} ${JSON.stringify(j.errors || j)}`);
  return j.data;
}
async function flowGuardSeen(env, key) {
  if (!key || !env.FLOW_GUARD) return false;
  const existing = await env.FLOW_GUARD.get(key);
  return Boolean(existing);
}
async function flowGuardMark(env, key) {
  if (!key || !env.FLOW_GUARD) return;
  const ttl = Number(env.FLOW_GUARD_TTL_SECONDS || 86400);
  await env.FLOW_GUARD.put(key, "1", { expirationTtl: ttl });
}

///////////////////////////////
// State & Department helpers
///////////////////////////////
const STATE_FULL = {
  AL:"Alabama", AK:"Alaska", AZ:"Arizona", AR:"Arkansas", CA:"California",
  CO:"Colorado", CT:"Connecticut", DE:"Delaware", FL:"Florida", GA:"Georgia",
  HI:"Hawaii", ID:"Idaho", IL:"Illinois", IN:"Indiana", IA:"Iowa",
  KS:"Kansas", KY:"Kentucky", LA:"Louisiana", ME:"Maine", MD:"Maryland",
  MA:"Massachusetts", MI:"Michigan", MN:"Minnesota", MS:"Mississippi", MO:"Missouri",
  MT:"Montana", NE:"Nebraska", NV:"Nevada", NH:"New Hampshire", NJ:"New Jersey",
  NM:"New Mexico", NY:"New York", NC:"North Carolina", ND:"North Dakota", OH:"Ohio",
  OK:"Oklahoma", OR:"Oregon", PA:"Pennsylvania", RI:"Rhode Island", SC:"South Carolina",
  SD:"South Dakota", TN:"Tennessee", TX:"Texas", UT:"Utah",
  VT:"Vermont", VA:"Virginia", WA:"Washington", WV:"West Virginia",
  WI:"Wisconsin", WY:"Wyoming", DC:"District of Columbia", PR:"Puerto Rico"
};
function stateFromSupervisor(supervisorDescription = "") {
  const m = String(supervisorDescription).trim().match(/^([A-Z]{2})\b/);
  if (!m) return "";
  return STATE_FULL[m[1]] || "";
}

// Expanded department mapping
// Expanded department mapping
function deriveDepartmentFromReason(reasonRaw = "") {
  const r = String(reasonRaw || "").trim();

  // // HR (resignation / quit)
  // if (/\b(quit|resign|resignation|two[-\s]?weeks|notice|separation|terminate|termination)\b/i.test(r)) {
  //   return "HR";
  // }

  // Payroll
  if (/\b(payroll|pay\s*(issue|check|stub)|w-?2|withhold|tax)\b/i.test(r)) return "Payroll";

  // Training
  if (/\b(training|train|course|lms|cert(ificate)?|guard\s*card)\b/i.test(r)) return "Training";

  // Operations (expanded)
  const opsRe = new RegExp([
    "\\b(call(?:ing)?\\s*off|callout|no\\s*show|coverage|cover|sick|pto|absence|absent)\\b",
    "\\b(late|arriv(?:e|ing)\\s*late|tardy|early\\s*out|leav(?:e|ing)\\s*early)\\b",
    "\\b(time\\s*card|timecard|punch|missed\\s*punch|schedule|swap|shift\\s*change)\\b",
    "\\b(transport|car\\s*(trouble|issue|problem|broke)|bus\\s*delay|uber|lyft|ride\\s*share|flat\\s*tire|traffic)\\b",
    "\\b(personal\\s*(issue|matter)|family\\s*(issue|emergency|matter)|child|kids?|doctor|dr\\.?|appointment|emergency)\\b",
    "\\b(quit|resign|resignation|two[-\s]?weeks|notice|separation|terminate|termination)\\b"
  ].join("|"), "i");
  if (opsRe.test(r)) return "Operations";

  return "Other";
}

// --- Supervisor/Dept → Division & Dept Email helpers ---

// Use in your pipeline like:
//   const division = deriveDivisionFromSupervisor(supervisorDescription, department);
//   const deptEmail = deriveDepartmentEmail(supervisorDescription, department);

function deriveDepartmentEmail(supervisorDescription = "", department = "") {
  const sup = String(supervisorDescription || "").trim();
  const dep = String(department || "").trim();

  // 1) Explicit department wins
  if (/^training$/i.test(dep))  return "training@secureone.com";
  if (/^payroll$/i.test(dep))   return "payroll@secureone.com";
  if (/^hr$/i.test(dep))        return "hr@secureone.com";
  if (/^corporate$/i.test(dep)) return "corporate@secureone.com";

  // 2) Corporate supervisors
  if (/\b(Chris\s+Jackson|Vince\s+Joyce|Jim\s+McGovern)\b/i.test(sup)) {
    return "corporate@secureone.com";
  }

  // 3) Ops state teams → {st}opsteam@secureone.com (e.g., ilopsteam@)
  const OPS_STATES = new Set(["AL","AZ","TX","TN","OH","IN","IL"]);
  // Match forms like "AZ OPS TEAM", "IL Operations", "... IL - Ops ..."
  const m = sup.match(/\b(AL|AZ|TX|TN|OH|IN|IL)\b.*\b(Ops|Operations)\b/i);
  if (m) {
    const st = m[1].toUpperCase();
    if (OPS_STATES.has(st)) return `${st.toLowerCase()}opsteam@secureone.com`;
  }

  // 4) Default: unknown
  return "";
}

function deriveDivisionFromSupervisor(supervisorDescription = "", department = "") {
  const sup = String(supervisorDescription || "").trim();
  const dep = String(department || "").trim();

  // 1) Explicit department "Corporate" wins
  if (/^corporate$/i.test(dep)) return "Corporate";

  // 2) Corporate supervisors
  if (/\b(Chris\s+Jackson|Vince\s+Joyce|Jim\s+McGovern)\b/i.test(sup)) {
    return "Corporate";
  }

  // 3) Ops state teams → Full state name + " Division"
  const STATE_NAME = {
    AL: "Alabama",
    AZ: "Arizona",
    TX: "Texas",
    TN: "Tennessee",
    OH: "Ohio",
    IN: "Indiana",
    IL: "Illinois",
  };
  const m = sup.match(/\b(AL|AZ|TX|TN|OH|IN|IL)\b.*\b(Ops|Operations)\b/i);
  if (m) {
    const st = m[1].toUpperCase();
    if (STATE_NAME[st]) return `${STATE_NAME[st]} Division`;
  }

  // 4) No inference
  return "";
}


///////////////////////////////
// Builders
///////////////////////////////
function mondayStatusLabel(label) {
  const v = String(label || "").trim();
  return v ? { label: v } : undefined;
}

function buildMondayColumnsFromFriendly(body) {
  // Accept aliases for time in/out
  if (body.ofctimeinorout && !body.timeInOut && !body.timeinorout) body.timeInOut = body.ofctimeinorout;
  if (body.timeinorout && !body.timeInOut) body.timeInOut = body.timeinorout;

  const out = {};
  const addIf = (friendlyKey, transform = (v) => v) => {
    if (body[friendlyKey] != null && body[friendlyKey] !== "") {
      const colId = MONDAY_COLUMN_MAP[friendlyKey];
      if (colId) out[colId] = transform(body[friendlyKey]);
    }
  };

  // text-ish
  addIf("site");
  addIf("reason");
  addIf("timeInOut");
  addIf("startTime");
  addIf("endTime");
  addIf("deptEmail");
  addIf("zoomGuid");
  addIf("shift");

  // Alias: ofcWorksite -> site column if 'site' wasn’t explicitly set
  if (!out[MONDAY_COLUMN_MAP.site] && body.ofcWorksite) {
    out[MONDAY_COLUMN_MAP.site] = String(body.ofcWorksite).trim();
  }

  if (body.itemIdEcho) out[MONDAY_COLUMN_MAP.itemIdEcho] = body.itemIdEcho;

  // email
  addIf("email", (v) => {
    const email = String(v).trim();
    if (!email || !/@/.test(email)) return undefined;
    return { email, text: email };
  });

  // phones
  const normPhone = (raw) => String(raw).replace(/[^\d+]/g, "").trim();
  addIf("phone",   (v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });
  addIf("callerId",(v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });

  // date/time
  if (body.dateTime) {
    const v = body.dateTime;
    if (typeof v === "object" && (v.date || v.time)) {
      out[MONDAY_COLUMN_MAP.dateTime] = v;
    } else {
      const ts = Date.parse(String(v));
      if (!isNaN(ts)) {
        const d = new Date(ts);
        out[MONDAY_COLUMN_MAP.dateTime] = {
          date: `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,"0")}-${String(d.getUTCDate()).padStart(2,"0")}`,
          time: `${String(d.getUTCHours()).padStart(2,"0")}:${String(d.getUTCMinutes()).padStart(2,"0")}:${String(d.getUTCSeconds()).padStart(2,"0")}`
        };
      }
    }
  }

  // ----- Division from explicit or derived state (prefer ofcWorkstate first) -----
  const explicitDivision = String(body.division || "").trim();

  let derivedDivision = "";
  if (!explicitDivision) {
    let rawState = String(
      body.ofcWorkstate || body.workState || body.state || body.ofcWorkState || body.ofcWork_state || ""
    ).trim();

    if (rawState) {
      if (rawState.length === 2 && STATE_FULL[rawState.toUpperCase()]) {
        derivedDivision = STATE_FULL[rawState.toUpperCase()];
      } else {
        derivedDivision = rawState;
      }
    }

    if (!derivedDivision) {
      derivedDivision = stateFromSupervisor(body.supervisorDescription);
    }
    if (!derivedDivision) {
      const rs = String(body.ofcWorkstate || body.workState || body.state || "").trim();
      if (rs.length === 2 && STATE_FULL[rs.toUpperCase()]) {
        derivedDivision = STATE_FULL[rs.toUpperCase()];
      } else if (rs) {
        derivedDivision = rs;
      }
    }
  }
  const finalDivision = explicitDivision || derivedDivision;
  if (finalDivision) out[MONDAY_COLUMN_MAP.division] = mondayStatusLabel(finalDivision);

  // ----- Department (existing behavior) -----
  const explicitDept = String(body.department || "").trim();
  const deptRaw = explicitDept || deriveDepartmentFromReason(body.callreason || body.reason || "");
  const dept = normalizeDepartmentLabel(deptRaw);
  if (dept) out[MONDAY_COLUMN_MAP.department] = mondayStatusLabel(dept);

  // cleanup
  for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k];
  return out;
}

///////////////////////////////
// WinTeam + Employee fetchers
///////////////////////////////
async function fetchEmployeeDetail(employeeNumber, env) {
  const url = buildEmployeeURL(employeeNumber);
  const wt = await callWinTeamJSON(url, env);
  if (!wt.ok) throw new Error(`WinTeam employees failed (${wt.status}) ${wt.error || ""}`);
  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];
  const e = results[0];
  if (!e) return null;
  return {
    fullName: [trim(e.firstName), trim(e.lastName)].filter(Boolean).join(" "),
    workstate: trim(e.state || e.workState || ""),
    supervisorDescription: trim(e.supervisorDescription || ""),
    emailAddress: trim(e.emailAddress || ""),
    phone1: normalizeUsPhone(e.phone1 || "")
  };
}

// Chunked (≤30d) cellId lookup; single-day when from/to provided
// Direct lookup within a provided window (kept < 30d). No chunking.
async function fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate, toDate }) {
  const want = String(cellId ?? "").trim();
  if (!employeeNumber || !want) return null;
  if (!fromDate || !toDate) return null;

  // clamp to 29 days max just in case
  const fromD = new Date(fromDate + "T00:00:00Z");
  const toD   = new Date(toDate   + "T00:00:00Z");
  const maxTo = new Date(fromD.getTime() + 29*24*60*60*1000);
  const toClamped = (toD > maxTo ? maxTo : toD);
  const fromY = ymd(fromD), toY = ymd(toClamped);

  const url = new URL(SHIFTS_BASE_EXACT);
  url.searchParams.set("employeeNumber", String(employeeNumber));
  url.searchParams.set("fromDate", fromY);
  url.searchParams.set("toDate", toY);

  const wt = await callWinTeamJSON(url.toString(), env);
  if (!wt.ok) {
    console.log("ZVA DEBUG writer: WT window failed", { fromYmd: fromY, toYmd: toY, status: wt.status, err: (wt.error || "").slice(0,160) });
    return null;
  }

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];

  for (const r of results) {
    const siteName = (r.jobDescription || "").toString().trim();
    const shifts = Array.isArray(r.shifts) ? r.shifts : [];
    for (const s of shifts) {
      const have = String(s.cellId ?? "").trim();
      if (have && have === want) {
        const startLocal = parseNaiveAsLocalWall(s.startTime);
        let   endLocal   = parseNaiveAsLocalWall(s.endTime);
        if (startLocal && endLocal && endLocal.getTime() <= startLocal.getTime()) {
          endLocal = new Date(endLocal.getTime() + 24*60*60*1000); // overnight
        }
        return {
          cellId: have,
          site: siteName,
          siteName,
          startLocalISO: startLocal ? startLocal.toISOString() : "",
          endLocalISO:   endLocal   ? endLocal.toISOString()   : ""
        };
      }
    }
  }
  return null;
}

// Fallback via our own /winteam/shifts (15d window) filtered by cellId
async function fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId, dateHint }) {
  const origin = new URL(req.url).origin;
  const want = String(cellId || "").trim();
  if (!want) return null;

  const body = { employeeNumber };
  if (dateHint) {
    try {
      const d = new Date(dateHint);
      if (!isNaN(d)) {
        const to  = new Date(d.getTime() + 24*60*60*1000);
        body.dateFrom = ymd(d);
        body.dateTo   = ymd(to);
      }
    } catch {}
  }

  let j;
  try {
    const r = await fetch(`${origin}/winteam/shifts`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });
    const text = await r.text();
    j = JSON.parse(text);
  } catch (e) {
    console.log("ZVA DEBUG viaSelf: fetch to /winteam/shifts threw", String(e?.message || e));
    return null;
  }

  const arr = Array.isArray(j?.entries) && j.entries.length ? j.entries : (Array.isArray(j?.entries_page) ? j.entries_page : []);
  const hit = arr.find(e => String(e?.cellId ?? "").trim() === want);
  if (!hit) return null;

  return {
    cellId: String(hit.cellId).trim(),
    site: String(hit.site || hit.siteName || "").trim(),
    siteName: String(hit.site || hit.siteName || "").trim(),
    startLocalISO: String(hit.startLocalISO || hit.startIso || "").trim(),
    endLocalISO:   String(hit.endLocalISO   || hit.endIso   || "").trim()
  };
}

///////////////////////////////
//
// Handlers
//
///////////////////////////////
async function handleAuthEmployee(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success:false, message:"Invalid JSON body." }, { status:400 });

  const employeeNumber = trim(body.employeeNumber);
  const ssnLast4 = trim(body.ssnLast4);
  if (!employeeNumber || !ssnLast4 || ssnLast4.length !== 4) {
    return json({ success:false, message:"Expect { employeeNumber, ssnLast4(4 digits) }." }, { status:400 });
  }

  const wt = await callWinTeamJSON(buildEmployeeURL(employeeNumber), env);
  if (!wt.ok) return json({ success:false, message:`WinTeam employees request failed (${wt.status}).`, detail: wt.error }, { status:502 });

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];
  const match = results[0] || null;
  if (!match) return json({ success:false, message:"No matching employee found." }, { status:404 });
  if (trim(match.partialSSN) !== ssnLast4) return json({ success:false, message:"SSN verification failed." }, { status:401 });

  const payload = {
    firstName: trim(match.firstName),
    lastName: trim(match.lastName),
    ofcFullname: [trim(match.firstName), trim(match.lastName)].filter(Boolean).join(" "),
    emailAddress: trim(match.emailAddress),
    phone1: normalizeUsPhone(match.phone1),
    supervisorDescription: trim(match.supervisorDescription),
    employeeNumber: match.employeeNumber,
    employeeId: match.employeeId,
    statusDescription: trim(match.statusDescription),
    typeDescription: trim(match.typeDescription)
  };
  return json({ success:true, message:"Successful employee lookup.", employee: payload });
}

async function handleShifts(req, env) {
  let body = await readJson(req);
  if (!body) body = {};
  if (body && typeof body === "object") {
    if (body.params && typeof body.params === "object") body = body.params;
    else if (body.json && typeof body.json === "object") body = body.json;
    else if (body.data && typeof body.data === "object") body = body.data;
  }

  const employeeNumber = trim(body.employeeNumber);
  const pageStart = Number(body.pageStart ?? 0) || 0;
  const reqDateFrom = trim(body.dateFrom || body.fromDate || "");
  const reqDateTo   = trim(body.dateTo   || body.toDate   || "");

  if (!employeeNumber) return json({ success:false, message:"employeeNumber is required." }, { status:400 });

  // ---- Choose window for WinTeam request (honor dateFrom/dateTo if provided) ----
  const startBase = nowAnchor();

  function validYmd(s) { return /^\d{4}-\d{2}-\d{2}$/.test(String(s || "")); }

  // Default: today ± 10 days (21-day window), always clamp to <30 days when custom range is provided.
  // ---- Choose window (honor dateFrom/dateTo if valid; else default ±10d) ----
  let winFrom, winTo;
  if (validYmd(reqDateFrom) && validYmd(reqDateTo)) {
    const fromD = new Date(reqDateFrom + "T00:00:00Z");
    const toD   = new Date(reqDateTo   + "T00:00:00Z");
    if (!isNaN(fromD) && !isNaN(toD)) {
      const maxTo = new Date(fromD.getTime() + 29*24*60*60*1000); // <30d cap
      const toClamped = (toD > maxTo ? maxTo : toD);
      winFrom = ymd(fromD);
      winTo   = ymd(toClamped);
    }
  }
  if (!winFrom || !winTo) {
    winFrom = ymd(addDaysUTC(startBase, -10));
    winTo   = ymd(addDaysUTC(startBase,  +10));
  }

  const wtUrl = new URL(SHIFTS_BASE_EXACT);
  wtUrl.searchParams.set("employeeNumber", employeeNumber);
  wtUrl.searchParams.set("fromDate", winFrom);
  wtUrl.searchParams.set("toDate", winTo);

  const wt = await callWinTeamJSON(wtUrl.toString(), env);
  if (!wt.ok) {
    return json({ success:false, message:`WinTeam shiftDetails failed (${wt.status}).`, detail: wt.error }, { status:502 });
  }

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];

  const rows = [];
  for (const r of results) {
    const site = trim(r.jobDescription);
    const role = trim(r.postDescription);
    const utcOffset = Number(r.utCoffset || 0);
    const list = Array.isArray(r.shifts) ? r.shifts : [];
    for (const s of list) {
      const startLocal = parseNaiveAsLocalWall(s.startTime);
      let   endLocal   = parseNaiveAsLocalWall(s.endTime);
      if (!startLocal || !endLocal) continue;
      if (endLocal.getTime() <= startLocal.getTime()) endLocal = new Date(endLocal.getTime() + 24*60*60*1000);

      const cellId = String(s.cellId ?? s.cellID ?? s.CellId ?? s.CellID ?? s.cell ?? "").trim();
      const scheduleDetailID = String(s.scheduleDetailID ?? s.ScheduleDetailID ?? s.scheduleDetailId ?? "").trim();

      rows.push({
        employeeNumber: trim(r.employeeNumber || employeeNumber),
        site, role, utcOffset,
        hours: s.hours,
        hourType: trim(s.hourType),
        hourDescription: trim(s.hourDescription),
        cellId,
        scheduleDetailID,
        id: cellId,
        startLocalISO: startLocal.toISOString(),
        endLocalISO:   endLocal.toISOString(),
        concise: `${ymd(startLocal)} ${pad2(startLocal.getUTCHours())}:${pad2(startLocal.getUTCMinutes())} → ${pad2(endLocal.getUTCHours())}:${pad2(endLocal.getUTCMinutes())}${site ? ` @ ${site}` : ""}${role ? ` (${role})` : ""}`,
        speakLine: `${["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][startLocal.getUTCDay()]}, ${["January","February","March","April","May","June","July","August","September","October","November","December"][startLocal.getUTCMonth()]} ${startLocal.getUTCDate()}, ${((h,m)=>{let hh=h%12; if(hh===0)hh=12; return `${hh}:${pad2(m)} ${h<12?"AM":"PM"}`;})(startLocal.getUTCHours(),startLocal.getUTCMinutes())} to ${["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][endLocal.getUTCDay()]} ${((h,m)=>{let hh=h%12; if(hh===0)hh=12; return `${hh}:${pad2(m)} ${h<12?"AM":"PM"}`;})(endLocal.getUTCHours(),endLocal.getUTCMinutes())}${site?` at ${site}`:""}${role?` (${role})`:""}`
      });
    }
  }

  // Sort & page (3)
  rows.sort((a,b) => Date.parse(a.startLocalISO) - Date.parse(b.startLocalISO));
  const countTotal = rows.length;
  const p = Math.max(0, pageStart|0);
  const pageRows = rows.slice(p, p+3);
  const nextPageStart = p + 3 < countTotal ? p + 3 : p;
  const hasNext = p + 3 < countTotal;

  return json({
    success:true,
    message:"Shifts lookup completed.",
    window:`${winFrom} → ${winTo}`,
    counts:{ rows: rows.length, filtered: rows.length },
    page:{ pageStart: p, nextPageStart, hasNext, pageCount: pageRows.length },
    speakable_page: pageRows.map(r => r.speakLine),
    entries_page: pageRows.map(({employeeNumber, site, role, startLocalISO, endLocalISO, hours, concise, hourType, hourDescription, cellId, scheduleDetailID, id}) => ({employeeNumber, site, role, startLocalISO, endLocalISO, hours, concise, hourType, hourDescription, cellId, scheduleDetailID, id})),
    speakable: rows.map(r => r.speakLine),
    entries: rows,
    raw: wt.data
  });
}

async function handleMondayWrite(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success:false, message:"Invalid JSON body." }, { status:400 });

  const modeRaw = String(body.mode || "").trim().toLowerCase();
  const mode = (modeRaw === "manual" ? "manual" : "auto");

  let engagementId = trim(body.engagementId || body.zoomEngId || body.zoomGuid || "");
  if (engagementId && !body.zoomGuid) body.zoomGuid = engagementId;

  const boardId = String(body.boardId || env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) return json({ success:false, message:"boardId is required (env or body)." }, { status:400 });

  const employeeNumber = trim(body.employeeNumber || body.ofcEmployeeNumber || "");
  const callreason     = trim(body.callreason || body.reason || "");
  const ofcPhone       = normalizeUsPhone(trim(body.ofcPhone || body.phone || ""));
  const ofcEmail       = trim(body.ofcEmail || body.email || "");
  const callerId       = normalizeUsPhone(trim(body.callerId || body.ANI || body.ani || ofcPhone || ""));
  const groupId        = trim(body.groupId || ""); // optional; same group used regardless

  // manual details & location
  const ofcFullname  = trim(body.ofcFullname || body.fullname || body.fullName || "");
  const ofcWorkstate = trim(body.ofcWorkstate || body.workState || body.state || "");
  const ofcWorksite  = trim(body.ofcWorksite  || body.site || "");

  // Manual validation
  if (mode === "manual") {
    const errs = [];
    if (!ofcFullname) errs.push("ofcFullname");
    if (!callreason)  errs.push("callreason");
    if (!ofcPhone && !ofcEmail && !callerId) errs.push("ofcPhone or ofcEmail");
    if (errs.length) return json({ success:false, message:"Missing required fields (manual mode).", missing: errs }, { status:400 });
  }

  // Item name policy (same board/group for both)
  let itemName = trim(body.itemName || body.name || "");
  if (!itemName) {
    if (mode === "manual") {
      itemName = `${ofcFullname || "Unknown"} (NO AUTH)`; // NO AUTH naming
    } else {
      itemName = `${employeeNumber || ofcFullname || "Unknown"} | Auto`;
    }
  }

  // Friendly → columns (no extra/manual-only columns)
  const cvFriendly = {
    ofcWorkstate,                // → Division via builder
    ofcWorksite,                 // → Account/Site via alias
    site: body.site,             // explicit site still honored
    reason: callreason,
    timeInOut: body.timeInOut || body.ofctimeinorout,
    startTime: body.startTime,
    endTime: body.endTime,
    zoomGuid: engagementId || "",
    email: ofcEmail,
    phone: ofcPhone,
    callerId,
    shift: body.shift || (mode === "manual" ? "Manual Intake – no shift provided" : undefined),

    division: body.division,
    department: body.department,
    deptEmail: body.deptEmail,
    dateTime: body.dateTime || new Date().toISOString()
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);

  // Dedupe key
  let dedupeKey = trim(body.dedupeKey || "");
  if (!dedupeKey) {
    if (engagementId && employeeNumber) dedupeKey = `${engagementId}:${employeeNumber}`;
    else if (engagementId) dedupeKey = engagementId;
    else if (callerId) dedupeKey = `manual:${callerId}:${new Date().toISOString().slice(0,13)}`;
  }

  if (dedupeKey && (await flowGuardSeen(env, dedupeKey))) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", mode, dedupeKey, upserted:false, item:null, columnValues });
  }

  // Optional UPSERT by Zoom GUID if present
  let targetItemId = null;
  if (engagementId && MONDAY_COLUMN_MAP.zoomGuid) {
    try {
      const hit = await mondayFindItemByText(env, { boardId, columnId: MONDAY_COLUMN_MAP.zoomGuid, value: engagementId });
      if (hit?.id) targetItemId = String(hit.id);
    } catch (e) {
      console.log("UPSERT lookup by zoomGuid failed:", String(e?.message || e));
    }
  }

  const cvString = JSON.stringify(columnValues);

  if (targetItemId) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) { id name }
      }`;
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: targetItemId, cv: cvString });
    } catch (e) {
      return json({ success:false, message:"Monday update (UPSERT) failed.", detail:String(e), itemId: targetItemId }, { status:502 });
    }
    if (dedupeKey) await flowGuardMark(env, dedupeKey);
    return json({ success:true, message:"Monday item updated (UPSERT).", mode, boardId, item: { id: targetItemId }, dedupeKey, engagementId, columnValuesSent: columnValues });
  }

  // CREATE (same group either way)
  const createMutation = `
    mutation ($boardId: ID!, $itemName: String!, $groupId: String) {
      create_item (board_id: $boardId, item_name: $itemName, group_id: $groupId) {
        id name board { id } group { id }
      }
    }`;
  let created;
  try {
    const res = await mondayGraphQL(env, createMutation, { boardId, itemName, groupId: groupId || null });
    created = res.create_item;
  } catch (e) {
    return json({ success:false, message:"Monday create_item failed.", detail:String(e) }, { status:502 });
  }

  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) { id name }
      }`;
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: String(created.id), cv: cvString });
    } catch (e) {
      return json({ success:false, message:"Monday change_multiple_column_values failed.", createdItem: created, detail:String(e) }, { status:502 });
    }
  }

  if (dedupeKey) await flowGuardMark(env, dedupeKey);

  return json({ success:true, message:"Monday item created/updated.", mode, boardId, item: created, dedupeKey: dedupeKey || null, engagementId: engagementId || null, columnValuesSent: columnValues });
}

// Legacy write (by selection index) — unchanged logic aside from ANI alias & timeInOut alias handled in builder
async function handleZvaShiftWrite(req, env) {
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const employeeNumber = String(body.employeeNumber || "").trim();
  const selectionIndex = Number(body.selectionIndex ?? 0) || 0;
  const reason         = String(body.callreason ?? "calling off sick").trim();
  const aniRaw         = String(body.ani || body.callerId || "").trim();
  const engagementId   = String(body.engagementId || "").trim();
  const pageStart      = Number(body.pageStart ?? 0) || 0;

  if (!employeeNumber) return json({ success:false, message:"employeeNumber required" }, { status:400 });

  const origin = new URL(req.url).origin;

  let shiftsResp, shifts;
  try {
    const url = new URL("/winteam/shifts", origin);
    shiftsResp = await fetch(url, { method:"POST", headers:{ "content-type":"application/json" }, body: JSON.stringify({ employeeNumber, pageStart }) });
    shifts = await shiftsResp.json();
  } catch (e) {
    return json({ success:false, message:`Failed to fetch shifts: ${e.message || e}` }, { status:502 });
  }

  const entries = Array.isArray(shifts?.entries) ? shifts.entries : [];
  if (!entries.length) return json({ success:false, message:"No shifts returned for employee." }, { status:404 });

  const e = entries[Math.min(Math.max(selectionIndex, 0), Math.min(2, entries.length - 1))];

  const site     = (e.site || e.siteName || "").toString().trim();
  const startISO = (e.startLocalISO || e.startIso || "").toString().trim();
  const endISO   = (e.endLocalISO   || e.endIso   || "").toString().trim();
  const itemName = `${employeeNumber || "unknown"} | ${(shifts?.employeeName || "").toString().trim() || "Unknown Caller"}`;
  const dateKey  = startISO ? startISO.slice(0,10) : "date?";
  const dedupeKey = engagementId || [employeeNumber || "emp?", site || "site?", dateKey].join("|");

  const normPhone = (raw) => {
    if (!raw) return "";
    const s = String(raw);
    const e164 = s.trim().startsWith("+") ? s.replace(/[^\+\d]/g, "") : null;
    const d = s.replace(/\D+/g, "");
    if (e164 && /^\+\d{8,15}$/.test(e164)) return e164;
    if (d.length === 11 && d[0] === "1") return "+" + d;
    if (d.length === 10) return "+1" + d;
    return "";
  };
  const callerId = normPhone(aniRaw);

  const mondayBody = { itemName, dedupeKey, site, shiftStart: startISO, shiftEnd: endISO, callerId, callreason: reason, engagementId };

  let mResp, mData;
  try {
    mResp = await fetch(`${origin}/monday/write`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(mondayBody) });
    mData = await mResp.json();
  } catch (e) {
    return json({ success:false, message:`Monday write failed: ${e.message || e}` }, { status:502 });
  }

  return json({ success: !!mData?.success, message: mData?.message || "Done.", sent: { itemName, dedupeKey, site, startISO, endISO, callerId, reason, engagementId }, monday: mData }, { status: mData?.success ? 200 : 500 });
}

// Preferred: write by cellId (with dateHint day window + chunked fallbacks)
// Preferred: write by cellId (with dateHint day window + chunked fallbacks)
async function handleZvaShiftWriteByCell(req, env) {
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const employeeNumber = String(body.employeeNumber || "").trim();
  const cellId         = String(body.cellId ?? body.selectedCellId ?? "").trim();
  const reason         = String(body.callreason || "calling off sick").trim();
  const aniRaw         = String(body.ani || body.callerId || "").trim();
  const engagementId   = String(body.engagementId || "").trim();
  const dateHint       = String(body.dateHint || "").trim();
  const ofctimeinorout = String(body.ofctimeinorout || body.timeinorout || "").trim();

  if (!employeeNumber) {
    return json({ success:false, message:"employeeNumber required" }, { status:400 });
  }

  // Only allow a no-cellId write to quit flow if:
  //  - reason looks like resignation, OR
  //  - caller explicitly set allow_no_cell=true.
  const explicitAllowNoCell = String(body.allow_no_cell || "").toLowerCase() === "true";
  const allowQuitNoCell = isResignationish(reason) || explicitAllowNoCell;

  // If no cellId but looks like resignation, delegate to quit writer
  if (!cellId && allowQuitNoCell) {
    const quitBody = {
      employeeNumber,
      ofcFullname: String(body.ofcFullname || body.fullname || body.fullName || ""),
      callerId: aniRaw,
      email: String(body.email || body.ofcEmail || ""),
      callreason: reason || "Resignation",
      notes: String(body.notes || body.note || body.details || ""),
      dateHint,                           // “last day” hint if provided
      groupId: String(body.groupId || "") // optional Monday group override
    };
    const fakeReq = new Request(new URL("/zva/quit-write", req.url).toString(), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(quitBody)
    });
    return await handleZvaQuitWrite(fakeReq, env);
  }

  // From here on, cellId is REQUIRED for shift-based writes.
  if (!cellId) {
    return json({
      success:false,
      message:"cellId required for shift-based calls. (Tip: use your tool’s manual intake path for callers without a shift selection, or pass allow_no_cell=true only when it is truly a resignation.)"
    }, { status:400 });
  }

  const normPhone = (raw) => {
    if (!raw) return "";
    const s = String(raw);
    const e = s.trim().startsWith("+") ? s.replace(/[^\+\d]/g, "") : null;
    const d = s.replace(/\D+/g, "");
    if (e && /^\+\d{8,15}$/.test(e)) return e;
    if (d.length === 11 && d[0] === "1") return "+" + d;
    if (d.length === 10) return "+1" + d;
    return "";
  };
  const callerId = normPhone(aniRaw);

  // Try to get employee (for division/department/email)
  let employee = null;
  try { employee = await fetchEmployeeDetail(employeeNumber, env); }
  catch (e) { console.log("ZVA DEBUG writer: fetchEmployeeDetail threw", String(e?.message || e)); }
  const fullName = (employee?.fullName || employee?.name || "").toString().trim();

  // ---- Find the shift by cellId ----
  let shift = null;

  // Resolve anchor date (dateHint if present, else today)
  const anchorYmd = dateHint ? ymdFromISODate(dateHint) : ymd(new Date());

  if (anchorYmd) {
    // 1) Exact day first
    const exact = windowExactDay(anchorYmd);
    console.log("ZVA DEBUG writer: date window exact", exact);
    shift = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate: exact.from, toDate: exact.to });
  }

  // 2) If not found, try ±14 days around anchor (29-day window, <30 cap)
  if (!shift && anchorYmd) {
    const around = windowAround(anchorYmd, 14, 14);
    console.log("ZVA DEBUG writer: date window around", around);
    shift = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate: around.from, toDate: around.to });
  }

  // 3) Last resort: via our own endpoint, still anchored near the same date
  if (!shift) {
    try {
      shift = await fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId, dateHint: anchorYmd });
      if (!shift) console.log("ZVA DEBUG writer: viaSelf fallback also missed", { employeeNumber, cellId, dateHint: anchorYmd });
    } catch (e) {
      console.log("ZVA DEBUG writer: viaSelf fallback threw", String(e?.message || e));
    }
  }

  if (!shift) {
    return json({ success:false, message:"Shift not found by cellId." }, { status:404 });
  }

  const site     = (shift.site || shift.siteName || "").toString().trim();
  const startISO = (shift.startLocalISO || shift.startIso || "").toString().trim();
  const endISO   = (shift.endLocalISO   || shift.endIso   || "").toString().trim();
  console.log("ZVA DEBUG writer: matched cellId", { employeeNumber, cellId, site, start: startISO, end: endISO });

  // ---- Monday write ----
  const itemName  = `${employeeNumber || "unknown"} | ${fullName || "Unknown Caller"}`;
  const dateKey   = startISO ? startISO.slice(0,10) : "date?";
  const dedupeKey = engagementId || [employeeNumber || "emp?", site || "site?", dateKey].join("|");

  const division   = stateFromSupervisor(employee?.supervisorDescription || "");
  const department = deriveDepartmentFromReason(reason || "");
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);

  const startNice = fmtYmdHmUTC(startISO);
  const endNice   = fmtYmdHmUTC(endISO);
  const nowISO    = new Date().toISOString();

  const cvFriendly = {
    site,
    reason: reason,
    callerId,
    startTime: startNice,
    endTime: endNice,
    timeInOut: ofctimeinorout,
    zoomGuid: engagementId || "",
    shift: `${startNice} → ${endNice} @ ${site || ""}`.trim(),
    division,
    department,
    deptEmail,
    dateTime: nowISO,
    email: employee?.emailAddress || "",
    phone: employee?.phone1 || ""
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);
  const boardId = String(env.MONDAY_BOARD_ID);
  const cvString = JSON.stringify(columnValues);

  const createMutation = `
    mutation ($boardId: ID!, $itemName: String!) {
      create_item (board_id: $boardId, item_name: $itemName) { id name board { id } }
    }`;
  let created;
  try {
    const res = await mondayGraphQL(env, createMutation, { boardId, itemName });
    created = res.create_item;
  } catch (e) {
    console.log("ZVA DEBUG writer: Monday create_item threw", String(e?.message || e));
    return json({ success:false, message:`Monday create_item failed: ${e?.message || e}` }, { status:502 });
  }

  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) { id name }
      }`;
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: String(created.id), cv: cvString });
    } catch (e) {
      console.log("ZVA DEBUG writer: Monday change_multiple_column_values threw", String(e?.message || e));
      return json({ success:false, message:`Monday change columns failed: ${e?.message || e}` }, { status:502 });
    }
  }

  if (dedupeKey) await flowGuardMark(env, dedupeKey);

  try {
    const summary = {
      employeeNumber,
      cellId,
      site,
      date: (startISO || "").slice(0, 10),
      timeInOut: ofctimeinorout || "",
      reason,
      department,
      deptEmail,
      mondayItemId: created?.id || null,
      dedupeKey
    };
    console.log(`✅ ZVA SUMMARY: ${pretty(summary)}`);
  } catch (e) {
    console.log("ZVA SUMMARY build error", String(e && e.message || e));
  }

  return json({
    success: true,
    message: "Monday item created/updated.",
    sent: { employeeNumber, cellId, site, startISO, endISO, reason, callerId, engagementId, itemName, dedupeKey },
    monday: { item: created, columnValuesSent: columnValues }
  }, { status: 200 });
}

// --- /zva/absence-write ---
// Creates a non-resignation "Call Off / Absence" item in Monday
async function handleZvaAbsenceWrite(req, env) {
  // Read + normalize
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const S = (v) => (v == null ? "" : String(v).trim());
  const employeeNumber = S(body.employeeNumber || body.ofcEmployeeNumber || "");
  const ofcFullname    = S(body.ofcFullname || body.fullname || body.fullName || "");
  const aniRaw         = S(body.ani || body.callerId || "");
  const emailRaw       = S(body.email || body.ofcEmail || "");
  const callreason     = S(body.callreason || body.reason || "Call off / absence");
  const notes          = S(body.notes || body.note || body.details || "");
  const dateHintRaw    = S(body.dateHint || body.fromDate || body.date || "");

  // Optional free-text hints when no shift was matched
  const siteHint       = S(body.siteHint || body.site || body.location || "");
  const startTime      = S(body.startTime || "");
  const endTime        = S(body.endTime || "");
  const hours          = S(body.hours || "");

  const groupId        = S(body.groupId || ""); // let caller override group if desired
  const boardId        = String(env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) return json({ success:false, message:"boardId missing (set MONDAY_BOARD_ID or MONDAY_DEFAULT_BOARD_ID in env)." }, { status:400 });

  // Minimal identity: employeeNumber OR (name + ANI)
  if (!employeeNumber && !(ofcFullname && aniRaw)) {
    return json({ success:false, message:"Provide employeeNumber OR (ofcFullname + callerId)." }, { status:400 });
  }

  // Normalize basics
  const callerId = normalizeUsPhone(aniRaw);
  const email    = (function (raw) {
    const e = String(raw || "").trim();
    return e && /@/.test(e) ? e : "";
  })(emailRaw);

  // Normalize dateHint -> YYYY-MM-DD (today/tomorrow supported)
  const dateHint = (function toYmd(v) {
    const s = S(v);
    if (!s) return "";
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const kw = s.toLowerCase();
    const base = new Date();
    if (kw === "today")    return ymd(base);
    if (kw === "tomorrow") return ymd(addDaysUTC(base, 1));
    const d = new Date(s);
    return isNaN(d) ? "" : ymd(d);
  })(dateHintRaw);

  // Enrich via WinTeam if we have an employee number
  let employee = null;
  if (employeeNumber) {
    try { employee = await fetchEmployeeDetail(employeeNumber, env); } catch {}
  }
  const fullName  = S(ofcFullname || employee?.fullName || "Unknown Caller");
  const division  = stateFromSupervisor(employee?.supervisorDescription || "") 
                 || STATE_FULL[(employee?.workstate || "").toUpperCase()] 
                 || S(employee?.workstate);
  // Classify as Operations (not HR)
  const department = "Operations";
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);

  // Build item name
  const who = employeeNumber ? `${employeeNumber} | ${fullName}` : fullName || callerId || "Unknown";
  const itemName = `${who} (Call Off)${dateHint ? ` — ${dateHint}` : ""}`;

  // Friendly → Monday column values
  const nowISO = new Date().toISOString();
  const cvFriendly = {
    division,                // Status
    department,              // Status
    deptEmail,               // text
    reason: [
      "Absence / Call Off",
      `Reason: ${callreason}`,
      dateHint ? `Date: ${dateHint}` : null,
      siteHint ? `Site: ${siteHint}` : null,
      (startTime || endTime) ? `Time: ${startTime || "?"} → ${endTime || "?"}` : null,
      hours ? `Hours: ${hours}` : null,
      notes ? `Notes: ${notes}` : null
    ].filter(Boolean).join("\n"),
    site: siteHint,          // also surface in the Site column if provided
    startTime,
    endTime,
    zoomGuid: "",            // none for manual absence
    email,
    phone: employee?.phone1 || "",
    callerId,
    dateTime: nowISO,        // audit stamp
    shift: "Manual Intake – no shift provided"
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);
  const cvString = JSON.stringify(columnValues);

  // Dedupe to avoid repeats (by emp/name + date + reason)
  const deKeyWho = (employeeNumber || fullName || callerId || "unknown").toLowerCase();
  const dedupeKey = `absence|${deKeyWho}|${dateHint || "nodate"}|${callreason.toLowerCase().slice(0,24)}`;
  if (await flowGuardSeen(env, dedupeKey)) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", dedupeKey, upserted:false });
  }

  // Create Monday item (same pattern as other handlers)
  const createMutation = `
    mutation ($boardId: ID!, $itemName: String!, $groupId: String) {
      create_item (board_id: $boardId, item_name: $itemName, group_id: $groupId) {
        id name board { id } group { id }
      }
    }`;
  let created;
  try {
    const res = await mondayGraphQL(env, createMutation, { boardId, itemName, groupId: groupId || null });
    created = res.create_item;
  } catch (e) {
    return json({ success:false, message:"Monday create_item failed.", detail:String(e) }, { status:502 });
  }

  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) { id name }
      }`;
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: String(created.id), cv: cvString });
    } catch (e) {
      return json({ success:false, message:"Monday change_multiple_column_values failed.", createdItem: created, detail:String(e) }, { status:502 });
    }
  }

  await flowGuardMark(env, dedupeKey);

  // Friendly log
  try {
    console.log(`✅ ZVA ABSENCE SUMMARY: ${JSON.stringify({
      employeeNumber: employeeNumber || null,
      fullname: fullName,
      date: dateHint || null,
      siteHint: siteHint || null,
      mondayItemId: created?.id || null,
      dedupeKey
    }, null, 2)}`);
  } catch {}

  return json({
    success: true,
    message: "Absence recorded in Monday.",
    item: created,
    dedupeKey,
    columnValuesSent: columnValues
  }, { status: 200 });
}



// Resignation writer: no shift, no engagementId required
async function handleZvaQuitWrite(req, env) {
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const s  = (v) => (v == null ? "" : String(v).trim());
  const employeeNumber = s(body.employeeNumber || body.ofcEmployeeNumber || "");
  const ofcFullname    = s(body.ofcFullname || body.fullname || body.fullName || "");
  const aniRaw         = s(body.ani || body.callerId || "");
  const emailRaw       = s(body.email || body.ofcEmail || "");
  const reasonRaw      = s(body.callreason || body.reason || "Resignation");
  const notesExtra     = s(body.notes || body.note || body.details || "");
  const lastDayHint    = s(body.quitLastDay || body.lastDay || body.dateHint || "");
  const groupId        = s(body.groupId || ""); // optional Monday group override

  // minimal inputs: employeeNumber OR (fullname + ANI)
  if (!employeeNumber && !(ofcFullname && aniRaw)) {
    return json({ success:false, message:"Provide employeeNumber OR (ofcFullname + callerId)." }, { status:400 });
  }

  // Normalize basics
  const callerId = normalizeUsPhone(aniRaw);
  const email    = (function (raw) {
    const e = String(raw || "").trim();
    return e && /@/.test(e) ? e : "";
  })(emailRaw);

  // Determine last day (default: today)
  const lastDayISO = (function() {
    if (!lastDayHint) return ymd(new Date());
    const d = new Date(lastDayHint);
    return isNaN(d.getTime()) ? ymd(new Date()) : ymd(d);
  })();

  // Enrich from WinTeam if we have an employeeNumber
  let employee = null;
  if (employeeNumber) {
    try { employee = await fetchEmployeeDetail(employeeNumber, env); } catch {}
  }
  const fullName = s(ofcFullname || employee?.fullName || "Unknown Caller");
  
  // Department + routing
  const department = "Operations"; 
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);
  const division   = stateFromSupervisor(employee?.supervisorDescription || "") 
                  || STATE_FULL[(employee?.workstate || "").toUpperCase()] 
                  || s(employee?.workstate);

  // Build item
  const itemName = `${employeeNumber || "UNKNOWN"} | ${fullName} (Resignation)`;

  // Monday board (required)
  const boardId = String(env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) return json({ success:false, message:"boardId missing (set MONDAY_BOARD_ID or MONDAY_DEFAULT_BOARD_ID in env)." }, { status:400 });

  // Idempotency key (uses KV FLOW_GUARD if bound)
  const dedupeKey = `quit|${employeeNumber || callerId || "unknown"}|${lastDayISO}`;
  if (await flowGuardSeen(env, dedupeKey)) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", dedupeKey, upserted:false });
  }

  // Column payload (reuse your builder keys)
  const nowISO = new Date().toISOString();
  const cvFriendly = {
    division,               // will map to Status via builder
    department,             // -> Status
    deptEmail,              // -> text
    reason: [
      "Resignation",
      reasonRaw && `Reason: ${reasonRaw}`,
      `Last day: ${lastDayISO}`,
      notesExtra && `Notes: ${notesExtra}`
    ].filter(Boolean).join("\n"),
    dateTime: nowISO,       // timestamp of intake
    email: email || employee?.emailAddress || "",
    phone: employee?.phone1 || "",
    callerId                // show ANI in its own column
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);
  const cvString = JSON.stringify(columnValues);

  // Create item
  const createMutation = `
    mutation ($boardId: ID!, $itemName: String!, $groupId: String) {
      create_item (board_id: $boardId, item_name: $itemName, group_id: $groupId) {
        id name board { id } group { id }
      }
    }`;
  let created;
  try {
    const res = await mondayGraphQL(env, createMutation, { boardId, itemName, groupId: groupId || null });
    created = res.create_item;
  } catch (e) {
    return json({ success:false, message:"Monday create_item failed.", detail:String(e) }, { status:502 });
  }

  // Set columns
  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) { id name }
      }`;
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: String(created.id), cv: cvString });
    } catch (e) {
      return json({ success:false, message:"Monday change_multiple_column_values failed.", createdItem: created, detail:String(e) }, { status:502 });
    }
  }

  // Mark idempotent
  await flowGuardMark(env, dedupeKey);

  // Friendly summary log
  console.log(`✅ ZVA QUIT SUMMARY: ${pretty({
    employeeNumber: employeeNumber || null,
    fullname: fullName,
    lastDay: lastDayISO,
    deptEmail,
    mondayItemId: created?.id || null,
    dedupeKey
  })}`);

  return json({
    success: true,
    message: "Resignation recorded in Monday.",
    item: created,
    dedupeKey,
    columnValuesSent: columnValues
  }, { status: 200 });
}

///////////////////////////////
// Router
///////////////////////////////
export default {
  fetch: withLogging(async (req, env) => {
    const url = new URL(req.url);
    const { method } = req;
    const path = url.pathname;

    if (method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-headers": "content-type, authorization",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-max-age": "86400"
        }
      });
    }
    
    if (method === "POST" && path === "/auth/employee")           return await handleAuthEmployee(req, env);
    if (method === "POST" && path === "/winteam/shifts")          return await handleShifts(req, env);
    if (method === "POST" && path === "/monday/write")            return await handleMondayWrite(req, env);
    if (method === "POST" && path === "/zva/shift-write")         return await handleZvaShiftWrite(req, env);
    if (method === "POST" && path === "/zva/shift-write-by-cell") return await handleZvaShiftWriteByCell(req, env);
    if (method === "POST" && path === "/zva/quit-write")          return await handleZvaQuitWrite(req, env);
    if (method === "POST" && path === "/zva/absence-write")       return await handleZvaAbsenceWrite(req, env);
  
    return json({ success:false, message:"Not found. Use POST /auth/employee, /winteam/shifts, /monday/write, /zva/shift-write, /zva/shift-write-by-cell, /zva/quit-write, /zva/absence-write" }, { status:404 });
  })
};
