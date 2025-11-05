// Cloudflare Worker: ZVA <-> WinTeam / Monday bridge (PASTE-READY)
// Endpoints:
//  - POST /auth/employee
//  - POST /winteam/shifts
//  - POST /monday/write
//  - POST /zva/shift-write
//  - POST /zva/shift-write-by-cell
//  - POST /zva/quit-write
//  - POST /zva/absence-write
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

// --- Friendly date helpers ---
const WD = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"];
function _ymdFromDate(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,"0")}-${String(d.getUTCDate()).padStart(2,"0")}`;
}
function parseFriendlyDate(input, baseDate = new Date()) {
  const s = String(input || "").trim().toLowerCase();
  if (!s) return "";

  if (s === "today") return _ymdFromDate(baseDate);
  if (s === "tomorrow") return _ymdFromDate(addDaysUTC(baseDate, 1));
  if (s === "yesterday") return _ymdFromDate(addDaysUTC(baseDate, -1));

  const nx = s.match(/^(next|this)\s+(sunday|monday|tuesday|wednesday|thursday|friday|saturday)$/i);
  if (nx) {
    const [, which, day] = nx;
    const target = WD.indexOf(day.toLowerCase());
    if (target >= 0) {
      const base = new Date(Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth(), baseDate.getUTCDate()));
      const cur = base.getUTCDay();
      let delta = (target - cur);
      if (which.toLowerCase() === "next") {
        if (delta <= 0) delta += 7;
        else delta += 7;
      } else {
        if (delta < 0) delta = 0;
      }
      const res = addDaysUTC(base, delta);
      return _ymdFromDate(res);
    }
  }

  const md = s.match(/^(\d{1,2})[\/\-](\d{1,2})$/);
  if (md) {
    const y = baseDate.getUTCFullYear();
    const m = +md[1], d = +md[2];
    const t = Date.UTC(y, m-1, d);
    const res = new Date(t);
    if (!isNaN(res)) return _ymdFromDate(res);
  }

  const mon = s.match(/^(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+(\d{1,2})$/i);
  if (mon) {
    const map = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,sept:8,oct:9,nov:10,dec:11};
    const y = baseDate.getUTCFullYear();
    const M = map[mon[1].slice(0,3).toLowerCase()];
    const d = +mon[2];
    const res = new Date(Date.UTC(y, M, d));
    if (!isNaN(res)) return _ymdFromDate(res);
  }

  const dt = new Date(input);
  if (!isNaN(dt)) return _ymdFromDate(dt);

  return "";
}

async function fetchShiftByDateViaSelf(req, env, { employeeNumber, ymdDate }) {
  if (!employeeNumber || !ymdDate) return null;
  const origin = new URL(req.url).origin;
  const r = await fetch(`${origin}/winteam/shifts`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ employeeNumber, dateFrom: ymdDate, dateTo: ymdAdd(ymdDate, 1) })
  });
  const j = await r.json().catch(() => ({}));
  const list = Array.isArray(j?.entries) ? j.entries : (Array.isArray(j?.entries_page) ? j.entries_page : []);
  if (!list.length) return null;

  list.sort((a,b) => Date.parse(a.startLocalISO || "") - Date.parse(b.startLocalISO || ""));
  const e = list[0];
  return e ? {
    cellId: String(e.cellId || "").trim(),
    site:   String(e.site || e.siteName || "").trim(),
    startLocalISO: String(e.startLocalISO || "").trim(),
    endLocalISO:   String(e.endLocalISO   || "").trim()
  } : null;
}

//
// Reason classifier + time phrase extraction
//
function classifyReason(raw) {
  const r = String(raw || "").toLowerCase();

  const resignationTerms = [
    /resign/, /resignation/, /quit/, /two\s*weeks/, /my\s*last\s*day/,
    /no\s*longer\s*work/, /i'?m\s*leaving\s*the\s*company/, /terminate\s*my\s*employment/
  ];

  const earlyOutTerms = [
    /leav(?:e|ing)\s*early/,
    /going\s*home\s*early/,
    /head(?:ing)?\s*out\s*early/,
    /off\s*early/,
    /left\s*early/,
    /need\s*to\s*go\s*early/,
    /leave\s+by\s+\d+(:\d{2})?\s*(am|pm)\b/
  ];

  const lateInTerms = [
    /\b(runn?ing|be|am|i'?m|arriv(?:e|ing)|come|coming|show(?:ing)?\s*up|start(?:ing)?)\s+late\b/,
    /\b(?:mins?|minutes?)\s*late\b/,
    /\blate\s+by\s+\d+\s*(?:mins?|minutes?)\b/,
    /\b(delay|delayed|traffic|bus\s*delay|train\s*delay|car\s*trouble).*\blate\b/,
    /\b(i'?ll|i\s*will)\s+be\s+\d+\s*(?:mins?|minutes?)\s+late\b/
  ];

  const absenceTerms = [
    /call(?:ing)?\s*off/, /sick/, /not\s*coming\s*in/, /can'?t\s*make\s*it/,
    /no\s*show/, /miss(?:ing)?\s*shift/, /family\s*emergency/
  ];

  if (resignationTerms.some(rx => rx.test(r))) return "resignation";
  if (earlyOutTerms.some(rx => rx.test(r)))    return "early_out";
  if (absenceTerms.some(rx => rx.test(r)))     return "absence";
  if (lateInTerms.some(rx => rx.test(r)))      return "late_in";
  return "unknown";
}

// resignation keyword helper for legacy paths
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

// Parse time-phrases to populate the Time In/Out column
function extractTimeInOutPhrase(text) {
  const s = String(text || "").toLowerCase();

  // e.g., "30 minutes late", "15 mins late", "i'll be 20 minutes late"
  let m = s.match(/\b(\d{1,3})\s*(minutes?|mins?)\s+late\b/);
  if (m) return `Late +${m[1]}m`;

  // e.g., "late by 10 minutes"
  m = s.match(/\blate\s+by\s+(\d{1,3})\s*(minutes?|mins?)\b/);
  if (m) return `Late +${m[1]}m`;

  // e.g., "an hour early", "1 hour early", "2 hours early"
  m = s.match(/\b((an|\d{1,2}))\s*hour(s)?\s+early\b/);
  if (m) {
    const n = m[2] === "an" ? 1 : Number(m[1]) || 1;
    return `Leave early -${n}h`;
  }

  // e.g., "leave by 8pm", "leave by 8:30 pm"
  m = s.match(/\bleave\s+by\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/);
  if (m) {
    let h = Number(m[1]);
    const min = Number(m[2] || 0);
    const ap = m[3];
    if (ap === "pm" && h !== 12) h += 12;
    if (ap === "am" && h === 12) h = 0;
    return `Leave by ${pad2(h)}:${pad2(min)}`;
  }

  // e.g., "i'll be 45 minutes late"
  m = s.match(/\bi'?ll\s+be\s+(\d{1,3})\s*(minutes?|mins?)\s+late\b/);
  if (m) return `Late +${m[1]}m`;

  return ""; // nothing detected
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
    for (const k of Object.keys(v)) out[k] = walk(v[k]);
    return out;
  }
  return walk(payload);
}
async function cloneBodyPreview(reqOrResp) {
  try {
    const ct = reqOrResp.headers?.get?.("content-type") || "";
    if (/json/i.test(ct)) {
      const data = await reqOrResp.clone().json();
      return data;
    }
  } catch {}
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
      resBody: resPreview,
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
// HTTP & external helpers
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
    const raw = await r.text();

    if (!r.ok) {
      return { ok:false, status:r.status, error: (raw || "").slice(0, 1000) || `HTTP ${r.status}` };
    }

    if (!raw || r.status === 204) {
      return { ok:true, data: { data: [] } };
    }

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
function deriveDepartmentFromReason(reasonRaw = "") {
  const r = String(reasonRaw || "").trim();

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
    "\\b(quit|resign|resignation|two[-\\s]?weeks|notice|separation|terminate|termination)\\b"
  ].join("|"), "i");
  if (opsRe.test(r)) return "Operations";

  return "Other";
}

function deriveDepartmentEmail(supervisorDescription = "", department = "") {
  const sup = String(supervisorDescription || "").trim();
  const dep = String(department || "").trim();

  if (/^training$/i.test(dep))  return "training@secureone.com";
  if (/^payroll$/i.test(dep))   return "payroll@secureone.com";
  if (/^hr$/i.test(dep))        return "hr@secureone.com";
  if (/^corporate$/i.test(dep)) return "corporate@secureone.com";

  if (/\b(Chris\s+Jackson|Vince\s+Joyce|Jim\s+McGovern)\b/i.test(sup)) {
    return "corporate@secureone.com";
  }

  const OPS_STATES = new Set(["AL","AZ","TX","TN","OH","IN","IL"]);
  const m = sup.match(/\b(AL|AZ|TX|TN|OH|IN|IL)\b.*\b(Ops|Operations)\b/i);
  if (m) {
    const st = m[1].toUpperCase();
    if (OPS_STATES.has(st)) return `${st.toLowerCase()}opsteam@secureone.com`;
  }

  return "";
}

function deriveDivisionFromSupervisor(supervisorDescription = "", department = "") {
  const sup = String(supervisorDescription || "").trim();
  const dep = String(department || "").trim();

  if (/^corporate$/i.test(dep)) return "Corporate";

  if (/\b(Chris\s+Jackson|Vince\s+Joyce|Jim\s+McGovern)\b/i.test(sup)) {
    return "Corporate";
  }

  const STATE_NAME = { AL:"Alabama", AZ:"Arizona", TX:"Texas", TN:"Tennessee", OH:"Ohio", IN:"Indiana", IL:"Illinois" };
  const m = sup.match(/\b(AL|AZ|TX|TN|OH|IN|IL)\b.*\b(Ops|Operations)\b/i);
  if (m) {
    const st = m[1].toUpperCase();
    if (STATE_NAME[st]) return `${STATE_NAME[st]} Division`;
  }

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
  if (body.ofctimeinorout && !body.timeInOut && !body.timeinorout) body.timeInOut = body.ofctimeinorout;
  if (body.timeinorout && !body.timeInOut) body.timeInOut = body.timeinorout;

  const out = {};
  const addIf = (friendlyKey, transform = (v) => v) => {
    if (body[friendlyKey] != null && body[friendlyKey] !== "") {
      const colId = MONDAY_COLUMN_MAP[friendlyKey];
      if (colId) out[colId] = transform(body[friendlyKey]);
    }
  };

  addIf("site");
  addIf("reason");
  addIf("timeInOut");
  addIf("startTime");
  addIf("endTime");
  addIf("deptEmail");
  addIf("zoomGuid");
  addIf("shift");

  if (!out[MONDAY_COLUMN_MAP.site] && body.ofcWorksite) {
    out[MONDAY_COLUMN_MAP.site] = String(body.ofcWorksite).trim();
  }

  if (body.itemIdEcho) out[MONDAY_COLUMN_MAP.itemIdEcho] = body.itemIdEcho;

  addIf("email", (v) => {
    const email = String(v).trim();
    if (!email || !/@/.test(email)) return undefined;
    return { email, text: email };
  });

  const normPhone = (raw) => String(raw).replace(/[^\d+]/g, "").trim();
  addIf("phone",   (v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });
  addIf("callerId",(v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });

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

  const explicitDept = String(body.department || "").trim();
  const deptRaw = explicitDept || deriveDepartmentFromReason(body.callreason || body.reason || "");
  const dept = (function normalizeDepartmentLabel(v = "") {
    const t = String(v).trim().toLowerCase();
    if (!t) return "";
    if (["hr","human resources","human-resources"].includes(t)) return "Human Resources";
    if (["ops","operations"].includes(t)) return "Operations";
    if (["payroll"].includes(t)) return "Payroll";
    if (["training","train"].includes(t)) return "Training";
    if (["sales"].includes(t)) return "Sales";
    if (["fingerprint","fingerprinting"].includes(t)) return "Fingerprint";
    if (["other"].includes(t)) return "Other";
    return "";
  })(deptRaw);
  if (dept) out[MONDAY_COLUMN_MAP.department] = mondayStatusLabel(dept);

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

// Direct lookup within a provided window (<30d cap)
async function fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate, toDate }) {
  const want = String(cellId ?? "").trim();
  if (!employeeNumber || !want) return null;
  if (!fromDate || !toDate) return null;

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
          endLocal = new Date(endLocal.getTime() + 24*60*60*1000);
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

// Fallback via our own /winteam/shifts filtered by cellId
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
    startLocalISO: String(hit.startLocalISO || "").trim(),
    endLocalISO:   String(hit.endLocalISO   || "").trim()
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

  const startBase = nowAnchor();
  function validYmd(s) { return /^\d{4}-\d{2}-\d{2}$/.test(String(s || "")); }

  let winFrom, winTo;
  if (validYmd(reqDateFrom) && validYmd(reqDateTo)) {
    const fromD = new Date(reqDateFrom + "T00:00:00Z");
    const toD   = new Date(reqDateTo   + "T00:00:00Z");
    if (!isNaN(fromD) && !isNaN(toD)) {
      const maxTo = new Date(fromD.getTime() + 29*24*60*60*1000);
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
  const groupId        = trim(body.groupId || "");

  const ofcFullname  = trim(body.ofcFullname || body.fullname || body.fullName || "");
  const ofcWorkstate = trim(body.ofcWorkstate || body.workState || body.state || "");
  const ofcWorksite  = trim(body.ofcWorksite  || body.site || "");

  if (mode === "manual") {
    const errs = [];
    if (!ofcFullname) errs.push("ofcFullname");
    if (!callreason)  errs.push("callreason");
    if (!ofcPhone && !ofcEmail && !callerId) errs.push("ofcPhone or ofcEmail");
    if (errs.length) return json({ success:false, message:"Missing required fields (manual mode).", missing: errs }, { status:400 });
  }

  let itemName = trim(body.itemName || body.name || "");
  if (!itemName) {
    if (mode === "manual") {
      itemName = `${ofcFullname || "Unknown"} (NO AUTH)`;
    } else {
      itemName = `${employeeNumber || ofcFullname || "Unknown"} | Auto`;
    }
  }

  // capture phrase if present
  const timeInOutForCv = trim(body.timeInOut || body.ofctimeinorout || body.timeinorout || extractTimeInOutPhrase(callreason));

  const cvFriendly = {
    ofcWorkstate,
    ofcWorksite,
    site: body.site,
    reason: callreason,
    timeInOut: timeInOutForCv,
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

  let dedupeKey = trim(body.dedupeKey || "");
  if (!dedupeKey) {
    if (engagementId && employeeNumber) dedupeKey = `${engagementId}:${employeeNumber}`;
    else if (engagementId) dedupeKey = engagementId;
    else if (callerId) dedupeKey = `manual:${callerId}:${new Date().toISOString().slice(0,13)}`;
  }

  if (dedupeKey && (await flowGuardSeen(env, dedupeKey))) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", mode, dedupeKey, upserted:false, item:null, columnValues });
  }

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

// Legacy write (by selection index)
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

  const idx = Math.min(Math.max(selectionIndex, 0), Math.min(2, entries.length - 1));
  const e = entries[idx];

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

  // push parsed phrase to timeInOut too
  const timeInOutForCv = extractTimeInOutPhrase(reason);

  const mondayBody = { itemName, dedupeKey, site, shiftStart: startISO, shiftEnd: endISO, callerId, callreason: reason, timeInOut: timeInOutForCv, engagementId };

  let mResp, mData;
  try {
    mResp = await fetch(`${origin}/monday/write`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(mondayBody) });
    mData = await mResp.json();
  } catch (e) {
    return json({ success:false, message:`Monday write failed: ${e.message || e}` }, { status:502 });
  }

  return json({ success: !!mData?.success, message: mData?.message || "Done.", sent: { itemName, dedupeKey, site, startISO, endISO, callerId, reason, engagementId }, monday: mData }, { status: mData?.success ? 200 : 500 });
}

// Preferred: write by cellId
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

  const explicitAllowNoCell = String(body.allow_no_cell || "").toLowerCase() === "true";
  const allowQuitNoCell = isResignationish(reason) || explicitAllowNoCell;

  if (!cellId && allowQuitNoCell) {
    const quitBody = {
      employeeNumber,
      ofcFullname: String(body.ofcFullname || body.fullname || body.fullName || ""),
      callerId: aniRaw,
      email: String(body.email || body.ofcEmail || ""),
      callreason: reason || "Resignation",
      notes: String(body.notes || body.note || body.details || ""),
      dateHint,
      groupId: String(body.groupId || ""),
      ssnLast4: String(body.ssnLast4 || "") // allow pass-through for verification
    };
    const fakeReq = new Request(new URL("/zva/quit-write", req.url).toString(), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(quitBody)
    });
    return await handleZvaQuitWrite(fakeReq, env);
  }

  if (!cellId) {
    return json({
      success:false,
      message:"cellId required for shift-based calls. (Use manual intake when no shift selection is available.)"
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

  let employee = null;
  try { employee = await fetchEmployeeDetail(employeeNumber, env); }
  catch (e) { console.log("ZVA DEBUG writer: fetchEmployeeDetail threw", String(e?.message || e)); }
  const fullName = (employee?.fullName || employee?.name || "").toString().trim();

  let shift = null;
  const anchorYmd = dateHint ? ymdFromISODate(dateHint) : ymd(new Date());

  if (anchorYmd) {
    const exact = windowExactDay(anchorYmd);
    console.log("ZVA DEBUG writer: date window exact", exact);
    shift = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate: exact.from, toDate: exact.to });
  }
  if (!shift && anchorYmd) {
    const around = windowAround(anchorYmd, 14, 14);
    console.log("ZVA DEBUG writer: date window around", around);
    shift = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId, fromDate: around.from, toDate: around.to });
  }
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
  const startISO = (shift.startLocalISO || "").toString().trim();
  const endISO   = (shift.endLocalISO   || "").toString().trim();
  console.log("ZVA DEBUG writer: matched cellId", { employeeNumber, cellId, site, start: startISO, end: endISO });

  const itemName  = `${employeeNumber || "unknown"} | ${fullName || "Unknown Caller"}`;
  const dateKey   = startISO ? startISO.slice(0,10) : "date?";
  const dedupeKey = engagementId || [employeeNumber || "emp?", site || "site?", dateKey].join("|");

  const division   = stateFromSupervisor(employee?.supervisorDescription || "");
  const department = deriveDepartmentFromReason(reason || "");
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);

  const startNice = fmtYmdHmUTC(startISO);
  const endNice   = fmtYmdHmUTC(endISO);
  const nowISO    = new Date().toISOString();

  // parse a phrase if present
  const timeInOutForCv = ofctimeinorout || extractTimeInOutPhrase(reason);

  const cvFriendly = {
    site,
    reason: reason,
    callerId,
    startTime: startNice,
    endTime: endNice,
    timeInOut: timeInOutForCv,
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
      timeInOut: timeInOutForCv || "",
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

// --- SSN verification for resignations ---
async function verifyEmployeeSSN(env, { employeeNumber, ssnLast4 }) {
  if (!employeeNumber || !ssnLast4 || String(ssnLast4).trim().length !== 4) return { ok:false, match:false };
  const wt = await callWinTeamJSON(buildEmployeeURL(employeeNumber), env);
  if (!wt.ok) return { ok:false, match:false, error: wt.error, status: wt.status };
  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];
  const match = results[0];
  if (!match) return { ok:false, match:false };
  return { ok:true, match: String(match.partialSSN || "").trim() === String(ssnLast4).trim(), raw: match };
}

// --- helper: get earliest shift on a specific YMD
async function fetchEarliestShiftOnDateViaSelf(req, env, { employeeNumber, ymdDay }) {
  if (!employeeNumber || !ymdDay) return null;

  // 1) Try via our own /winteam/shifts
  try {
    const origin = new URL(req.url).origin;
    const body = { employeeNumber, dateFrom: ymdDay, dateTo: ymdAdd(ymdDay, 1), pageStart: 0 };
    const r = await fetch(`${origin}/winteam/shifts`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });

    if (r.ok) {
      let j = null;
      const ct = (r.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("json")) {
        j = await r.json();
      } else {
        try { j = JSON.parse(await r.text()); } catch (_) { j = null; }
      }
      if (j) {
        const arr = Array.isArray(j?.entries) && j.entries.length
          ? j.entries
          : (Array.isArray(j?.entries_page) ? j.entries_page : []);
        if (arr.length) {
          const first = arr[0];
          return {
            cellId: String(first.cellId || "").trim(),
            site: String(first.site || first.siteName || "").trim(),
            siteName: String(first.site || first.siteName || "").trim(),
            startLocalISO: String(first.startLocalISO || first.startIso || "").trim(),
            endLocalISO:   String(first.endLocalISO   || first.endIso   || "").trim()
          };
        }
      }
    } else {
      console.log("ZVA DEBUG earliestOnDate: /winteam/shifts non-OK", r.status);
    }
  } catch (e) {
    console.log("ZVA DEBUG earliestOnDate: /winteam/shifts threw", String(e?.message || e));
  }

  // 2) Fallback: call WinTeam shiftDetails directly and pick the earliest
  try {
    const fromDate = ymdDay, toDate = ymdAdd(ymdDay, 1);
    const url = new URL(SHIFTS_BASE_EXACT);
    url.searchParams.set("employeeNumber", String(employeeNumber));
    url.searchParams.set("fromDate", fromDate);
    url.searchParams.set("toDate", toDate);

    const wt = await callWinTeamJSON(url.toString(), env);
    if (!wt.ok) {
      console.log("ZVA DEBUG earliestOnDate fallback: WT failed", wt.status, (wt.error || "").slice(0, 160));
      return null;
    }

    const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
    const results = Array.isArray(page?.results) ? page.results : [];
    const all = [];
    for (const r of results) {
      const siteName = (r.jobDescription || "").toString().trim();
      const shifts = Array.isArray(r.shifts) ? r.shifts : [];
      for (const s of shifts) {
        const startLocal = parseNaiveAsLocalWall(s.startTime);
        let   endLocal   = parseNaiveAsLocalWall(s.endTime);
        if (!startLocal || !endLocal) continue;
        if (endLocal.getTime() <= startLocal.getTime()) endLocal = new Date(endLocal.getTime() + 24*60*60*1000);
        all.push({
          cellId: String(s.cellId ?? "").trim(),
          site: siteName,
          siteName,
          startLocalISO: startLocal.toISOString(),
          endLocalISO:   endLocal.toISOString()
        });
      }
    }
    if (!all.length) return null;
    all.sort((a,b) => Date.parse(a.startLocalISO) - Date.parse(b.startLocalISO));
    return all[0];
  } catch (e) {
    console.log("ZVA DEBUG earliestOnDate fallback WT threw", String(e?.message || e));
    return null;
  }
}

// --- /zva/absence-write (UPGRADED & timeInOut capture)
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
  const selectedCellId = S(body.selectedCellId || body.cellId || body.selectedCellID || "");

  // Optional free-text when no shift match
  const siteHint       = S(body.siteHint || body.site || body.location || "");
  const startTimeFree  = S(body.startTime || "");
  const endTimeFree    = S(body.endTime || "");
  const hours          = S(body.hours || "");

  const groupId        = S(body.groupId || "");
  const boardId        = String(env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) return json({ success:false, message:"boardId missing (set MONDAY_BOARD_ID or MONDAY_DEFAULT_BOARD_ID in env)." }, { status:400 });

  // Identity: employeeNumber OR (name + ANI)
  if (!employeeNumber && !(ofcFullname && aniRaw)) {
    return json({ success:false, message:"Provide employeeNumber OR (ofcFullname + callerId)." }, { status:400 });
  }

  const callerId = normalizeUsPhone(aniRaw);
  const email    = (() => {
    const e = String(emailRaw || "").trim();
    return e && /@/.test(e) ? e : "";
  })();

  // Friendly date -> YYYY-MM-DD
  const dateHint = (() => {
    const s = dateHintRaw;
    if (!s) return "";
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const kw = s.toLowerCase();
    const base = new Date();
    if (kw === "today")    return ymd(base);
    if (kw === "tomorrow") return ymd(addDaysUTC(base, 1));
    if (kw === "yesterday")return ymd(addDaysUTC(base, -1));
    const d = new Date(s);
    return isNaN(d) ? "" : ymd(d);
  })();

  // Classify reason
  const reasonClass = classifyReason(callreason);
  if (reasonClass === "resignation") {
    return json({ success:false, message:"Resignation requests must use quit-write (with identity verification).", reasonClass }, { status:422 });
  }

  let absenceType = "Call Off";
  if (reasonClass === "early_out") absenceType = "Leave Early";
  if (reasonClass === "absence")   absenceType = "Absence / Call Off";
  if (reasonClass === "late_in")   absenceType = "Late Arrival";

  const reasonSlug = (reasonClass === "early_out") ? "leave_early"
                   : (reasonClass === "absence")   ? "absence"
                   : (reasonClass === "late_in")   ? "late_in"
                   : "other";

  // Enrich via WinTeam
  let employee = null;
  if (employeeNumber) {
    try { employee = await fetchEmployeeDetail(employeeNumber, env); } catch {}
  }
  const fullName  = S(ofcFullname || employee?.fullName || "Unknown Caller");
  const division  = stateFromSupervisor(employee?.supervisorDescription || "") 
                 || STATE_FULL[(employee?.workstate || "").toUpperCase()] 
                 || S(employee?.workstate);
  const department = "Operations";
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);

  // Resolve shift
  let shiftCtx = null;
  if (selectedCellId) {
    const anchorYmd = dateHint || ymd(new Date());
    shiftCtx = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId: selectedCellId, fromDate: anchorYmd, toDate: ymdAdd(anchorYmd, 1) });
    if (!shiftCtx) {
      const around = windowAround(anchorYmd, 14, 14);
      shiftCtx = await fetchShiftByCellIdDirect(env, { employeeNumber, cellId: selectedCellId, fromDate: around.from, toDate: around.to });
    }
    if (!shiftCtx) {
      try {
        shiftCtx = await fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId: selectedCellId, dateHint: anchorYmd });
      } catch (e) {
        console.log("ZVA DEBUG absence: viaSelf fallback threw", String(e?.message || e));
      }
    }
  }
  if (!shiftCtx && dateHint && employeeNumber) {
    shiftCtx = await fetchEarliestShiftOnDateViaSelf(req, env, { employeeNumber, ymdDay: dateHint });
  }

  const nowISO    = new Date().toISOString();
  const itemName   = employeeNumber ? `${employeeNumber} | ${fullName}` : fullName || callerId || "Unknown";
  // const   = `${itemWho} (${absenceType})${dateHint ? ` — ${dateHint}` : ""}`;

  let siteForCv = siteHint;
  let startNice = "";
  let endNice   = "";

  if (shiftCtx) {
    siteForCv = shiftCtx.site || shiftCtx.siteName || siteForCv;
    startNice = fmtYmdHmUTC(shiftCtx.startLocalISO);
    endNice   = fmtYmdHmUTC(shiftCtx.endLocalISO);
  } else {
    startNice = startTimeFree;
    endNice   = endTimeFree;
  }

  // For "Leave Early": require shift or explicit times
  if (reasonClass === "early_out" && !shiftCtx && !startNice && !endNice) {
    return json({ success:false, message:"Leaving early requires either a selectedCellId or start/end times." }, { status:422 });
  }

  // Derive Time In/Out value from inputs + phrase parsing
  const explicitTimeInOut = S(body.timeInOut || body.ofctimeinorout || body.timeinorout || "");
  const parsedPhrase      = extractTimeInOutPhrase(callreason) || extractTimeInOutPhrase(notes);
  const timeInOutForCv    = explicitTimeInOut || parsedPhrase;

  const reasonBlock = [
    absenceType,
    `Reason: ${callreason}`,
    dateHint ? `Date: ${dateHint}` : null,
    siteForCv ? `Site: ${siteForCv}` : null,
    (startNice || endNice) ? `Time: ${startNice || "?"} → ${endNice || "?"}` : null,
    timeInOutForCv ? `Time In/Out: ${timeInOutForCv}` : null,
    hours ? `Hours: ${hours}` : null,
    notes ? `Notes: ${notes}` : null
  ].filter(Boolean).join("\n");

  const cvFriendly = {
    division,
    department,
    deptEmail,
    reason: reasonBlock,
    site: siteForCv,
    startTime: startNice,
    endTime: endNice,
    timeInOut: timeInOutForCv,
    zoomGuid: "",
    email,
    phone: employee?.phone1 || "",
    callerId,
    dateTime: nowISO,
    shift: shiftCtx
      ? `${startNice} → ${endNice} @ ${siteForCv || ""}`.trim()
      : "Manual Intake – no shift provided"
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);
  const cvString = JSON.stringify(columnValues);

  const deKeyWho = (employeeNumber || fullName || callerId || "unknown").toLowerCase();
  const dedupeKey = `absence|${deKeyWho}|${(dateHint || "nodate")}|${reasonSlug}`;
  if (await flowGuardSeen(env, dedupeKey)) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", dedupeKey, upserted:false });
  }

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

  try {
    console.log(`✅ ZVA ABSENCE SUMMARY: ${JSON.stringify({
      employeeNumber: employeeNumber || null,
      fullname: fullName,
      date: dateHint || (shiftCtx?.startLocalISO || "").slice(0,10) || null,
      siteHint: siteForCv || null,
      selectedCellId: selectedCellId || null,
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

// Resignation writer: requires resignation intent + SSN verification
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
  const ssnLast4       = s(body.ssnLast4 || "");
  const groupId        = s(body.groupId || "");

  if (!employeeNumber && !(ofcFullname && aniRaw)) {
    return json({ success:false, message:"Provide employeeNumber OR (ofcFullname + callerId)." }, { status:400 });
  }

  // Must actually look like a resignation
  if (classifyReason(reasonRaw) !== "resignation") {
    return json({ success:false, message:"This does not appear to be a resignation. Use absence-write for call-offs or leaving early." }, { status:422 });
  }

  // Require SSN verification for resignation
  let verified = false, employee = null;
  if (employeeNumber && ssnLast4.length === 4) {
    try {
      const v = await verifyEmployeeSSN(env, { employeeNumber, ssnLast4 });
      verified = !!v.match;
      if (v.ok && v.raw) {
        employee = {
          fullName: [s(v.raw.firstName), s(v.raw.lastName)].filter(Boolean).join(" "),
          workstate: s(v.raw.state || v.raw.workState || ""),
          supervisorDescription: s(v.raw.supervisorDescription || ""),
          emailAddress: s(v.raw.emailAddress || ""),
          phone1: normalizeUsPhone(v.raw.phone1 || "")
        };
      }
    } catch {}
  }
  if (!verified) {
    return json({ success:false, message:"Identity verification required for resignation. Please authenticate with employeeNumber + last4 SSN." }, { status:401 });
  }

  if (!employee && employeeNumber) {
    try { employee = await fetchEmployeeDetail(employeeNumber, env); } catch {}
  }

  const fullName = s(ofcFullname || employee?.fullName || "Unknown Caller");
  const callerId = normalizeUsPhone(aniRaw);
  const email    = (function (raw) { const e = String(raw || "").trim(); return e && /@/.test(e) ? e : ""; })(emailRaw);

  const friendlyLastDay = lastDayHint ? parseFriendlyDate(lastDayHint) : "";
  const lastDayISO = friendlyLastDay || ymd(new Date());

  const department = "Operations"; // change to "HR" if you prefer
  const deptEmail  = deriveDepartmentEmail(employee?.supervisorDescription || "", department);
  const division   = stateFromSupervisor(employee?.supervisorDescription || "") 
                  || STATE_FULL[(employee?.workstate || "").toUpperCase()] 
                  || s(employee?.workstate);

  const itemName = `${employeeNumber || "UNKNOWN"} | ${fullName}`;

  const boardId = String(env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) return json({ success:false, message:"boardId missing (set MONDAY_BOARD_ID or MONDAY_DEFAULT_BOARD_ID in env)." }, { status:400 });

  const dedupeKey = `quit|${employeeNumber || callerId || "unknown"}|${lastDayISO}`;
  if (await flowGuardSeen(env, dedupeKey)) {
    return json({ success:true, message:"Duplicate suppressed by flow guard.", dedupeKey, upserted:false });
  }

  const nowISO = new Date().toISOString();
  const cvFriendly = {
    division,
    department,
    deptEmail,
    reason: [
      "Resignation",
      reasonRaw && `Reason: ${reasonRaw}`,
      `Last day: ${lastDayISO}`,
      "Identity: Verified by SSN last4",
      notesExtra && `Notes: ${notesExtra}`
    ].filter(Boolean).join("\n"),
    dateTime: nowISO,
    email: email || employee?.emailAddress || "",
    phone: employee?.phone1 || "",
    callerId
  };
  const columnValues = buildMondayColumnsFromFriendly(cvFriendly);
  const cvString = JSON.stringify(columnValues);

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

  console.log(`✅ ZVA QUIT SUMMARY: ${pretty({
    employeeNumber: employeeNumber || null,
    fullname: fullName,
    lastDay: lastDayISO,
    verified: true,
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
