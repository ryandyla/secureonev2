// Cloudflare Worker: ZVA <-> WinTeam / Monday bridge
//
// Endpoints:
//  - POST /auth/employee
//  - POST /winteam/shifts
//  - POST /monday/write
//  - GET  /debug/env           (optional: shows which bindings exist; no values)
//
// Required secrets: WINTEAM_TENANT_ID, WINTEAM_API_KEY, MONDAY_API_KEY
// Optional secrets: MONDAY_BOARD_ID, MONDAY_DEFAULT_BOARD_ID, FLOW_GUARD_TTL_SECONDS
// Optional KV binding: FLOW_GUARD
//
// wrangler.toml example:
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
  emailStatus: "color_mkv0cpxc",  // "Email Status:" (Status if used)
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
function normalizeUsPhone(raw) {
  if (!raw) return "";
  const s = String(raw);
  const d = s.replace(/\D+/g, "");
  if (d.length === 11 && d[0] === "1") return "+" + d;
  if (d.length === 10) return "+1" + d;
  if (/^\+\d{8,15}$/.test(s.trim())) return s.trim();
  return s.trim();
}
function fmt12h(h, m) {
  let hour = h % 12;
  if (hour === 0) hour = 12;
  const ampm = h < 12 ? "AM" : "PM";
  return `${hour}:${pad2(m)} ${ampm}`;
}
function weekdayMonthDay(d) {
  const wk = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][d.getUTCDay()];
  const mo = ["January","February","March","April","May","June","July","August","September","October","November","December"][d.getUTCMonth()];
  return `${wk}, ${mo} ${d.getUTCDate()}`;
}
function ymdFromDate(d) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

///////////////////////////////
// Logging (PII-safe)
///////////////////////////////

const MAX_LOG_BODY = 2048;

function headerObj(headers) {
  const safeKeys = [
    "content-type","user-agent","cf-ray","x-forwarded-for","x-real-ip",
    "accept","accept-encoding","accept-language","host","origin","referer"
  ];
  const out = {};
  for (const [k, v] of headers.entries()) {
    if (/authorization|api|key|token|cookie|set-cookie|ocp-apim-subscription-key/i.test(k)) continue;
    if (safeKeys.includes(k.toLowerCase())) out[k] = v;
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
async function cloneBodyPreview(reqOrResp) {
  try {
    const ct = reqOrResp.headers?.get?.("content-type") || "";
    if (/json/i.test(ct)) {
      const text = JSON.stringify(await reqOrResp.clone().json());
      return maskPII(text).slice(0, MAX_LOG_BODY);
    }
  } catch {}
  try {
    const text = await reqOrResp.clone().text();
    return maskPII(text).slice(0, MAX_LOG_BODY);
  } catch { return ""; }
}
function withLogging(handler) {
  return async (req, env, ctx) => {
    const start = Date.now();
    const url = new URL(req.url);
    const reqPreview = await cloneBodyPreview(req);
    let res;
    try {
      res = await handler(req, env, ctx);
    } catch (e) {
      res = json({ success:false, message:"Unhandled error.", detail:String(e && e.message || e) }, { status: 500 });
    }
    const resPreview = await cloneBodyPreview(res);
    const ms = Date.now() - start;
    console.log("HTTP", JSON.stringify({
      at: new Date().toISOString(),
      ray: req.headers.get("cf-ray") || undefined,
      method: req.method,
      path: url.pathname,
      query: Object.fromEntries(url.searchParams.entries()),
      headers: headerObj(req.headers),
      reqBody: reqPreview,
      status: res.status,
      resHeaders: headerObj(res.headers || new Headers()),
      resBody: resPreview,
      ms
    }));
    return res;
  };
}

///////////////////////////////
// HTTP helpers
///////////////////////////////

async function readJson(req) {
  try {
    const j = await req.json();
    if (j && typeof j === "object" && typeof j.body === "string") {
      try { return JSON.parse(j.body); } catch { return j; }
    }
    return j;
  } catch {}
  try {
    const t = await req.text();
    if (!t) return null;
    const j = JSON.parse(t);
    if (j && typeof j === "object" && typeof j.body === "string") {
      try { return JSON.parse(j.body); } catch { return j; }
    }
    return j;
  } catch { return null; }
}

function buildWTHeaders(TENANT_ID, API_KEY) {
  return {
    tenantId: TENANT_ID,
    "Ocp-Apim-Subscription-Key": API_KEY,
    accept: "application/json"
  };
}

async function callWinTeamJSON(url, env) {
  const TENANT_ID = env.WINTEAM_TENANT_ID || "";
  const API_KEY = env.WINTEAM_API_KEY || "";
  if (!TENANT_ID || !API_KEY) {
    return { ok: false, status: 500, error: "Missing WINTEAM_TENANT_ID or WINTEAM_API_KEY." };
  }
  try {
    const r = await fetch(url, { headers: buildWTHeaders(TENANT_ID, API_KEY) });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      return { ok: false, status: r.status, error: text.slice(0, 1000) };
    }
    const data = await r.json();
    return { ok: true, data };
  } catch (e) {
    return { ok: false, status: 502, error: String(e?.message || e) };
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
// Time parsing for shifts
///////////////////////////////

// Parse "YYYY-MM-DDTHH:mm:ss" (no TZ) as a *local wall-clock* Date in UTC frame.
function parseNaiveAsLocalWall(isoNoTZ) {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(String(isoNoTZ));
  if (!m) return null;
  const [ , Y, M, D, h, min, s ] = m;
  return new Date(Date.UTC(
    Number(Y), Number(M) - 1, Number(D),
    Number(h), Number(min), Number(s || 0)
  ));
}

// Tenant "home" anchor for WinTeam windowing; adjust if your org prefers another tz.
// Using UTC is acceptable; we also locally filter per-row using utCoffset.
function nowAnchor() {
  return new Date(); // UTC "now"
}

///////////////////////////////
// Division/Department helpers
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
  // Examples: "IL Ops Team", "AZ Operations", "TX Night Shift"
  const m = String(supervisorDescription).trim().match(/^([A-Z]{2})\b/);
  if (!m) return "";
  const full = STATE_FULL[m[1]];
  return full || "";
}

function deriveDepartmentFromReason(reasonRaw = "") {
  const r = String(reasonRaw).toLowerCase();
  if (/\b(payroll|pay\s*issue|pay\s*check|pay\s*stub|w2|w-?2|tax|withhold)/i.test(r)) return "Payroll";
  if (/\b(training|train|course|lms|cert|certificate|guard\s*card)/i.test(r)) return "Training";
  if (/\b(call\s*off|calloff|no\s*show|incident|report|time\s*card|timecard|punch|missed\s*punch|late|coverage|schedule)/i.test(r)) return "Operations";
  return "Other";
}

function mondayStatusLabel(label) {
  const v = String(label || "").trim();
  return v ? { label: v } : undefined;
}

///////////////////////////////
// Handlers
///////////////////////////////

async function handleAuthEmployee(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

  const employeeNumber = trim(body.employeeNumber);
  const ssnLast4 = trim(body.ssnLast4);
  if (!employeeNumber || !ssnLast4 || ssnLast4.length !== 4) {
    return json({ success: false, message: "Expect { employeeNumber, ssnLast4(4 digits) }." }, { status: 400 });
  }

  const wt = await callWinTeamJSON(buildEmployeeURL(employeeNumber), env);
  if (!wt.ok) {
    return json(
      { success: false, message: `WinTeam employees request failed (${wt.status}).`, detail: wt.error },
      { status: 502 }
    );
  }

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];
  const match = results[0] || null;
  if (!match) return json({ success: false, message: "No matching employee found." }, { status: 404 });
  if (trim(match.partialSSN) !== ssnLast4) {
    return json({ success: false, message: "SSN verification failed." }, { status: 401 });
  }

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

  return json({ success: true, message: "Successful employee lookup.", employee: payload });
}

async function handleShifts(req, env) {
  // ---- Read body and unwrap common shapes ----
  let body = await readJson(req);
  if (!body) body = {};
  if (body && typeof body === "object") {
    if (body.params && typeof body.params === "object") body = body.params;
    else if (body.json && typeof body.json === "object") body = body.json;
    else if (body.data && typeof body.data === "object") body = body.data;
  }

  const employeeNumber = trim(body.employeeNumber);
  const pageStart = Number(body.pageStart ?? 0) || 0;

  // Use distinct names to avoid collisions / shadowing
  const reqDateFrom = trim(body.dateFrom || ""); // optional YYYY-MM-DD
  const reqDateTo   = trim(body.dateTo   || ""); // optional YYYY-MM-DD

  if (!employeeNumber) {
    return json({ success:false, message:"employeeNumber is required." }, { status:400 });
  }

  // ---- Choose window for WinTeam request (honor dateFrom/dateTo if provided) ----
  const startBase = nowAnchor();
  const winFrom = reqDateFrom || ymd(startBase);
  const winTo   = reqDateTo   || ymd(addDaysUTC(startBase, 15));

  const wtUrl = new URL(SHIFTS_BASE_EXACT);
  wtUrl.searchParams.set("employeeNumber", employeeNumber);
  wtUrl.searchParams.set("fromDate", winFrom);
  wtUrl.searchParams.set("toDate", winTo);

  const wt = await callWinTeamJSON(wtUrl.toString(), env);
  if (!wt.ok) {
    return json(
      { success: false, message: `WinTeam shiftDetails failed (${wt.status}).`, detail: wt.error },
      { status: 502 }
    );
  }

  // ---- Normalize rows ----
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
      if (endLocal.getTime() <= startLocal.getTime()) {
        endLocal = new Date(endLocal.getTime() + 24*60*60*1000); // overnight roll
      }

      const concise =
        `${ymdFromDate(startLocal)} ${pad2(startLocal.getUTCHours())}:${pad2(startLocal.getUTCMinutes())}` +
        ` → ${pad2(endLocal.getUTCHours())}:${pad2(endLocal.getUTCMinutes())}` +
        (site ? ` @ ${site}` : "") + (role ? ` (${role})` : "");

      const speakLine =
        `${weekdayMonthDay(startLocal)}, ${fmt12h(startLocal.getUTCHours(), startLocal.getUTCMinutes())}` +
        ` to ${weekdayMonthDay(endLocal)} ${fmt12h(endLocal.getUTCHours(), endLocal.getUTCMinutes())}` +
        (site ? ` at ${site}` : "") + (role ? ` (${role})` : "");

      const cellId = String(s.cellId ?? s.cellID ?? s.CellId ?? s.CellID ?? s.cell ?? "").trim();
      const scheduleDetailID = String(s.scheduleDetailID ?? s.ScheduleDetailID ?? s.scheduleDetailId ?? "").trim();

      rows.push({
        employeeNumber: trim(r.employeeNumber || employeeNumber),
        site, role, utcOffset,
        hours: s.hours,
        hourType: trim(s.hourType),
        hourDescription: trim(s.hourDescription),
        cellId,                 // unique ID we’ll use
        scheduleDetailID,       // NOT unique; for reference only
        id: cellId,             // alias if something expects .id
        startLocalISO: startLocal.toISOString(),
        endLocalISO:   endLocal.toISOString(),
        concise, speakLine
      });
    }
  }

  // ---- Optional additional narrowing by exact day range (if caller provided both) ----
  const inRange = (iso, fromYmd, toYmd) => {
    if (!fromYmd || !toYmd || !iso) return true;
    const d = new Date(iso); if (isNaN(d)) return false;
    const s = d.toISOString().slice(0,10);
    return (s >= fromYmd && s < toYmd);
  };

  let filtered = rows;
  if (reqDateFrom && reqDateTo) {
    filtered = filtered.filter(e =>
      inRange(e.startLocalISO, reqDateFrom, reqDateTo) ||
      inRange(e.endLocalISO,   reqDateFrom, reqDateTo)
    );
  }

  // ---- Sort & slice a page of 3 (compat) ----
  filtered.sort((a, b) => Date.parse(a.startLocalISO) - Date.parse(b.startLocalISO));
  const countTotal = filtered.length;
  const p = Math.max(0, pageStart|0);
  const pageRows = filtered.slice(p, p + 3);
  const nextPageStart = p + 3 < countTotal ? p + 3 : p;
  const hasNext = p + 3 < countTotal;

  const speakable_page = pageRows.map(r => r.speakLine);
  const entries_page = pageRows.map(({employeeNumber, site, role, startLocalISO, endLocalISO, hours, concise, hourType, hourDescription, scheduleDetailID, cellId, id}) => ({
    employeeNumber, site, role, startLocalISO, endLocalISO, hours, concise, hourType, hourDescription, cellId, scheduleDetailID, id
  }));

  return json({
    success: true,
    message: "Shifts lookup completed.",
    window: `${winFrom} → ${winTo}`,
    counts: { rows: rows.length, filtered: countTotal },
    page: { pageStart: p, nextPageStart, hasNext, pageCount: pageRows.length },
    speakable_page,
    entries_page,
    speakable: filtered.map(r => r.speakLine),
    entries: filtered,
    raw: wt.data
  });
}

function buildMondayColumnsFromFriendly(body) {
  const out = {};
  const addIf = (friendlyKey, transform = (v) => v) => {
    if (body[friendlyKey] != null && body[friendlyKey] !== "") {
      const colId = MONDAY_COLUMN_MAP[friendlyKey];
      if (colId) out[colId] = transform(body[friendlyKey]);
    }
  };

  // 1) Plain text-ish fields
  addIf("site");
  addIf("reason");
  addIf("timeInOut");
  addIf("startTime");
  addIf("endTime");
  addIf("deptEmail");
  addIf("zoomGuid");
  addIf("shift");

  if (body.itemIdEcho) out[MONDAY_COLUMN_MAP.itemIdEcho] = body.itemIdEcho;

  // 2) EMAIL
  addIf("email", (v) => {
    const email = String(v).trim();
    if (!email || !/@/.test(email)) return undefined;
    return { email, text: email };
  });

  // 3) PHONE / CALLER ID
  const normPhone = (raw) => String(raw).replace(/[^\d+]/g, "").trim();
  addIf("phone",   (v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });
  addIf("callerId",(v) => { const p = normPhone(v); return p ? { phone: p, countryShortName: "US" } : undefined; });

  // 4) DATE/TIME
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

  // 5) DIVISION
  const explicitDivision = String(body.division || "").trim();
  let derivedDivision = "";
  if (!explicitDivision) {
    derivedDivision = stateFromSupervisor(body.supervisorDescription);
    if (!derivedDivision) {
      const rawState = String(body.ofcWorkstate || body.workState || body.state || "").trim();
      if (rawState.length === 2 && STATE_FULL[rawState.toUpperCase()]) {
        derivedDivision = STATE_FULL[rawState.toUpperCase()];
      } else if (rawState) {
        derivedDivision = rawState;
      }
    }
  }
  const finalDivision = explicitDivision || derivedDivision;
  if (finalDivision) out[MONDAY_COLUMN_MAP.division] = mondayStatusLabel(finalDivision);

  // 6) DEPARTMENT
  const explicitDept = String(body.department || "").trim();
  const dept = explicitDept || deriveDepartmentFromReason(body.reason || "");
  if (dept) out[MONDAY_COLUMN_MAP.department] = mondayStatusLabel(dept);

  // Remove undefined
  for (const k of Object.keys(out)) { if (out[k] === undefined) delete out[k]; }
  return out;
}

async function handleMondayWrite(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

  // Normalize engagement id
  let engagementId = trim(body.engagementId || body.zoomEngId || body.zoomGuid || "");
  if (engagementId && !body.zoomGuid) body.zoomGuid = engagementId;

  // Board ID
  const boardId = String(body.boardId || env.MONDAY_BOARD_ID || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
  if (!boardId) {
    return json({ success:false, message:"boardId is required (env or body)." }, { status: 400 });
  }

  const itemName = trim(body.itemName || body.name || "");
  const groupId = trim(body.groupId || "");
  if (!itemName) return json({ success: false, message: "itemName is required." }, { status: 400 });

  // Dedupe Key
  const employeeNumber = trim(body.employeeNumber || body.ofcEmployeeNumber || "");
  let dedupeKey = trim(body.dedupeKey || "");
  if (!dedupeKey) {
    if (engagementId && employeeNumber) dedupeKey = `${engagementId}:${employeeNumber}`;
    else if (engagementId) dedupeKey = engagementId;
  }

  const columnValues = buildMondayColumnsFromFriendly(body);

  if (dedupeKey && (await flowGuardSeen(env, dedupeKey))) {
    return json({ success: true, message: "Duplicate suppressed by flow guard.", dedupeKey, upserted: false, item: null, columnValues });
  }

  // Create item
  const createMutation = `
    mutation ($boardId: ID!, $itemName: String!, $groupId: String) {
      create_item (board_id: $boardId, item_name: $itemName, group_id: $groupId) {
        id
        name
        board { id }
        group { id }
      }
    }
  `;
  let created;
  try {
    const res = await mondayGraphQL(env, createMutation, { boardId, itemName, groupId: groupId || null });
    created = res.create_item;
  } catch (e) {
    return json({ success: false, message: "Monday create_item failed.", detail: String(e) }, { status: 502 });
  }

  // Update columns
  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: ID!, $itemId: ID!, $cv: JSON!) {
        change_multiple_column_values (board_id: $boardId, item_id: $itemId, column_values: $cv) {
          id
          name
        }
      }
    `;
    const cvString = JSON.stringify(columnValues);
    try {
      await mondayGraphQL(env, changeMutation, { boardId, itemId: String(created.id), cv: cvString });
    } catch (e) {
      return json({ success: false, message: "Monday change_multiple_column_values failed.", createdItem: created, detail: String(e) }, { status: 502 });
    }
  }

  if (dedupeKey) await flowGuardMark(env, dedupeKey);

  return json({
    success: true,
    message: "Monday item created/updated.",
    boardId,
    item: created,
    dedupeKey: dedupeKey || null,
    engagementId: engagementId || null,
    columnValuesSent: columnValues
  });
}

// It expects JSON: { employeeNumber, selectionIndex, reason, ani, engagementId, pageStart? }
async function handleZvaShiftWrite(req, env) {
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const employeeNumber = String(body.employeeNumber || "").trim();
  const selectionIndex = Number(body.selectionIndex ?? 0) || 0;
  const reason         = (body.reason ?? "calling off sick").toString().trim();
  const ani            = (body.ani || "").toString().trim();
  const engagementId   = (body.engagementId || "").toString().trim();
  const pageStart      = Number(body.pageStart ?? 0) || 0;
  // added for consistency in case we decide to call this endpoint again, cell ID is piped in
  const cellId = String(body.cellId ?? body.selectedCellId ?? "").trim();

  if (!employeeNumber) {
    return json({ success:false, message:"employeeNumber required" }, { status:400 });
  }

  const origin = new URL(req.url).origin;

  // Pull shifts from same worker
  let shiftsResp, shifts;
  try {
    const url = new URL("/winteam/shifts", origin);
    // keep POST to avoid GET/POST mismatch
    shiftsResp = await fetch(url, { method:"POST", headers:{ "content-type":"application/json" },
      body: JSON.stringify({ employeeNumber, pageStart }) });
    shifts = await shiftsResp.json();
  } catch (e) {
    return json({ success:false, message:`Failed to fetch shifts: ${e.message || e}` }, { status:502 });
  }

  const entries = Array.isArray(shifts?.entries) ? shifts.entries : [];
  if (!entries.length) {
    return json({ success:false, message:"No shifts returned for employee." }, { status:404 });
  }

  const e = entries[Math.min(Math.max(selectionIndex, 0), Math.min(2, entries.length - 1))];

  const site       = (e.site || e.siteName || "").toString().trim();
  const startISO   = (e.startLocalISO || e.startIso || "").toString().trim();
  const endISO     = (e.endLocalISO   || e.endIso   || "").toString().trim();
  const emp        = employeeNumber;

  const itemName = `${emp || "unknown"} | ${(shifts?.employeeName || "").toString().trim() || "Unknown Caller"}`;
  const dateKey = startISO ? startISO.slice(0,10) : "date?";
  const dedupeKey = engagementId || [emp || "emp?", site || "site?", dateKey].join("|");

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
  const callerId = normPhone(ani);

  const mondayBody = { itemName, dedupeKey };
  if (site)       mondayBody.accountSite = site;
  if (startISO)   mondayBody.shiftStart  = startISO;
  if (endISO)     mondayBody.shiftEnd    = endISO;
  if (callerId)   mondayBody.callerId    = callerId;
  if (reason)     mondayBody.reason      = reason;
  if (engagementId) mondayBody.engagementId = engagementId;

  let mResp, mData;
  try {
    mResp = await fetch(`${origin}/monday/write`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(mondayBody)
    });
    mData = await mResp.json();
  } catch (e) {
    return json({ success:false, message:`Monday write failed: ${e.message || e}` }, { status:502 });
  }

  return json({
    success: !!mData?.success,
    message: mData?.message || "Done.",
    sent: { itemName, dedupeKey, site, startISO, endISO, callerId, reason, engagementId },
    monday: mData
  }, { status: mData?.success ? 200 : 500 });
}

// Write by cellId (preferred)
async function handleZvaShiftWriteByCell(req, env) {
  let body = await readJson(req);
  if (body?.json && typeof body.json === "object") body = body.json;
  if (body?.data && typeof body.data === "object") body = body.data;

  const employeeNumber = String(body.employeeNumber || "").trim();
  const cellId         = String(body.cellId ?? body.selectedCellId ?? "").trim();
  const reason         = String(body.reason || "calling off sick").trim();
  const aniRaw         = String(body.ani || "").trim();
  const engagementId   = String(body.engagementId || "").trim();
  const dateHint       = String(body.dateHint || "").trim(); // optional: "YYYY-MM-DD"

  if (!employeeNumber) return json({ success:false, message:"employeeNumber required" }, { status:400 });
  if (!cellId)         return json({ success:false, message:"cellId required" }, { status:400 });

  const origin = new URL(req.url).origin;
  const normPhone = (raw)=> {
    if (!raw) return "";
    const s=String(raw);
    const e=s.trim().startsWith("+")?s.replace(/[^\+\d]/g,""):null;
    const d=s.replace(/\D+/g,"");
    if (e && /^\+\d{8,15}$/.test(e)) return e;
    if (d.length===11 && d[0]==="1") return "+"+d;
    if (d.length===10) return "+1"+d;
    return "";
  };

  // TODO: wire to your real WinTeam helpers
  const employee = await fetchEmployeeDetail(employeeNumber, env).catch(()=>null);
  const fullName = (employee?.fullName || employee?.name || "").toString().trim();

  // --- 2) Fetch SHIFT details by calling our own /winteam/shifts and filtering by cellId
  let shift = null;
+  // --- 2) Fetch SHIFT details by calling our own /winteam/shifts and filtering by cellId
+  // Use the helper that actually exists: fetchShiftByCellIdViaSelf
+  let shift = await fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId, dateHint }).catch(()=>null);
+  // If a dateHint was provided but the narrow search missed, retry without the hint (wider 15-day window).
+  if (!shift && dateHint) {
+    try {
+      console.log("ZVA DEBUG writer: no hit with dateHint; retrying without dateHint", { employeeNumber, cellId, dateHint });
+      shift = await fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId, dateHint: "" });
+    } catch (e) {
+      console.log("ZVA DEBUG writer: retry fetch without dateHint threw", String(e && e.message || e));
+    }
+  }
+  if (!shift) {
+    console.log("ZVA DEBUG writer: Shift not found by cellId in either attempt", { employeeNumber, cellId, dateHint });
+    return json({ success:false, message:"Shift not found by cellId." }, { status:404 });
+  }

  const site     = (shift.site || shift.siteName || "").toString().trim();
  const startISO = (shift.startLocalISO || shift.startIso || "").toString().trim();
  const endISO   = (shift.endLocalISO   || shift.endIso   || "").toString().trim();

  const itemName = `${employeeNumber || "unknown"} | ${fullName || "Unknown Caller"}`;
  const dateKey  = startISO ? startISO.slice(0,10) : "date?";
  const dedupeKey = engagementId || [employeeNumber || "emp?", site || "site?", dateKey].join("|");
  const callerId = normPhone(aniRaw);

  const mondayBody = { itemName, dedupeKey };
  if (site)       mondayBody.accountSite = site;
  if (startISO)   mondayBody.shiftStart  = startISO;
  if (endISO)     mondayBody.shiftEnd    = endISO;
  if (callerId)   mondayBody.callerId    = callerId;
  if (reason)     mondayBody.reason      = reason;
  if (engagementId) mondayBody.engagementId = engagementId;

  let mResp, mData;
  try {
    mResp = await fetch(`${origin}/monday/write`, {
      method: "POST",
      headers: { "content-type":"application/json" },
      body: JSON.stringify(mondayBody)
    });
    mData = await mResp.json();
  } catch (e) {
    return json({ success:false, message:`Monday write failed: ${e?.message||e}` }, { status:502 });
  }

  return json({
    success: !!mData?.success,
    message: mData?.message || "Done.",
    sent: { employeeNumber, cellId, site, startISO, endISO, reason, callerId, engagementId, itemName, dedupeKey },
    monday: mData
  }, { status: mData?.success ? 200 : 500 });
}

// ---- Winteam Helpers
async function fetchEmployeeDetail(employeeNumber, env) {
  const url = buildEmployeeURL(employeeNumber);
  const wt = await callWinTeamJSON(url, env);
  if (!wt.ok) throw new Error(`WinTeam employees failed (${wt.status}) ${wt.error||""}`);

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];
  const e = results[0];
  if (!e) return null;
  return {
    fullName: [trim(e.firstName), trim(e.lastName)].filter(Boolean).join(" "),
    workstate: trim(e.state || e.workState || ""),
    supervisorDescription: trim(e.supervisorDescription || "")
  };
}
/**
 * Fetch a single shift by cellId (aka scheduleDetailID) for an employee.
 * Uses a wide window so we don’t miss past/future dates.
 */
async function fetchShiftByCellId(employeeNumber, cellId, env) {
  const norm = (v) => String(v ?? "").trim();
  const want = norm(cellId);

  // Search a reasonably-wide window (past 60d → next 90d)
  const now = new Date();
  const fromDate = ymd(addDaysUTC(now, -60));
  const toDate   = ymd(addDaysUTC(now,  90));

  const url = new URL(SHIFTS_BASE_EXACT);
  url.searchParams.set("employeeNumber", String(employeeNumber));
  url.searchParams.set("fromDate", fromDate);
  url.searchParams.set("toDate", toDate);

  const wt = await callWinTeamJSON(url.toString(), env);
  if (!wt.ok) throw new Error(`WinTeam shiftDetails failed (${wt.status}) ${wt.error||""}`);

  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];

  for (const r of results) {
    const siteName = trim(r.jobDescription);
    const roleName = trim(r.postDescription);
    const shifts = Array.isArray(r.shifts) ? r.shifts : [];
    for (const s of shifts) {
      const have = norm(s.scheduleDetailID ?? s.cellId);
      if (have && have === want) {
        // Build normalized shape the writer expects
        const startLocal = parseNaiveAsLocalWall(s.startTime);
        let   endLocal   = parseNaiveAsLocalWall(s.endTime);
        if (startLocal && endLocal && endLocal.getTime() <= startLocal.getTime()) {
          endLocal = new Date(endLocal.getTime() + 24*60*60*1000); // overnight
        }
        return {
          cellId: have,
          site: siteName,
          siteName,
          role: roleName,
          startLocalISO: startLocal ? startLocal.toISOString() : "",
          endLocalISO:   endLocal   ? endLocal.toISOString()   : ""
        };
      }
    }
  }
  return null; // not found in the window
}

// Look up a single shift by cellId by calling our own /winteam/shifts and filtering.
async function fetchShiftByCellIdViaSelf(req, env, { employeeNumber, cellId, dateHint }) {
  const origin = new URL(req.url).origin;
  const want = String(cellId || "").trim();
  if (!want) return null;

  // Build request to /winteam/shifts (optionally narrowed by dateHint)
  const body = { employeeNumber: String(employeeNumber || "").trim() };
  let dr = null;
  if (dateHint) {
    try {
      const d = new Date(dateHint);
      if (!isNaN(d)) {
        const toYmd = (x) => x.toISOString().slice(0, 10);
        const to    = new Date(d.getTime() + 24*60*60*1000);
        body.dateFrom = toYmd(d);
        body.dateTo   = toYmd(to);
        dr = `${body.dateFrom}→${body.dateTo}`;
      }
    } catch {}
  }

  // DEBUG: show what we’re about to request
  console.log("ZVA DEBUG fetchByCell: want cellId=", want, " dateHint=", dateHint || "(none)", " reqBody=", JSON.stringify(body));

  let r;
  try {
    r = await fetch(`${origin}/winteam/shifts`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });
  } catch (e) {
    console.log("ZVA DEBUG fetchByCell: request failed", { error: String(e) });
    return null;
  }

  if (!r.ok) {
    const raw = await r.text().catch(() => "");
    console.log("ZVA DEBUG fetchByCell: non-OK response", {
      status: r.status,
      ct: r.headers.get("content-type") || "",
      body: raw.slice(0, 300)
    });
    return null;
  }

  const ct = r.headers.get("content-type") || "";
  const raw = await r.text().catch(() => "");
  let j;
  try {
    j = /json/i.test(ct) ? JSON.parse(raw) : JSON.parse(raw);
  } catch (e) {
    console.log("ZVA DEBUG fetchByCell: failed to parse JSON", {
      status: r.status,
      ct,
      err: String(e),
      body: raw.slice(0, 300)
    });
    return null;
  }
  // Prefer full list; fallback to page list
  const arr = Array.isArray(j?.entries) ? j.entries
            : Array.isArray(j?.entries_page) ? j.entries_page
            : [];

  // DEBUG: show the cellIds we actually saw
  const seen = arr.map(e => String(e?.cellId ?? e?.id ?? e?.scheduleDetailID ?? "")).filter(Boolean);
  console.log("ZVA DEBUG fetchByCell: window=", dr || j?.window || "(default)", " count=", arr.length, " seenCellIds=", seen);

  // STRICT: only compare against the real cellId field
  const hit = arr.find(e => String(e?.cellId ?? "").trim() === want);

  console.log("ZVA DEBUG fetchByCell: match", hit ? "FOUND" : "NOT FOUND");

  if (!hit) return null;

  // Normalize what the writer needs
  return {
    cellId: String(hit.cellId).trim(),
    site: String(hit.site || hit.siteName || "").trim(),
    siteName: String(hit.site || hit.siteName || "").trim(),
    startLocalISO: String(hit.startLocalISO || hit.startIso || "").trim(),
    endLocalISO:   String(hit.endLocalISO   || hit.endIso   || "").trim()
  };
}

///////////////////////////////
// Router (module syntax, single export)
///////////////////////////////
export default {
  fetch: withLogging(async (req, env) => {
    const url = new URL(req.url);
    const { method } = req;
    const path = url.pathname;

    // CORS preflight
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

    if (method === "GET"  && path === "/debug/env")                 return await handleEnvDebug(req, env);

    if (method === "POST" && path === "/auth/employee")             return await handleAuthEmployee(req, env);
    if (method === "POST" && path === "/winteam/shifts")            return await handleShifts(req, env);
    if (method === "POST" && path === "/monday/write")              return await handleMondayWrite(req, env);

    if (method === "POST" && path === "/zva/shift-write")           return await handleZvaShiftWrite(req, env);
    if (method === "POST" && path === "/zva/shift-write-by-cell")   return await handleZvaShiftWriteByCell(req, env);

    return json({ success: false, message: "Not found. Use POST /auth/employee, /winteam/shifts, /monday/write" }, { status: 404 });
  })
};

