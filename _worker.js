// Cloudflare Worker: ZVA <-> WinTeam / Monday bridge
//
// Endpoints:
//  - POST /auth/employee
//  - POST /winteam/shifts
//  - POST /monday/write
//  - GET  /debug/env           (optional: shows which bindings exist; no values)
//
// Required secrets: WINTEAM_TENANT_ID, WINTEAM_API_KEY, MONDAY_API_KEY
// Optional secrets: MONDAY_DEFAULT_BOARD_ID, FLOW_GUARD_TTL_SECONDS
// Optional KV binding: FLOW_GUARD
//
// wrangler.toml (example):
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
  division: "color_mktd81zp",     // "Division:"
  department: "color_mktsk31h",   // "Department"
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
  emailStatus: "color_mkv0cpxc",  // "Email Status:"
  itemIdEcho: "pulse_id_mkv6rhgy",
  zoomGuid: "text_mkv7j2fq",
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

// Central default (adjust if your tenant is in a different home timezone)
function nowCentral() {
  // naive approach: "now" in UTC minus 5 hours; good enough for a rolling window
  const d = new Date();
  d.setUTCHours(d.getUTCHours() - 5);
  return d;
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
// This avoids treating the naive string as real UTC and keeps comparisons consistent.
function parseNaiveAsLocalWall(isoNoTZ) {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(String(isoNoTZ));
  if (!m) return null;
  const [ , Y, M, D, h, min, s ] = m;
  return new Date(Date.UTC(
    Number(Y), Number(M) - 1, Number(D),
    Number(h), Number(min), Number(s || 0)
  ));
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
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

  const employeeNumber = trim(body.employeeNumber);
  const pageStart = Number(trim(body.pageStart ?? "0")) || 0;
  if (!employeeNumber) {
    return json({ success: false, message: "employeeNumber is required." }, { status: 400 });
  }

  // Always ask WinTeam for a 15-day window, based on "central" now
  const startBase = nowCentral();                 // tenant home tz anchor (tweak if needed)
  const fromDate = ymd(startBase);                // YYYY-MM-DD (today)
  const toDate   = ymd(addDaysUTC(startBase, 15)); // +15 days

  const url = new URL(SHIFTS_BASE_EXACT);
  url.searchParams.set("employeeNumber", employeeNumber);
  url.searchParams.set("fromDate", fromDate);   
  url.searchParams.set("toDate", toDate);       

  const wt = await callWinTeamJSON(url.toString(), env);
  if (!wt.ok) {
    return json(
      { success: false, message: `WinTeam shiftDetails failed (${wt.status}).`, detail: wt.error },
      { status: 502 }
    );
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
      let endLocal = parseNaiveAsLocalWall(s.endTime);
      if (!startLocal || !endLocal) continue;

      // Overnight support
      if (endLocal.getTime() <= startLocal.getTime()) {
        endLocal = new Date(endLocal.getTime() + 24 * 60 * 60 * 1000);
      }

      const date1 = weekdayMonthDay(startLocal);
      const date2 = weekdayMonthDay(endLocal);
      const time1 = fmt12h(startLocal.getUTCHours(), startLocal.getUTCMinutes());
      const time2 = fmt12h(endLocal.getUTCHours(), endLocal.getUTCMinutes());

      const concise =
        `${ymdFromDate(startLocal)} ${pad2(startLocal.getUTCHours())}:${pad2(startLocal.getUTCMinutes())}` +
        ` → ${pad2(endLocal.getUTCHours())}:${pad2(endLocal.getUTCMinutes())}` +
        (site ? ` @ ${site}` : "") + (role ? ` (${role})` : "");

      const speakLine =
        `${date1}, ${time1} to ${date2}${date2 !== date1 ? "" : ""} ${time2}` +
        (site ? ` at ${site}` : "") + (role ? ` (${role})` : "");

      rows.push({
        employeeNumber: trim(r.employeeNumber || employeeNumber),
        site,
        role,
        utcOffset,
        hours: s.hours,
        hourType: trim(s.hourType),
        hourDescription: trim(s.hourDescription),
        scheduleDetailID: s.scheduleDetailID,
        // local wall-clock frame
        startLocalISO: startLocal.toISOString(),
        endLocalISO: endLocal.toISOString(),
        speakLine,
        concise
      });
    }
  }

  // Always apply "now(local) → now+15d" window per row's utcOffset
  const nowUTC = new Date();
  const filtered = rows.filter(r => {
    const startT = Date.parse(r.startLocalISO);
    if (isNaN(startT)) return false;
    const offset = Number(r.utcOffset || 0);
    const nowLocal = new Date(nowUTC.getTime());
    nowLocal.setUTCHours(nowLocal.getUTCHours() + offset);
    const endLocal = new Date(nowLocal.getTime());
    endLocal.setUTCDate(endLocal.getUTCDate() + 15);
    return startT >= nowLocal.getTime() && startT <= endLocal.getTime();
  });

  // Sort by soonest start
  filtered.sort((a, b) => Date.parse(a.startLocalISO) - Date.parse(b.startLocalISO));

  // Server-side pagination (3 per page)
  const countTotal = filtered.length;
  const p = Math.max(0, pageStart | 0);
  const pageRows = filtered.slice(p, p + 3);
  const nextPageStart = p + 3 < countTotal ? p + 3 : p;
  const hasNext = p + 3 < countTotal;

  const speakable_page = pageRows.map(r => r.speakLine);
  const entries_page = pageRows.map(r => ({
    employeeNumber: r.employeeNumber,
    site: r.site,
    role: r.role,
    startLocalISO: r.startLocalISO,
    endLocalISO: r.endLocalISO,
    hours: r.hours,
    concise: r.concise,
    hourType: r.hourType,
    hourDescription: r.hourDescription,
    scheduleDetailID: r.scheduleDetailID
  }));

  const speakable = filtered.map(r => r.speakLine);
  const entries = filtered.map(r => ({
    employeeNumber: r.employeeNumber,
    site: r.site,
    role: r.role,
    startLocalISO: r.startLocalISO,
    endLocalISO: r.endLocalISO,
    hours: r.hours,
    concise: r.concise,
    hourType: r.hourType,
    hourDescription: r.hourDescription,
    scheduleDetailID: r.scheduleDetailID
  }));

  const mondayShiftText = entries[0]?.concise || "";

  return json({
    success: true,
    message: "Shifts lookup completed.",
    window: "now(local) → now+15d",
    counts: { rows: rows.length, filtered: countTotal },
    page: { pageStart: p, nextPageStart, hasNext, pageCount: pageRows.length },
    speakable_page,
    entries_page,
    // legacy full lists (if you’re still using them)
    speakable,
    mondayShiftText,
    entries,
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
  addIf("division");
  addIf("department");
  addIf("site");
  addIf("email");
  addIf("phone");
  addIf("callerId");
  addIf("reason");
  addIf("timeInOut");
  addIf("startTime");
  addIf("endTime");
  addIf("deptEmail");
  addIf("emailStatus");
  addIf("zoomGuid");
  addIf("shift");

  if (body.dateTime) {
    const v = body.dateTime;
    if (typeof v === "object" && (v.date || v.time)) {
      out[MONDAY_COLUMN_MAP.dateTime] = v;
    } else {
      const ts = Date.parse(String(v));
      if (!isNaN(ts)) {
        const d = new Date(ts);
        out[MONDAY_COLUMN_MAP.dateTime] = {
          date: `${d.getUTCFullYear()}-${pad2(d.getUTCMonth()+1)}-${pad2(d.getUTCDate())}`,
          time: `${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}:${pad2(d.getUTCSeconds())}`
        };
      } else {
        out[MONDAY_COLUMN_MAP.dateTime] = String(v);
      }
    }
  }
  if (body.itemIdEcho) out[MONDAY_COLUMN_MAP.itemIdEcho] = body.itemIdEcho;
  return out;
}

async function handleMondayWrite(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

// BEFORE
// const boardId = Number(body.boardId || env.MONDAY_DEFAULT_BOARD_ID);

// AFTER
const boardId = String(body.boardId || env.MONDAY_DEFAULT_BOARD_ID || "").trim();
if (!boardId) {
  return json({ success:false, message:"boardId is required (set MONDAY_DEFAULT_BOARD_ID or pass boardId)." }, { status: 400 });
}
  const itemName = trim(body.itemName || body.name || "");
  const groupId = trim(body.groupId || "");
  const dedupeKey = trim(body.dedupeKey || body.engagementId || "");

  if (!boardId || !itemName) {
    return json({ success: false, message: "boardId and itemName are required." }, { status: 400 });
  }

  const fromFriendly = buildMondayColumnsFromFriendly(body);
  let explicit = body.columnValues && typeof body.columnValues === "object" ? body.columnValues : {};
  const columnValues = { ...fromFriendly, ...explicit };

  if (dedupeKey && (await flowGuardSeen(env, dedupeKey))) {
    return json({
      success: true,
      message: "Duplicate suppressed by flow guard.",
      dedupeKey,
      upserted: false,
      item: null,
      columnValues
    });
  }

  // const createMutation = `
  //   mutation ($boardId: Int!, $itemName: String!, $groupId: String) {
  //     create_item (board_id: $boardId, item_name: $itemName, group_id: $groupId) {
  //       id
  //       name
  //       board { id }
  //       group { id }
  //     }
  //   }
  // `;

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
    const res = await mondayGraphQL(env, createMutation, {
      boardId,
      itemName,
      groupId: groupId || null
    });
    created = res.create_item;
  } catch (e) {
    return json({ success: false, message: "Monday create_item failed.", detail: String(e) }, { status: 502 });
  }

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
      await mondayGraphQL(env, changeMutation, {
        boardId,
        itemId: Number(created.id),
        cv: cvString
      });
    } catch (e) {
      return json(
        { success: false, message: "Monday change_multiple_column_values failed.", createdItem: created, detail: String(e) },
        { status: 502 }
      );
    }
  }

  if (dedupeKey) await flowGuardMark(env, dedupeKey);

  return json({
    success: true,
    message: "Monday item created/updated.",
    boardId,
    item: created,
    dedupeKey: dedupeKey || null,
    columnValuesSent: columnValues
  });
}

// Optional: env presence check (no secret values)
async function handleEnvDebug(req, env) {
  const present = (k) => !!env[k];
  return json({
    nameHint: "Verify this matches the worker you're deploying",
    has: {
      WINTEAM_TENANT_ID: present("WINTEAM_TENANT_ID"),
      WINTEAM_API_KEY: present("WINTEAM_API_KEY"),
      MONDAY_API_KEY: present("MONDAY_API_KEY"),
      MONDAY_DEFAULT_BOARD_ID: present("MONDAY_DEFAULT_BOARD_ID"),
      FLOW_GUARD: !!env.FLOW_GUARD
    }
  });
}

///////////////////////////////
// Router (with logging)
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

    if (method === "GET"  && path === "/debug/env")       return await handleEnvDebug(req, env);
    if (method === "POST" && path === "/auth/employee")   return await handleAuthEmployee(req, env);
    if (method === "POST" && path === "/winteam/shifts")  return await handleShifts(req, env);
    if (method === "POST" && path === "/monday/write")    return await handleMondayWrite(req, env);

    return json({ success: false, message: "Not found. Use POST /auth/employee, /winteam/shifts, /monday/write" }, { status: 404 });
  })
};
