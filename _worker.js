// Cloudflare Worker: ZVA <-> WinTeam / Monday bridge (Option A + Monday column mapping)
//
// Endpoints:
//  - POST /auth/employee   : verify employeeNumber + ssnLast4; return select fields
//  - POST /winteam/shifts  : get shifts via shiftDetails; return speakable + entries + mondayShiftText
//  - POST /monday/write    : create Monday item + set columns; optional idempotency via KV (FLOW_GUARD)
//
// Required secrets: WINTEAM_TENANT_ID, WINTEAM_API_KEY, MONDAY_API_KEY
// Optional secrets: MONDAY_DEFAULT_BOARD_ID, FLOW_GUARD_TTL_SECONDS
// Optional KV binding: FLOW_GUARD
//
// Deploy URL example: https://secureonev2.rdyla.workers.dev

// ------------------- WinTeam endpoints -------------------
const EMPLOYEE_BASE =
  "http://apim.myteamsoftware.com/wtnextgen/employees/v1/api/employees";
const SHIFTS_BASE_EXACT =
  "http://apim.myteamsoftware.com/wtnextgen/schedules/v1/api/shiftDetails";

// ------------------- Monday.com endpoint -------------------
const MONDAY_API_URL = "https://api.monday.com/v2";

// ------------------- Monday column map (friendly → columnId) -------------------
const MONDAY_COLUMN_MAP = {
  // name column is set via itemName, not column values
  division: "color_mktd81zp",           // "Division:"
  department: "color_mktsk31h",         // "Department"
  site: "text_mktj4gmt",                // "Account/Site:"
  email: "email_mktdyt3z",              // "Email Address:"
  phone: "phone_mktdphra",              // "Phone Number:"
  callerId: "phone_mkv0p9q3",           // "Caller ID:"
  reason: "text_mktdb8pg",              // "Call Issue/Reason:"
  timeInOut: "text_mktsvsns",           // "Time In/Out:"
  startTime: "text_mkv0t29z",           // "Start Time (if applicable):"
  endTime: "text_mkv0nmq1",             // "End Time (if applicable)"
  dateTime: "date4",                    // "Date/Time:" (Monday date column)
  deptEmail: "text_mkv07gad",           // "Department Email:"
  emailStatus: "color_mkv0cpxc",        // "Email Status:"
  itemIdEcho: "pulse_id_mkv6rhgy",      // (usually auto; rarely set)
  zoomGuid: "text_mkv7j2fq",            // "Zoom Call GUID"
  shift: "text_mkwn6bzw",               // "Shift"
};

// ------------------- tiny utils -------------------
const json = (obj, { status = 200, cors = true } = {}) =>
  new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(cors
        ? {
            "access-control-allow-origin": "*",
            "access-control-allow-headers": "content-type, authorization",
            "access-control-allow-methods": "GET,POST,OPTIONS",
          }
        : {}),
    },
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

// Normalize US phone to +E.164 when possible
function normalizeUsPhone(raw) {
  if (!raw) return "";
  const s = String(raw);
  const d = s.replace(/\D+/g, "");
  if (d.length === 11 && d[0] === "1") return "+" + d;
  if (d.length === 10) return "+1" + d;
  if (/^\+\d{8,15}$/.test(s.trim())) return s.trim();
  return s.trim();
}

async function readJson(req) {
  try {
    return await req.json();
  } catch {
    return null;
  }
}

// ------------------- WinTeam helpers -------------------
function buildWTHeaders(TENANT_ID, API_KEY) {
  // EXACT header names per your APIM gateway
  return {
    tenantId: TENANT_ID,
    "Ocp-Apim-Subscription-Key": API_KEY,
    accept: "application/json",
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

// ------------------- Time formatting helpers -------------------
function parseNoTZToUTC(dateStr) {
  // "YYYY-MM-DDTHH:mm:ss" w/out TZ → treated as UTC on Workers
  const t = Date.parse(dateStr);
  return isNaN(t) ? null : new Date(t);
}
function addHours(d, hours) {
  const nd = new Date(d.getTime());
  nd.setUTCHours(nd.getUTCHours() + hours);
  return nd;
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
function ymdFromDate(d) { return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`; }

// ------------------- Monday helpers -------------------
async function mondayGraphQL(env, query, variables) {
  const token = env.MONDAY_API_KEY;
  if (!token) throw new Error("MONDAY_API_KEY not configured.");
  const r = await fetch(MONDAY_API_URL, {
    method: "POST",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  const j = await r.json();
  if (!r.ok || j.errors) {
    throw new Error(`Monday API error: ${r.status} ${JSON.stringify(j.errors || j)}`);
  }
  return j.data;
}

// Optional idempotency guard w/ KV
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

// ------------------- Handlers -------------------
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
    typeDescription: trim(match.typeDescription),
  };

  return json({ success: true, message: "Successful employee lookup.", employee: payload });
}

async function handleShifts(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

  const employeeNumber = trim(body.employeeNumber);
  if (!employeeNumber) {
    return json({ success: false, message: "employeeNumber is required." }, { status: 400 });
  }

  // Optional filter window for convenience (UTC yyyy-mm-dd); used for post-filtering
  const fromISO = trim(body.from) || ymd(new Date());
  const toISO   = trim(body.to)   || ymd(addDaysUTC(new Date(), 15));

  // Exact endpoint + param
  const url = new URL(SHIFTS_BASE_EXACT);
  url.searchParams.set("employeeNumber", employeeNumber);

  const wt = await callWinTeamJSON(url.toString(), env);
  if (!wt.ok) {
    return json(
      { success: false, message: `WinTeam shiftDetails failed (${wt.status}).`, detail: wt.error },
      { status: 502 }
    );
  }

  // Shape: data[0].results[*] => { jobDescription, postDescription, utCoffset, shifts: [{startTime, endTime, ...}] }
  const page = Array.isArray(wt.data?.data) ? wt.data.data[0] : null;
  const results = Array.isArray(page?.results) ? page.results : [];

  const rows = [];
  for (const r of results) {
    const site = trim(r.jobDescription);
    const role = trim(r.postDescription);
    const utcOffset = Number(r.utCoffset || 0);
    const list = Array.isArray(r.shifts) ? r.shifts : [];

    for (const s of list) {
      const startUTC = parseNoTZToUTC(s.startTime);
      let endUTC = parseNoTZToUTC(s.endTime);
      if (!startUTC || !endUTC) continue;

      // Convert to "local" using utCoffset: local = UTC + offsetHours
      const startLocal = addHours(startUTC, utcOffset);
      let endLocal = addHours(endUTC, utcOffset);

      // Overnight support
      if (endLocal.getTime() <= startLocal.getTime()) {
        endLocal = addHours(endLocal, 24);
      }

      const date1 = weekdayMonthDay(startLocal);
      const date2 = weekdayMonthDay(endLocal);
      const time1 = fmt12h(startLocal.getUTCHours(), startLocal.getUTCMinutes());
      const time2 = fmt12h(endLocal.getUTCHours(), endLocal.getUTCMinutes());

      // Concise for Monday
      const concise =
        `${ymdFromDate(startLocal)} ${pad2(startLocal.getUTCHours())}:${pad2(startLocal.getUTCMinutes())}` +
        ` → ${pad2(endLocal.getUTCHours())}:${pad2(endLocal.getUTCMinutes())}` +
        (site ? ` @ ${site}` : "") +
        (role ? ` (${role})` : "");

      // Speakable for agent
      const speakLine =
        `${date1}, ${time1} to ${date2}${date2 !== date1 ? "" : ""} ${time2}` +
        (site ? ` at ${site}` : "") +
        (role ? ` (${role})` : "");

      rows.push({
        employeeNumber: trim(r.employeeNumber || employeeNumber),
        site,
        role,
        utcOffset,
        hours: s.hours,
        hourType: trim(s.hourType),
        hourDescription: trim(s.hourDescription),
        scheduleDetailID: s.scheduleDetailID,
        startLocalISO: startLocal.toISOString(),
        endLocalISO: endLocal.toISOString(),
        speakLine,
        concise,
      });
    }
  }

  // Optional post-filter
  const fromT = Date.parse(fromISO + "T00:00:00Z");
  const toT   = Date.parse(toISO   + "T23:59:59Z");
  const filtered = rows.filter(r => {
    const t = Date.parse(r.startLocalISO);
    if (isNaN(t)) return true;
    return t >= fromT && t <= toT;
  });

  // Outputs for Option A
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
  const mondayShiftText = entries.length ? entries[0].concise : "";

  return json({
    success: true,
    message: "Shifts lookup completed.",
    query: { employeeNumber, from: fromISO, to: toISO },
    speakable,
    mondayShiftText,
    entries,
    raw: wt.data
  });
}

// Build column_values from either raw columnValues or friendly fields
function buildMondayColumnsFromFriendly(body) {
  const out = {};
  const addIf = (friendlyKey, transform = (v) => v) => {
    if (body[friendlyKey] != null && body[friendlyKey] !== "") {
      const colId = MONDAY_COLUMN_MAP[friendlyKey];
      if (colId) out[colId] = transform(body[friendlyKey]);
    }
  };

  // Strings are fine for most types. You can supply richer objects if desired.
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

  // Date column allows JSON like {date:"YYYY-MM-DD", time:"HH:MM:SS"}.
  // Accept plain strings too.
  if (body.dateTime) {
    const v = body.dateTime;
    if (typeof v === "object" && (v.date || v.time)) {
      out[MONDAY_COLUMN_MAP.dateTime] = v;
    } else {
      // try to parse ISO and split
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

  // Echo item id if you want (rare)
  if (body.itemIdEcho) out[MONDAY_COLUMN_MAP.itemIdEcho] = body.itemIdEcho;

  return out;
}

async function handleMondayWrite(req, env) {
  const body = await readJson(req);
  if (!body) return json({ success: false, message: "Invalid JSON body." }, { status: 400 });

  const boardId = Number(body.boardId || env.MONDAY_DEFAULT_BOARD_ID);
  const itemName = trim(body.itemName || body.name || "");
  const groupId = trim(body.groupId || "");
  const dedupeKey = trim(body.dedupeKey || body.engagementId || "");

  if (!boardId || !itemName) {
    return json({ success: false, message: "boardId and itemName are required." }, { status: 400 });
  }

  // Build columns:
  // 1) start with friendly field mapping
  const fromFriendly = buildMondayColumnsFromFriendly(body);
  // 2) merge any explicit columnValues (explicit wins)
  let explicit = body.columnValues && typeof body.columnValues === "object" ? body.columnValues : {};
  const columnValues = { ...fromFriendly, ...explicit };

  // Optional idempotency
  if (dedupeKey && (await flowGuardSeen(env, dedupeKey))) {
    return json({
      success: true,
      message: "Duplicate suppressed by flow guard.",
      dedupeKey,
      upserted: false,
      item: null,
      columnValues // echo back what we would have sent
    });
  }

  // 1) Create item
  const createMutation = `
    mutation ($boardId: Int!, $itemName: String!, $groupId: String) {
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
      groupId: groupId || null,
    });
    created = res.create_item;
  } catch (e) {
    return json({ success: false, message: "Monday create_item failed.", detail: String(e) }, { status: 502 });
  }

  // 2) Set columns (if any)
  if (Object.keys(columnValues).length) {
    const changeMutation = `
      mutation ($boardId: Int!, $itemId: Int!, $cv: JSON!) {
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
        cv: cvString,
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

// ------------------- Router -------------------
export default {
  async fetch(req, env) {
    const { method } = req;
    const url = new URL(req.url);
    const path = url.pathname;

    // CORS preflight
    if (method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-headers": "content-type, authorization",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-max-age": "86400",
        },
      });
    }

    try {
      if (method === "POST" && path === "/auth/employee") return await handleAuthEmployee(req, env);
      if (method === "POST" && path === "/winteam/shifts") return await handleShifts(req, env);
      if (method === "POST" && path === "/monday/write")  return await handleMondayWrite(req, env);

      return json(
        { success: false, message: "Not found. Use POST /auth/employee, /winteam/shifts, /monday/write" },
        { status: 404 }
      );
    } catch (e) {
      return json({ success: false, message: "Unhandled error.", detail: String(e) }, { status: 500 });
    }
  },
};
