// =============================================================================
//  Yomi（月讀）— 伺服器
//  純 Node.js HTTP、無外部依賴。Pipeline：上傳音檔 → nlm 上傳 NotebookLM
//  → 取逐字稿 → query 產生摘要與反問問題 → SSE 推進度回前端
//
//  資料模型：1 meeting（卷宗）= 1 NotebookLM notebook，底下有 records[]（場次）。
//  每個 record 各自有 sources[] + summary + stage 等。新場次共用同一個 notebook，
//  但摘要 prompt 會限制只看自己的 source。
// =============================================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { SUMMARY_PROMPT, SYNC_SUMMARY_PROMPT, CLASS_SUMMARY_PROMPT, TITLE_PROMPT } = require('./prompts.js');

// 依卷宗模式挑摘要 prompt。預設 meeting（向下相容舊資料）
function normalizeMeetingMode(mode) {
  const m = String(mode || '').toLowerCase();
  return ['meeting', 'sync', 'class'].includes(m) ? m : 'meeting';
}

function summaryPromptFor(meeting) {
  switch (normalizeMeetingMode(meeting && meeting.mode)) {
    case 'class': return CLASS_SUMMARY_PROMPT;
    case 'sync': return SYNC_SUMMARY_PROMPT;
    default: return SUMMARY_PROMPT;
  }
}

// 把 record 內 sources 的標題列出來，前綴給 NotebookLM 限定範圍
function scopedPromptFor(meeting, record) {
  const titles = (record.sources || [])
    .map(s => s.title)
    .filter(t => typeof t === 'string' && t.trim());
  const base = summaryPromptFor(meeting);
  if (titles.length === 0) return base;
  const list = titles.map(t => `「${t}」`).join('、');
  return `本卷宗可能有多筆來源（source），請只根據以下指定的來源來回答，其餘來源請完全忽略：\n${list}\n\n---\n\n${base}`;
}

const PORT = Number(process.env.PORT || 3748);
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, 'meetings-data.json');
const RECORDINGS_DIR = path.join(ROOT, 'recordings');
const NLM_BIN = process.env.NLM_BIN || 'nlm';

const state = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
const save = () => fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));

const TERMINAL_STAGES = new Set(['done', 'error', 'cancelled']);

// ---------- 啟動恢復 + 結構升級 ----------
// 1) 把舊 meeting（sources/summary/stage 等都在 meeting 上）遷移成 records[0]
// 2) 把中斷的 record 標成 error（伺服器重啟導致工作中斷）
// 3) 補上 records[].id（舊格式 source 也補 title 預設值，後面 scopedPromptFor 才有東西列）
let recovered = 0;
let mutated = false;
for (const m of state.meetings) {
  if (!Array.isArray(m.records)) {
    const r0 = {
      id: 0,
      title: m.title || '會議記錄 1',
      createdAt: m.createdAt,
      stage: m.stage || 'done',
      stageMsg: m.stageMsg,
      completedAt: m.completedAt,
      error: m.error,
      appendError: m.appendError,
      sources: Array.isArray(m.sources) ? m.sources : [],
      summary: m.summary,
    };
    m.records = [r0];
    delete m.sources;
    delete m.summary;
    delete m.stage;
    delete m.stageMsg;
    delete m.completedAt;
    delete m.error;
    delete m.appendError;
    delete m.cancelled;
    mutated = true;
  }
  for (const r of m.records) {
    if (!TERMINAL_STAGES.has(r.stage)) {
      r.stage = 'error';
      r.stageMsg = '伺服器重啟導致工作中斷，請刪除後重新上傳';
      r.error = r.stageMsg;
      recovered++;
      mutated = true;
    }
    // 補預設 source title，scopedPromptFor 才列得出來
    for (let i = 0; i < (r.sources || []).length; i++) {
      const s = r.sources[i];
      if (!s.title) {
        s.title = defaultSourceTitle(s, r, i);
        mutated = true;
      }
    }
  }
}
if (mutated) save();
if (recovered) console.log(`已恢復 ${recovered} 筆中斷的場次為 error`);

// 追蹤每個 record 當下 spawn 出去的 child process（取消用）。key = `${meetingId}:${recordId}`
const activeProcs = {};
// 追蹤哪些 record 正在續錄／重新摘要中（失敗時要回滾到 done 而非標 error）
const busyRecords = new Set();
function procKey(meetingId, recordId) { return `${meetingId}:${recordId}`; }

// ---------- SSE ----------
const sseClients = {};
function broadcast(meetingId, event, data) {
  const list = sseClients[meetingId] || [];
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of list) { try { res.write(payload); } catch {} }
}
function pushStage(meeting, record, stage, msg) {
  record.stage = stage;
  record.stageMsg = msg;
  save();
  broadcast(meeting.id, 'stage', { recordId: record.id, stage, msg });
}

// ---------- nlm wrappers ----------
function run(cmd, args, { input, onProc } = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    if (onProc) onProc(p);
    let out = '', err = '';
    p.stdout.on('data', d => out += d.toString());
    p.stderr.on('data', d => err += d.toString());
    p.on('close', (code, signal) => {
      if (code === 0) resolve({ stdout: out, stderr: err });
      else reject(new Error(`${cmd} ${args.join(' ')} → exit ${code ?? 'signal:' + signal}\n${err || out}`));
    });
    p.on('error', reject);
    if (input) p.stdin.end(input);
    else p.stdin.end();
  });
}

function runForRecord(meetingId, recordId, cmd, args, opts = {}) {
  const key = procKey(meetingId, recordId);
  return run(cmd, args, {
    ...opts,
    onProc: p => { activeProcs[key] = p; }
  }).finally(() => {
    delete activeProcs[key];
  });
}

function extractField(stdout, fields) {
  const s = stdout.trim();
  try {
    const j = JSON.parse(s);
    const candidates = [j, j?.value, Array.isArray(j) ? j[0] : null, Array.isArray(j) ? j[0]?.value : null];
    for (const c of candidates) {
      if (!c) continue;
      for (const f of fields) {
        if (typeof c[f] === 'string') return c[f].trim();
      }
    }
  } catch {}
  return s;
}
const extractAnswer = s => extractField(s, ['answer']);
const extractContent = s => extractField(s, ['content', 'transcript', 'text']);

function parseId(stdout) {
  const s = stdout.trim();
  try {
    const j = JSON.parse(s);
    if (j.id) return j.id;
    if (Array.isArray(j) && j[0]?.id) return j[0].id;
  } catch {}
  const m = s.match(/\b[a-f0-9-]{20,}\b/i);
  return m ? m[0] : null;
}

// ---------- Pipeline ----------
function checkCancelled(record) {
  if (record.cancelled) throw new Error('__CANCELLED__');
}

function friendlyError(raw) {
  if (/no.*language|language.*not.*detected|no.*content|偵測不到|沒有內容/i.test(raw)) {
    return 'NotebookLM 無法從音檔偵測到語言內容（可能太短、太雜訊、或未說話）。請重新錄音或上傳。\n\n原始訊息：\n' + raw;
  }
  if (/timeout|逾時/i.test(raw)) {
    return 'NotebookLM 處理逾時。可能音檔過長或服務繁忙，請稍後重試。\n\n原始訊息：\n' + raw;
  }
  return raw;
}

const KIND_UPLOAD_MSG = {
  audio:   '上傳音檔到 NotebookLM（請耐心等候，1 小時錄音約需 5 分鐘）…',
  file:    '上傳檔案到 NotebookLM…',
  text:    '加入文字稿到 NotebookLM…',
  url:     '抓取網頁內容…',
  youtube: '處理 YouTube 影片…',
  drive:   '加入 Google Drive 來源…',
};
const KIND_TRANSCRIBE_MSG = {
  audio:   '取得逐字稿…',
  file:    '取得檔案內容…',
  url:     '取得網頁內容…',
  youtube: '取得 YouTube 文字稿…',
  drive:   '取得 Drive 文件內容…',
};

// 預設 source 標題（給 scopedPromptFor 用、也給 nlm 端顯示）
function defaultSourceTitle(src, record, sourceIndex) {
  const kind = src.kind || 'audio';
  const base = record.title || '場次';
  const tag = (kind === 'text') ? '文字稿'
            : (kind === 'url') ? '網頁'
            : (kind === 'youtube') ? 'YouTube'
            : (kind === 'drive') ? 'Drive'
            : '錄音';
  return `${base}・${tag} ${sourceIndex + 1}`;
}

// 把一段 source 加進既有 notebook、取內容、回填。
async function ingestSource(meeting, record, src, opts = {}) {
  const kind = src.kind || 'audio';
  const isText = kind === 'text';
  const uploadStage = opts.uploadStage || 'uploading';
  const uploadMsg = opts.uploadMsg || KIND_UPLOAD_MSG[kind] || KIND_UPLOAD_MSG.audio;
  const transcribeMsg = opts.transcribeMsg || KIND_TRANSCRIBE_MSG[kind] || KIND_TRANSCRIBE_MSG.audio;

  checkCancelled(record);
  pushStage(meeting, record, uploadStage, uploadMsg);

  let addArgs;
  if (kind === 'text') {
    addArgs = ['source', 'add', meeting.notebookId, '--text', src.text || ''];
  } else if (kind === 'url') {
    addArgs = ['source', 'add', meeting.notebookId, '--url', src.url || ''];
  } else if (kind === 'youtube') {
    addArgs = ['source', 'add', meeting.notebookId, '--youtube', src.url || ''];
  } else if (kind === 'drive') {
    addArgs = ['source', 'add', meeting.notebookId, '--drive', src.driveId || ''];
    if (src.driveType) addArgs.push('--type', src.driveType);
  } else {
    const audioPath = path.join(RECORDINGS_DIR, src.audioFile);
    addArgs = ['source', 'add', meeting.notebookId, '--file', audioPath];
  }
  addArgs.push('--title', src.title);
  addArgs.push('--wait', '--wait-timeout', '1800');

  const addRes = await runForRecord(meeting.id, record.id, NLM_BIN, addArgs);
  src.sourceId = parseId(addRes.stdout);
  if (isText) src.transcript = src.text;
  save();

  if (!src.sourceId) {
    const raw = (addRes.stdout || addRes.stderr || '').trim();
    throw new Error(
      '上傳到 NotebookLM 完成但無法解析 source ID，可能上傳失敗或服務回應異常。\n\n原始回應：\n' +
      (raw.slice(0, 800) || '(無輸出)')
    );
  }

  if (!isText) {
    checkCancelled(record);
    pushStage(meeting, record, 'transcribing', transcribeMsg);
    let transcribeError = null;
    try {
      const tRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['source', 'get', src.sourceId]);
      src.transcript = extractContent(tRes.stdout);
    } catch (e) {
      transcribeError = e;
      src.transcript = `(無法取得內容: ${e.message})`;
    }
    save();

    const trimmed = (src.transcript || '').trim();
    const empty = !trimmed || /^\(無法取得內容/.test(trimmed);
    if ((kind === 'audio' || kind === 'file') && empty) {
      throw new Error(
        'NotebookLM 沒有從此來源取得任何內容。可能音檔／檔案無聲、太短、格式不支援，' +
        '或 NotebookLM 端處理失敗。請確認來源有效後重試。' +
        (transcribeError ? `\n\n原始訊息：\n${transcribeError.message}` : '')
      );
    }
  }
}

function validateSummary(stdout) {
  const summary = stripNotebookCitations(extractAnswer(stdout));
  const trimmed = (summary || '').trim();
  if (!trimmed || trimmed.length < 20) {
    throw new Error(
      'NotebookLM 沒有回傳有效摘要（內容為空或過短），可能來源未被正確處理或服務暫時不可用。\n\n原始回應：\n' +
      ((stdout || '').slice(0, 800) || '(空)')
    );
  }
  return summary;
}

function stripNotebookCitations(text) {
  return String(text || '')
    .replace(/\s*\[(?:\d+\s*(?:,\s*\d+\s*)*)\]/g, '')
    .replace(/[ \t]+$/gm, '')
    .trim();
}

// NLM 回的標題常會夾雜引號、前綴、結尾標點，洗乾淨再覆寫
function sanitizeTitle(raw) {
  if (!raw) return '';
  let s = String(raw).trim();
  s = s.split(/\r?\n/)[0].trim();
  s = s.replace(/^(?:標題|題目|筆記本名稱|Title)\s*[：:\-—]\s*/i, '');
  s = s.replace(/^[\s"'「『《\[【〈]+/, '').replace(/[\s"'」』》\]】〉]+$/, '');
  s = s.replace(/[。．！？\?]+$/, '');
  s = s.replace(/\s+/g, ' ').trim();
  if (s.length > 60) s = s.slice(0, 60).trim();
  return s;
}

// 由 NLM 依卷宗內容回一個標題，覆寫 meeting.title 並同步重命名 NLM notebook。
// 完全 best-effort：任何錯誤都不影響主流程，留下原本的時間戳標題即可。
async function autoTitleMeeting(meeting, record) {
  if (!meeting.notebookId) return;
  try {
    const tRes = await runForRecord(
      meeting.id, record.id, NLM_BIN,
      ['notebook', 'query', meeting.notebookId, TITLE_PROMPT]
    );
    const title = sanitizeTitle(extractAnswer(tRes.stdout));
    if (!title) return;
    meeting.title = title;
    save();
    try {
      await run(NLM_BIN, ['notebook', 'rename', meeting.notebookId, title]);
    } catch (e) {
      console.warn('[autoTitle] NLM 同步重命名失敗（本地已更新）:', String(e.message || e).slice(0, 200));
    }
  } catch (e) {
    console.warn('[autoTitle] 取得自動標題失敗:', String(e.message || e).slice(0, 200));
  }
}

// 第一場 record：要先建 notebook 再 ingest source 再產摘要
async function processFirstRecord(meeting) {
  const record = meeting.records[0];
  try {
    checkCancelled(record);
    pushStage(meeting, record, 'creating', '建立 NotebookLM 筆記中…');
    const createRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['notebook', 'create', meeting.title]);
    const notebookId = parseId(createRes.stdout);
    if (!notebookId) throw new Error('無法解析 notebook id: ' + createRes.stdout);
    meeting.notebookId = notebookId;
    save();

    await ingestSource(meeting, record, record.sources[0]);

    checkCancelled(record);
    pushStage(meeting, record, 'summarizing', '產生摘要與可詢問議題…');
    const qRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['notebook', 'query', notebookId, scopedPromptFor(meeting, record)]);
    record.summary = validateSummary(qRes.stdout);
    record.completedAt = new Date().toISOString();

    // 摘要產完後，由 NLM 依內容自動命名卷宗（覆寫掉預設的時間戳標題）
    pushStage(meeting, record, 'summarizing', '由月讀為此卷命名…');
    await autoTitleMeeting(meeting, record);

    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'done', { recordId: record.id, meeting });
  } catch (e) {
    const raw = String(e.message || e);
    if (raw.includes('__CANCELLED__') || record.cancelled) {
      pushStage(meeting, record, 'cancelled', '已取消');
      broadcast(meeting.id, 'cancelled', { recordId: record.id, msg: '已取消' });
      return;
    }
    const friendly = friendlyError(raw);
    record.error = friendly;
    pushStage(meeting, record, 'error', friendly);
    broadcast(meeting.id, 'error', { recordId: record.id, msg: friendly });
  }
}

// 在既有卷宗（notebook 已建）開新一場 record：ingest 第一份 source、產這場的摘要
async function processNewRecord(meeting, record) {
  try {
    if (!meeting.notebookId) throw new Error('此卷宗尚未建立 NotebookLM 筆記，無法新增場次');
    await ingestSource(meeting, record, record.sources[0]);

    checkCancelled(record);
    pushStage(meeting, record, 'summarizing', '產生本場摘要…');
    const qRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['notebook', 'query', meeting.notebookId, scopedPromptFor(meeting, record)]);
    record.summary = validateSummary(qRes.stdout);
    record.completedAt = new Date().toISOString();
    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'done', { recordId: record.id, meeting });
  } catch (e) {
    const raw = String(e.message || e);
    if (raw.includes('__CANCELLED__') || record.cancelled) {
      pushStage(meeting, record, 'cancelled', '已取消');
      broadcast(meeting.id, 'cancelled', { recordId: record.id, msg: '已取消' });
      return;
    }
    const friendly = friendlyError(raw);
    record.error = friendly;
    pushStage(meeting, record, 'error', friendly);
    broadcast(meeting.id, 'error', { recordId: record.id, msg: friendly });
  }
}

// 把新一段 source 加入指定 record，並重生這場摘要。失敗時回滾到原 done 狀態
async function appendSourceToRecord(meeting, record, sourceIndex) {
  const src = record.sources[sourceIndex];
  const previousSummary = record.summary;
  const previousCompletedAt = record.completedAt;
  busyRecords.add(procKey(meeting.id, record.id));
  try {
    if (!meeting.notebookId) throw new Error('此卷宗尚未建立 NotebookLM 筆記，無法續錄');
    await ingestSource(meeting, record, src, {
      uploadStage: 'appending',
      uploadMsg: `續錄第 ${sourceIndex + 1} 段來源到本場…`,
      transcribeMsg: `取得第 ${sourceIndex + 1} 段內容…`,
    });

    checkCancelled(record);
    pushStage(meeting, record, 'summarizing', '重新整合本場所有來源產生摘要…');
    const qRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['notebook', 'query', meeting.notebookId, scopedPromptFor(meeting, record)]);
    record.summary = validateSummary(qRes.stdout);
    record.completedAt = new Date().toISOString();
    delete record.appendError;
    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'done', { recordId: record.id, meeting });
  } catch (e) {
    const raw = String(e.message || e);
    const wasCancel = raw.includes('__CANCELLED__') || record.cancelled;
    const friendly = wasCancel ? '已取消續錄' : friendlyError(raw);

    const ingested = !!src.sourceId;
    const failedAtSummary = ingested && record.stage === 'summarizing';

    if (failedAtSummary) {
      record.appendError = `第 ${sourceIndex + 1} 段已加入，但摘要重整失敗：${friendly}`;
    } else {
      const removed = record.sources.splice(sourceIndex, 1)[0];
      if (removed && removed.audioFile) {
        try { fs.unlinkSync(path.join(RECORDINGS_DIR, removed.audioFile)); } catch {}
      }
      record.appendError = wasCancel
        ? '已取消續錄；新增的來源未保留。'
        : `續錄失敗（已回到上次成功的狀態）：${friendly}`;
    }
    record.summary = previousSummary;
    record.completedAt = previousCompletedAt;
    record.cancelled = false;
    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'append-error', { recordId: record.id, msg: record.appendError, meeting });
  } finally {
    busyRecords.delete(procKey(meeting.id, record.id));
  }
}

// 用既有 notebook 重跑某 record 的摘要
async function resummarizeRecord(meeting, record) {
  const previousSummary = record.summary;
  const previousCompletedAt = record.completedAt;
  busyRecords.add(procKey(meeting.id, record.id));
  try {
    if (!meeting.notebookId) throw new Error('此卷宗尚未建立 NotebookLM 筆記，無法重新摘要');
    checkCancelled(record);
    pushStage(meeting, record, 'summarizing', '重新摘要中…');
    const qRes = await runForRecord(meeting.id, record.id, NLM_BIN, ['notebook', 'query', meeting.notebookId, scopedPromptFor(meeting, record)]);
    record.summary = validateSummary(qRes.stdout);
    record.completedAt = new Date().toISOString();
    delete record.appendError;
    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'done', { recordId: record.id, meeting });
  } catch (e) {
    const raw = String(e.message || e);
    const wasCancel = raw.includes('__CANCELLED__') || record.cancelled;
    const friendly = wasCancel ? '已取消重新摘要' : friendlyError(raw);
    record.summary = previousSummary;
    record.completedAt = previousCompletedAt;
    record.cancelled = false;
    record.appendError = wasCancel ? '已取消重新摘要；保留先前摘要。' : `重新摘要失敗：${friendly}`;
    pushStage(meeting, record, 'done', '完成');
    broadcast(meeting.id, 'append-error', { recordId: record.id, msg: record.appendError, meeting });
  } finally {
    busyRecords.delete(procKey(meeting.id, record.id));
  }
}

// ---------- 建立 helpers ----------
function defaultMeetingTitle(kind, payload) {
  const now = new Date();
  const ts = now.toLocaleString('zh-TW', { hour12: false }).replace(/\//g, '-');
  if (kind === 'text') return `紀錄 ${ts}`;
  if (kind === 'url') {
    try { return `網頁 · ${new URL(payload.url).hostname}`; } catch { return `網頁 ${ts}`; }
  }
  if (kind === 'youtube') return `YouTube ${ts}`;
  if (kind === 'drive') return `Drive · ${(payload.driveId || '').slice(0, 10)}`;
  return `會議 ${ts}`;
}

function buildSourceEntry(kind, payload) {
  const addedAt = new Date().toISOString();
  if (kind === 'text') {
    return { kind: 'text', text: payload.text, transcript: payload.text, audioSize: Buffer.byteLength(payload.text, 'utf8'), addedAt };
  }
  if (kind === 'url') return { kind: 'url', url: payload.url, addedAt };
  if (kind === 'youtube') return { kind: 'youtube', url: payload.url, addedAt };
  if (kind === 'drive') return { kind: 'drive', driveId: payload.driveId, driveType: payload.driveType || 'doc', addedAt };
  return { kind: 'audio', audioFile: payload.audioFile, audioSize: payload.audioSize, addedAt };
}

// 為新 meeting 建立第一場 record + 第一個 source
function createMeetingFor(id, kind, payload, mode) {
  const now = new Date();
  const addedAt = now.toISOString();
  const source = buildSourceEntry(kind, payload);
  const record = {
    id: 0,
    title: '會議記錄 1',
    createdAt: addedAt,
    stage: 'queued',
    sources: [source],
  };
  source.title = defaultSourceTitle(source, record, 0);
  const meeting = {
    id,
    title: defaultMeetingTitle(kind, payload),
    mode: normalizeMeetingMode(mode),
    createdAt: addedAt,
    records: [record],
  };
  state.meetings.unshift(meeting);
  save();
  return meeting;
}

// 在既有 meeting 加新一場 record + 第一個 source
function createRecordFor(meeting, kind, payload) {
  const addedAt = new Date().toISOString();
  const maxId = meeting.records.reduce((acc, r) => Math.max(acc, r.id), -1);
  const recordId = maxId + 1;
  const title = `會議記錄 ${meeting.records.length + 1}`;
  const source = buildSourceEntry(kind, payload);
  const record = {
    id: recordId,
    title,
    createdAt: addedAt,
    stage: 'queued',
    sources: [source],
  };
  source.title = defaultSourceTitle(source, record, 0);
  meeting.records.push(record);
  save();
  return record;
}

// ---------- HTTP ----------
function sendJson(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

// 解析 source body 為對應的 payload；失敗則回 {error}
function parseSourcePayload(buf, headers, meetingId, recordId, sourceIndex) {
  const kind = (headers['x-source-type'] || 'audio').toLowerCase();
  if (kind === 'text') {
    const text = buf.toString('utf8').trim();
    if (!text) return { error: '文字內容不能為空' };
    if (text.length > 800000) return { error: '文字稿過長（上限約 80 萬字元），請分段上傳' };
    return { kind: 'text', payload: { text } };
  }
  if (kind === 'url' || kind === 'youtube') {
    const u = buf.toString('utf8').trim();
    if (!u) return { error: '網址不能為空' };
    if (!/^https?:\/\//i.test(u)) return { error: '請提供有效的 http(s) 網址' };
    return { kind, payload: { url: u } };
  }
  if (kind === 'drive') {
    const driveId = buf.toString('utf8').trim();
    if (!driveId) return { error: 'Drive 文件 ID 不能為空' };
    const driveType = (headers['x-drive-type'] || 'doc').toLowerCase().replace(/[^a-z]/g, '') || 'doc';
    return { kind: 'drive', payload: { driveId, driveType } };
  }
  // audio / file。檔名帶 meeting + record + source index 避免衝突
  const ext = String(headers['x-audio-ext'] || 'm4a').replace(/[^a-z0-9]/gi, '') || 'm4a';
  const tag = recordId == null ? 'init' : `r${recordId}`;
  const audioFile = sourceIndex == null
    ? `${meetingId}-${tag}.${ext}`
    : `${meetingId}-${tag}-${sourceIndex}.${ext}`;
  fs.writeFileSync(path.join(RECORDINGS_DIR, audioFile), buf);
  return { kind: 'audio', payload: { audioFile, audioSize: buf.length } };
}

function readBody(req, maxBytes, cb) {
  const chunks = [];
  let total = 0;
  req.on('data', c => {
    total += c.length;
    if (total > maxBytes) { req.destroy(); return; }
    chunks.push(c);
  });
  req.on('end', () => cb(Buffer.concat(chunks)));
}

// meeting 整體狀態：取最後一場 record 的 stage 當代表（用來在 sidebar 顯示）
function meetingAggregateStage(m) {
  const rs = m.records || [];
  if (rs.length === 0) return { stage: 'error', stageMsg: '無場次' };
  const last = rs[rs.length - 1];
  return { stage: last.stage, stageMsg: last.stageMsg };
}

function findMeeting(id) {
  return state.meetings.find(x => x.id === id);
}
function findRecord(m, rid) {
  return m && m.records ? m.records.find(r => r.id === rid) : null;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(path.join(ROOT, 'public/index.html')));
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/health') {
    return sendJson(res, 200, {
      ok: true,
      pid: process.pid,
      uptime: Math.floor(process.uptime()),
      activePipelines: Object.keys(activeProcs).length
    });
  }

  if (req.method === 'POST' && url.pathname === '/api/restart') {
    sendJson(res, 200, { ok: true });
    setTimeout(() => {
      const child = spawn('bash', ['-lc', 'sleep 0.5 && cd "' + ROOT + '" && npm run restart'], {
        detached: true,
        stdio: 'ignore'
      });
      child.unref();
      setTimeout(() => process.exit(0), 300);
    }, 100);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/meetings') {
    return sendJson(res, 200, state.meetings.map(m => {
      const agg = meetingAggregateStage(m);
      const recs = m.records || [];
      return {
        id: m.id, title: m.title,
        mode: m.mode || 'meeting',
        createdAt: m.createdAt,
        stage: agg.stage, stageMsg: agg.stageMsg,
        recordCount: recs.length,
        records: recs.map((r, i) => ({
          id: r.id,
          title: r.title || `第 ${i + 1} 場`,
          stage: r.stage,
        })),
      };
    }));
  }

  const idMatch = url.pathname.match(/^\/api\/meetings\/(\d+)$/);
  if (req.method === 'GET' && idMatch) {
    const m = findMeeting(Number(idMatch[1]));
    if (!m) return sendJson(res, 404, { error: 'not found' });
    return sendJson(res, 200, m);
  }

  if (req.method === 'PATCH' && idMatch) {
    const id = Number(idMatch[1]);
    const m = findMeeting(id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    readBody(req, 64 * 1024, async (buf) => {
      let body;
      try { body = JSON.parse(buf.toString('utf8') || '{}'); }
      catch { return sendJson(res, 400, { error: 'invalid json' }); }
      const title = String(body.title || '').trim();
      if (!title) return sendJson(res, 400, { error: '標題不可為空' });
      m.title = title;
      save();
      if (m.notebookId) {
        try {
          await run(NLM_BIN, ['notebook', 'rename', m.notebookId, title]);
          sendJson(res, 200, { meeting: m, synced: true });
        } catch (e) {
          const msg = String(e.message || e);
          if (/NOT_FOUND/i.test(msg)) {
            // NotebookLM 上找不到對應筆記本（被手動刪掉或帳號換過），把 stale id 清掉
            m.notebookId = null;
            save();
            sendJson(res, 200, { meeting: m, synced: false, notebookMissing: true });
          } else {
            sendJson(res, 200, { meeting: m, synced: false, warning: msg });
          }
        }
      } else {
        sendJson(res, 200, { meeting: m, synced: false });
      }
    });
    return;
  }

  if (req.method === 'DELETE' && idMatch) {
    const id = Number(idMatch[1]);
    const idx = state.meetings.findIndex(x => x.id === id);
    if (idx === -1) return sendJson(res, 404, { error: 'not found' });
    const m = state.meetings[idx];

    // 取消所有還在跑的 record
    for (const r of m.records || []) {
      if (!TERMINAL_STAGES.has(r.stage)) {
        r.cancelled = true;
        const proc = activeProcs[procKey(id, r.id)];
        if (proc && !proc.killed) { try { proc.kill('SIGTERM'); } catch {} }
      }
    }

    state.meetings.splice(idx, 1);
    save();
    for (const r of m.records || []) {
      for (const src of r.sources || []) {
        if (src.audioFile) {
          try { fs.unlinkSync(path.join(RECORDINGS_DIR, src.audioFile)); } catch {}
        }
      }
    }

    const notebookId = m.notebookId;
    if (notebookId) {
      run(NLM_BIN, ['notebook', 'delete', notebookId, '--confirm'])
        .then(() => sendJson(res, 200, { ok: true, nlmDeleted: true }))
        .catch(e => sendJson(res, 200, { ok: true, nlmDeleted: false, warning: String(e.message || e) }));
      return;
    }
    return sendJson(res, 200, { ok: true, nlmDeleted: false });
  }

  // ---------- 建立卷宗 + 第一場 record ----------
  if (req.method === 'POST' && url.pathname === '/api/meetings') {
    readBody(req, 500 * 1024 * 1024, (buf) => {
      const id = state.nextId++;
      const parsed = parseSourcePayload(buf, req.headers, id, 0, null);
      if (parsed.error) return sendJson(res, 400, { error: parsed.error });
      const mode = String(req.headers['x-meeting-mode'] || '').toLowerCase();
      const meeting = createMeetingFor(id, parsed.kind, parsed.payload, mode);
      sendJson(res, 200, { id, meeting });
      processFirstRecord(meeting);
    });
    return;
  }

  // ---------- 開新一場 record（在既有 meeting） ----------
  const recordsMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records$/);
  if (req.method === 'POST' && recordsMatch) {
    const id = Number(recordsMatch[1]);
    const m = findMeeting(id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    if (!m.notebookId) return sendJson(res, 400, { error: '此卷宗尚未建立 NotebookLM 筆記，無法新增場次' });
    readBody(req, 500 * 1024 * 1024, (buf) => {
      const tempId = m.records.length;
      const parsed = parseSourcePayload(buf, req.headers, id, tempId, null);
      if (parsed.error) return sendJson(res, 400, { error: parsed.error });
      const record = createRecordFor(m, parsed.kind, parsed.payload);
      sendJson(res, 200, { meeting: m, recordId: record.id });
      processNewRecord(m, record);
    });
    return;
  }

  // ---------- 取消（指定 record） ----------
  const cancelMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)\/cancel$/);
  if (req.method === 'POST' && cancelMatch) {
    const id = Number(cancelMatch[1]);
    const rid = Number(cancelMatch[2]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    if (TERMINAL_STAGES.has(r.stage)) {
      return sendJson(res, 200, { ok: true, alreadyDone: true });
    }
    r.cancelled = true;
    const proc = activeProcs[procKey(id, rid)];
    if (proc && !proc.killed) {
      try { proc.kill('SIGTERM'); } catch {}
      setTimeout(() => { try { if (!proc.killed) proc.kill('SIGKILL'); } catch {} }, 2000);
    }
    if (!busyRecords.has(procKey(id, rid))) {
      pushStage(m, r, 'cancelled', '已取消');
      broadcast(id, 'cancelled', { recordId: rid, msg: '已取消' });
    }
    return sendJson(res, 200, { ok: true });
  }

  const clearErrMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)\/clear-append-error$/);
  if (req.method === 'POST' && clearErrMatch) {
    const id = Number(clearErrMatch[1]);
    const rid = Number(clearErrMatch[2]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    delete r.appendError;
    save();
    return sendJson(res, 200, { ok: true, meeting: m });
  }

  // ---------- record 改名 ----------
  const recordItemMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)$/);
  if (recordItemMatch && req.method === 'PATCH') {
    const id = Number(recordItemMatch[1]);
    const rid = Number(recordItemMatch[2]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    readBody(req, 64 * 1024, (buf) => {
      let body;
      try { body = JSON.parse(buf.toString('utf8') || '{}'); }
      catch { return sendJson(res, 400, { error: 'invalid json' }); }
      const title = String(body.title || '').trim();
      if (!title) return sendJson(res, 400, { error: '標題不可為空' });
      r.title = title;
      save();
      sendJson(res, 200, { meeting: m });
    });
    return;
  }

  // ---------- 刪 record（保留卷宗；連同其 source 一起刪） ----------
  if (recordItemMatch && req.method === 'DELETE') {
    const id = Number(recordItemMatch[1]);
    const rid = Number(recordItemMatch[2]);
    const m = findMeeting(id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    const ridx = m.records.findIndex(r => r.id === rid);
    if (ridx === -1) return sendJson(res, 404, { error: 'record not found' });
    if (m.records.length === 1) {
      return sendJson(res, 400, { error: '無法刪除最後一場記錄；請改為刪除整個卷宗' });
    }
    const r = m.records[ridx];
    if (!TERMINAL_STAGES.has(r.stage)) {
      r.cancelled = true;
      const proc = activeProcs[procKey(id, rid)];
      if (proc && !proc.killed) { try { proc.kill('SIGTERM'); } catch {} }
    }
    const removed = m.records.splice(ridx, 1)[0];
    save();

    // NotebookLM 上刪掉這 record 對應的 sources
    const deletes = (removed.sources || [])
      .filter(s => s.sourceId)
      .map(s => run(NLM_BIN, ['source', 'delete', s.sourceId, '--confirm']).catch(e => String(e.message || e)));
    Promise.all(deletes).then(results => {
      // 本地音檔
      for (const src of removed.sources || []) {
        if (src.audioFile) {
          try { fs.unlinkSync(path.join(RECORDINGS_DIR, src.audioFile)); } catch {}
        }
      }
      const warnings = results.filter(x => typeof x === 'string');
      sendJson(res, 200, { meeting: m, nlmWarnings: warnings.length ? warnings : undefined });
    });
    return;
  }

  // ---------- 在指定 record 加 source（追加） ----------
  const recSourcesMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)\/sources$/);
  if (req.method === 'POST' && recSourcesMatch) {
    const id = Number(recSourcesMatch[1]);
    const rid = Number(recSourcesMatch[2]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    if (r.stage !== 'done') return sendJson(res, 400, { error: '只有已封印（done）的場次能續錄。請先等目前的處理完成。' });
    if (!m.notebookId) return sendJson(res, 400, { error: '此卷宗沒有對應的 NotebookLM 筆記，無法續錄' });

    readBody(req, 500 * 1024 * 1024, (buf) => {
      const nextIdx = r.sources.length;
      const parsed = parseSourcePayload(buf, req.headers, id, rid, nextIdx);
      if (parsed.error) return sendJson(res, 400, { error: parsed.error });
      r.cancelled = false;
      r.error = undefined;
      const src = buildSourceEntry(parsed.kind, parsed.payload);
      src.title = defaultSourceTitle(src, r, nextIdx);
      r.sources.push(src);
      save();
      sendJson(res, 200, { meeting: m, recordId: r.id });
      appendSourceToRecord(m, r, r.sources.length - 1);
    });
    return;
  }

  // ---------- record 內 source 改名／刪除 ----------
  const recSourceItemMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)\/sources\/(\d+)$/);
  if (recSourceItemMatch && (req.method === 'DELETE' || req.method === 'PATCH')) {
    const id = Number(recSourceItemMatch[1]);
    const rid = Number(recSourceItemMatch[2]);
    const sidx = Number(recSourceItemMatch[3]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    if (!TERMINAL_STAGES.has(r.stage)) {
      return sendJson(res, 400, { error: '場次處理中，無法編輯來源；請先取消或等候完成' });
    }
    const src = r.sources?.[sidx];
    if (!src) return sendJson(res, 404, { error: 'source not found' });

    if (req.method === 'DELETE') {
      const nlmTask = src.sourceId
        ? run(NLM_BIN, ['source', 'delete', src.sourceId, '--confirm']).then(() => null).catch(e => String(e.message || e))
        : Promise.resolve(null);
      nlmTask.then(warning => {
        const removed = r.sources.splice(sidx, 1)[0];
        if (removed?.audioFile) {
          try { fs.unlinkSync(path.join(RECORDINGS_DIR, removed.audioFile)); } catch {}
        }
        save();
        sendJson(res, 200, { meeting: m, nlmDeleted: !warning, warning: warning || undefined });
      });
      return;
    }

    readBody(req, 64 * 1024, async (buf) => {
      let body;
      try { body = JSON.parse(buf.toString('utf8') || '{}'); }
      catch { return sendJson(res, 400, { error: 'invalid json' }); }
      const title = String(body.title || '').trim();
      if (!title) return sendJson(res, 400, { error: '標題不可為空' });
      src.title = title;
      save();
      if (src.sourceId && m.notebookId) {
        try {
          await run(NLM_BIN, ['source', 'rename', src.sourceId, title, '--notebook', m.notebookId]);
          sendJson(res, 200, { meeting: m, synced: true });
        } catch (e) {
          sendJson(res, 200, { meeting: m, synced: false, warning: String(e.message || e) });
        }
      } else {
        sendJson(res, 200, { meeting: m, synced: false });
      }
    });
    return;
  }

  // ---------- 對指定 record 重新摘要 ----------
  const resummarizeMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/records\/(\d+)\/resummarize$/);
  if (req.method === 'POST' && resummarizeMatch) {
    const id = Number(resummarizeMatch[1]);
    const rid = Number(resummarizeMatch[2]);
    const m = findMeeting(id);
    const r = findRecord(m, rid);
    if (!m || !r) return sendJson(res, 404, { error: 'not found' });
    if (r.stage !== 'done') return sendJson(res, 400, { error: '只有已封印（done）的場次能重新摘要' });
    if (!m.notebookId) return sendJson(res, 400, { error: '此卷宗沒有對應的 NotebookLM 筆記，無法重新摘要' });
    if (!r.sources?.length) return sendJson(res, 400, { error: '此場次目前沒有任何來源，無法產生摘要' });
    readBody(req, 64 * 1024, (buf) => {
      let body = {};
      const raw = buf.toString('utf8');
      if (raw) {
        try { body = JSON.parse(raw); }
        catch { return sendJson(res, 400, { error: 'invalid json' }); }
      }
      // 模式切換放在 meeting 上（整個卷宗共用）
      const nextMode = ['meeting', 'sync', 'class'].includes(body.mode) ? body.mode : null;
      if (nextMode && nextMode !== (m.mode || 'meeting')) {
        m.mode = nextMode;
      }
      r.cancelled = false;
      r.error = undefined;
      save();
      sendJson(res, 200, { meeting: m, recordId: r.id });
      resummarizeRecord(m, r);
    });
    return;
  }

  // ---------- SSE ----------
  const streamMatch = url.pathname.match(/^\/stream\/(\d+)$/);
  if (req.method === 'GET' && streamMatch) {
    const id = Number(streamMatch[1]);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.write(': connected\n\n');
    const m = findMeeting(id);
    if (m) {
      // 把每個 record 當下的 stage 推一次，前端初始化
      for (const r of m.records || []) {
        res.write(`event: stage\ndata: ${JSON.stringify({ recordId: r.id, stage: r.stage, msg: r.stageMsg })}\n\n`);
      }
    }
    sseClients[id] = sseClients[id] || [];
    sseClients[id].push(res);
    req.on('close', () => {
      sseClients[id] = (sseClients[id] || []).filter(r => r !== res);
    });
    return;
  }

  res.writeHead(404).end();
});

server.listen(PORT, () => {
  console.log(`Yomi 月讀 → http://localhost:${PORT}`);
});
