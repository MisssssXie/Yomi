// =============================================================================
//  Yomi（讀）— 伺服器
//  純 Node.js HTTP、無外部依賴。Pipeline：上傳音檔 → nlm 上傳 NotebookLM
//  → 取逐字稿 → query 產生摘要與反問問題 → SSE 推進度回前端
// =============================================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = Number(process.env.PORT || 3748);
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, 'meetings-data.json');
const RECORDINGS_DIR = path.join(ROOT, 'recordings');
const NLM_BIN = process.env.NLM_BIN || 'nlm';

const state = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
const save = () => fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));

// 啟動恢復 + 結構升級：把舊欄位（audioFile / sourceId / transcript）搬進 sources[]
const TERMINAL_STAGES = new Set(['done', 'error', 'cancelled']);
let recovered = 0;
let mutated = false;
for (const m of state.meetings) {
  if (!TERMINAL_STAGES.has(m.stage)) {
    m.stage = 'error';
    m.stageMsg = '伺服器重啟導致工作中斷，請刪除後重新上傳';
    m.error = m.stageMsg;
    recovered++;
    mutated = true;
  }
  if (!Array.isArray(m.sources) || m.sources.length === 0) {
    if (m.audioFile) {
      m.sources = [{
        audioFile: m.audioFile,
        audioSize: m.audioSize,
        sourceId: m.sourceId,
        transcript: m.transcript,
        addedAt: m.createdAt
      }];
    } else {
      m.sources = [];
    }
    mutated = true;
  }
  if ('audioFile' in m || 'audioSize' in m || 'sourceId' in m || 'transcript' in m) {
    delete m.audioFile; delete m.audioSize; delete m.sourceId; delete m.transcript;
    mutated = true;
  }
}
if (mutated) save();
if (recovered) console.log(`已恢復 ${recovered} 筆中斷的會議為 error`);

// 追蹤每個 meeting 當下 spawn 出去的 child process（取消用）
const activeProcs = {};
// 追蹤哪些 meeting 正在續錄中（失敗時要回滾到 done 而非標 error）
const appendingMeetings = new Set();

// ---------- SSE ----------
const sseClients = {};
function broadcast(id, event, data) {
  const list = sseClients[id] || [];
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of list) { try { res.write(payload); } catch {} }
}
function pushStage(meeting, stage, msg) {
  meeting.stage = stage;
  meeting.stageMsg = msg;
  save();
  broadcast(meeting.id, 'stage', { stage, msg });
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

// 跑 nlm 指令並把 child 註冊到 activeProcs，方便外部取消
function runForMeeting(meetingId, cmd, args, opts = {}) {
  return run(cmd, args, {
    ...opts,
    onProc: p => { activeProcs[meetingId] = p; }
  }).finally(() => {
    if (activeProcs[meetingId] && !activeProcs[meetingId].killed) {
      // child 正常結束，不清空（保留給其他指令覆蓋）
    }
    delete activeProcs[meetingId];
  });
}

// 從 nlm 的 JSON 輸出取出指定欄位（會嘗試頂層 / value / 陣列第一筆）
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

// 解析 nlm 輸出。優先 JSON，否則用 regex 抓 ID。
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
function checkCancelled(meeting) {
  if (meeting.cancelled) throw new Error('__CANCELLED__');
}

const SUMMARY_PROMPT = `你是專業會議助理。請完整參考此卷宗內**所有**會議錄音 source（可能是同一場會議中場休息後的多段），輸出以下 markdown 區塊，**只能輸出這些區塊，不要前後加任何說明**：

## 三句話摘要
- （第一句）
- （第二句）
- （第三句）

## 行動項目
| 負責人 | 任務 | 時程 |
| --- | --- | --- |
| ... | ... | ... |

## 決議事項
- ...

## 可反問的關鍵問題
1. ...
2. ...
3. ...`;

function friendlyError(raw) {
  if (/no.*language|language.*not.*detected|no.*content|偵測不到|沒有內容/i.test(raw)) {
    return 'NotebookLM 無法從音檔偵測到語言內容（可能太短、太雜訊、或未說話）。請重新錄音或上傳。\n\n原始訊息：\n' + raw;
  }
  if (/timeout|逾時/i.test(raw)) {
    return 'NotebookLM 處理逾時。可能音檔過長或服務繁忙，請稍後重試。\n\n原始訊息：\n' + raw;
  }
  return raw;
}

// 各種 source 類型對應的 stage 訊息
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

// 把一段 source（audio/file/text/url/youtube/drive）加進既有 notebook、取內容、回填。
async function ingestSource(meeting, src, opts = {}) {
  const kind = src.kind || 'audio';
  const isText = kind === 'text';
  const uploadStage = opts.uploadStage || 'uploading';
  const uploadMsg = opts.uploadMsg || KIND_UPLOAD_MSG[kind] || KIND_UPLOAD_MSG.audio;
  const transcribeMsg = opts.transcribeMsg || KIND_TRANSCRIBE_MSG[kind] || KIND_TRANSCRIBE_MSG.audio;

  checkCancelled(meeting);
  pushStage(meeting, uploadStage, uploadMsg);

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
    // audio / file
    const audioPath = path.join(RECORDINGS_DIR, src.audioFile);
    addArgs = ['source', 'add', meeting.notebookId, '--file', audioPath];
  }
  if (src.title) addArgs.push('--title', src.title);
  addArgs.push('--wait', '--wait-timeout', '1800');

  const addRes = await runForMeeting(meeting.id, NLM_BIN, addArgs);
  src.sourceId = parseId(addRes.stdout);
  if (isText) src.transcript = src.text;
  save();

  // 上傳指令回了 0 但拿不到 source ID — 視為上傳失敗，不要默默繼續
  if (!src.sourceId) {
    const raw = (addRes.stdout || addRes.stderr || '').trim();
    throw new Error(
      '上傳到 NotebookLM 完成但無法解析 source ID，可能上傳失敗或服務回應異常。\n\n原始回應：\n' +
      (raw.slice(0, 800) || '(無輸出)')
    );
  }

  if (!isText) {
    checkCancelled(meeting);
    pushStage(meeting, 'transcribing', transcribeMsg);
    let transcribeError = null;
    try {
      const tRes = await runForMeeting(meeting.id, NLM_BIN, ['source', 'get', src.sourceId]);
      src.transcript = extractContent(tRes.stdout);
    } catch (e) {
      transcribeError = e;
      src.transcript = `(無法取得內容: ${e.message})`;
    }
    save();

    // 音檔／檔案類來源是「我們交給 NotebookLM 的東西」，如果連內容都讀不回來，
    // 後續摘要也不會有意義；直接視為錯誤。網頁 / YouTube / Drive 由 NotebookLM 自行
    // 抓內容，取不到逐字稿不一定代表失敗，仍給機會繼續走摘要。
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

// 驗證 NotebookLM 摘要回傳是否有意義；空字串 / 過短 / 純錯誤訊息都當失敗
function validateSummary(stdout) {
  const summary = extractAnswer(stdout);
  const trimmed = (summary || '').trim();
  if (!trimmed || trimmed.length < 20) {
    throw new Error(
      'NotebookLM 沒有回傳有效摘要（內容為空或過短），可能來源未被正確處理或服務暫時不可用。\n\n原始回應：\n' +
      ((stdout || '').slice(0, 800) || '(空)')
    );
  }
  return summary;
}

async function processMeeting(meeting) {
  try {
    checkCancelled(meeting);
    pushStage(meeting, 'creating', '建立 NotebookLM 筆記中…');
    const createRes = await runForMeeting(meeting.id, NLM_BIN, ['notebook', 'create', meeting.title]);
    const notebookId = parseId(createRes.stdout);
    if (!notebookId) throw new Error('無法解析 notebook id: ' + createRes.stdout);
    meeting.notebookId = notebookId;
    save();

    await ingestSource(meeting, meeting.sources[0]);

    checkCancelled(meeting);
    pushStage(meeting, 'summarizing', '產生摘要與反問問題…');
    const qRes = await runForMeeting(meeting.id, NLM_BIN, ['notebook', 'query', notebookId, SUMMARY_PROMPT]);
    meeting.summary = validateSummary(qRes.stdout);
    meeting.completedAt = new Date().toISOString();
    pushStage(meeting, 'done', '完成');
    broadcast(meeting.id, 'done', { meeting });
  } catch (e) {
    const raw = String(e.message || e);
    if (raw.includes('__CANCELLED__') || meeting.cancelled) {
      pushStage(meeting, 'cancelled', '已取消');
      broadcast(meeting.id, 'cancelled', { msg: '已取消' });
      return;
    }
    const friendly = friendlyError(raw);
    meeting.error = friendly;
    pushStage(meeting, 'error', friendly);
    broadcast(meeting.id, 'error', { msg: friendly });
  }
}

// 把新一段音源加入既有卷宗，並重新生成摘要（會把所有 source 一起納入）
// 失敗時回滾到原本的 done 狀態，把錯誤訊息掛在 meeting.appendError，不會把整個卷宗弄壞
async function appendSourceToMeeting(meeting, sourceIndex) {
  const src = meeting.sources[sourceIndex];
  const previousSummary = meeting.summary;
  const previousCompletedAt = meeting.completedAt;
  appendingMeetings.add(meeting.id);
  try {
    if (!meeting.notebookId) throw new Error('此卷宗尚未建立 NotebookLM 筆記，無法續錄');
    await ingestSource(meeting, src, {
      uploadStage: 'appending',
      uploadMsg: `續錄第 ${sourceIndex + 1} 段來源到既有卷宗…`,
      transcribeMsg: `取得第 ${sourceIndex + 1} 段內容…`,
    });

    checkCancelled(meeting);
    pushStage(meeting, 'summarizing', '重新整合所有來源產生摘要…');
    const qRes = await runForMeeting(meeting.id, NLM_BIN, ['notebook', 'query', meeting.notebookId, SUMMARY_PROMPT]);
    meeting.summary = validateSummary(qRes.stdout);
    meeting.completedAt = new Date().toISOString();
    delete meeting.appendError;
    pushStage(meeting, 'done', '完成');
    broadcast(meeting.id, 'done', { meeting });
  } catch (e) {
    const raw = String(e.message || e);
    const wasCancel = raw.includes('__CANCELLED__') || meeting.cancelled;
    const friendly = wasCancel ? '已取消續錄' : friendlyError(raw);

    // 判斷失敗階段：source 是否成功進到 NotebookLM（拿到 sourceId 代表 ingest 至少前半段過了）
    const ingested = !!src.sourceId;
    const failedAtSummary = ingested && meeting.stage === 'summarizing';

    if (failedAtSummary) {
      // 新來源已上傳成功，只是摘要重生失敗。保留來源、保留舊摘要，下次續錄會再嘗試。
      meeting.appendError = `第 ${sourceIndex + 1} 段已加入，但摘要重整失敗：${friendly}`;
    } else {
      // 上傳或轉錄階段失敗，新來源沒完整進到 NotebookLM。把這個 source 從本地移除。
      const removed = meeting.sources.splice(sourceIndex, 1)[0];
      if (removed && removed.audioFile) {
        try { fs.unlinkSync(path.join(RECORDINGS_DIR, removed.audioFile)); } catch {}
      }
      meeting.appendError = wasCancel
        ? '已取消續錄；新增的來源未保留。'
        : `續錄失敗（已回到上次成功的狀態）：${friendly}`;
    }
    // 還原 done 狀態，把舊摘要保留住
    meeting.summary = previousSummary;
    meeting.completedAt = previousCompletedAt;
    meeting.cancelled = false;
    pushStage(meeting, 'done', '完成');
    broadcast(meeting.id, 'append-error', { msg: meeting.appendError, meeting });
  } finally {
    appendingMeetings.delete(meeting.id);
  }
}

// 預設標題
function defaultTitle(kind, payload) {
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

// 從各種 source 類型建立會議
function createMeetingFor(id, kind, payload) {
  const now = new Date();
  const addedAt = now.toISOString();
  let source;
  if (kind === 'text') {
    source = { kind: 'text', text: payload.text, transcript: payload.text, audioSize: Buffer.byteLength(payload.text, 'utf8'), addedAt };
  } else if (kind === 'url') {
    source = { kind: 'url', url: payload.url, addedAt };
  } else if (kind === 'youtube') {
    source = { kind: 'youtube', url: payload.url, addedAt };
  } else if (kind === 'drive') {
    source = { kind: 'drive', driveId: payload.driveId, driveType: payload.driveType || 'doc', addedAt };
  } else {
    source = { audioFile: payload.audioFile, audioSize: payload.audioSize, addedAt };
  }
  const meeting = {
    id, title: defaultTitle(kind, payload),
    sources: [source], stage: 'queued', createdAt: addedAt
  };
  state.meetings.unshift(meeting);
  save();
  return meeting;
}

// 為 /:id/sources 端點建立 source entry
function buildSourceForAppend(kind, payload) {
  const addedAt = new Date().toISOString();
  if (kind === 'text') {
    return { kind: 'text', text: payload.text, transcript: payload.text, audioSize: Buffer.byteLength(payload.text, 'utf8'), addedAt };
  }
  if (kind === 'url') return { kind: 'url', url: payload.url, addedAt };
  if (kind === 'youtube') return { kind: 'youtube', url: payload.url, addedAt };
  if (kind === 'drive') return { kind: 'drive', driveId: payload.driveId, driveType: payload.driveType || 'doc', addedAt };
  return { audioFile: payload.audioFile, audioSize: payload.audioSize, addedAt };
}

// ---------- HTTP ----------
function sendJson(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
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
    // 留一點時間給 response 送出，然後 detached spawn 重啟腳本，自己退出讓 npm run restart 接手
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
    return sendJson(res, 200, state.meetings.map(m => ({
      id: m.id, title: m.title, stage: m.stage, stageMsg: m.stageMsg,
      createdAt: m.createdAt, completedAt: m.completedAt, error: m.error
    })));
  }

  const idMatch = url.pathname.match(/^\/api\/meetings\/(\d+)$/);
  if (req.method === 'GET' && idMatch) {
    const m = state.meetings.find(x => x.id === Number(idMatch[1]));
    if (!m) return sendJson(res, 404, { error: 'not found' });
    return sendJson(res, 200, m);
  }

  const clearErrMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/clear-append-error$/);
  if (req.method === 'POST' && clearErrMatch) {
    const id = Number(clearErrMatch[1]);
    const m = state.meetings.find(x => x.id === id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    delete m.appendError;
    save();
    return sendJson(res, 200, { ok: true, meeting: m });
  }

  const cancelMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/cancel$/);
  if (req.method === 'POST' && cancelMatch) {
    const id = Number(cancelMatch[1]);
    const m = state.meetings.find(x => x.id === id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    if (TERMINAL_STAGES.has(m.stage)) {
      return sendJson(res, 200, { ok: true, alreadyDone: true });
    }
    m.cancelled = true;
    const proc = activeProcs[id];
    if (proc && !proc.killed) {
      try { proc.kill('SIGTERM'); } catch {}
      setTimeout(() => { try { if (!proc.killed) proc.kill('SIGKILL'); } catch {} }, 2000);
    }
    // 續錄中的取消由 pipeline 的 catch 處理（會回滾到 done）
    if (!appendingMeetings.has(id)) {
      pushStage(m, 'cancelled', '已取消');
      broadcast(id, 'cancelled', { msg: '已取消' });
    }
    return sendJson(res, 200, { ok: true });
  }

  if (req.method === 'PATCH' && idMatch) {
    const id = Number(idMatch[1]);
    const m = state.meetings.find(x => x.id === id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', async () => {
      let body;
      try { body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); }
      catch { return sendJson(res, 400, { error: 'invalid json' }); }
      const title = String(body.title || '').trim();
      if (!title) return sendJson(res, 400, { error: '標題不可為空' });
      const prev = m.title;
      m.title = title;
      save();
      if (m.notebookId) {
        try {
          await run(NLM_BIN, ['notebook', 'rename', m.notebookId, title]);
          sendJson(res, 200, { meeting: m, synced: true });
        } catch (e) {
          // 本地已存，NotebookLM 那邊失敗就回 warning，不 rollback
          sendJson(res, 200, { meeting: m, synced: false, warning: String(e.message || e) });
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

    // 若還在跑就先取消，停掉子程序
    if (!TERMINAL_STAGES.has(m.stage)) {
      m.cancelled = true;
      const proc = activeProcs[id];
      if (proc && !proc.killed) {
        try { proc.kill('SIGTERM'); } catch {}
      }
    }

    state.meetings.splice(idx, 1);
    save();
    for (const src of m.sources || []) {
      if (src.audioFile) {
        try { fs.unlinkSync(path.join(RECORDINGS_DIR, src.audioFile)); } catch {}
      }
    }

    // 連動刪除 NotebookLM 筆記（如果建立過）
    const notebookId = m.notebookId;
    if (notebookId) {
      run(NLM_BIN, ['notebook', 'delete', notebookId, '--confirm'])
        .then(() => sendJson(res, 200, { ok: true, nlmDeleted: true }))
        .catch(e => sendJson(res, 200, { ok: true, nlmDeleted: false, warning: String(e.message || e) }));
      return;
    }
    return sendJson(res, 200, { ok: true, nlmDeleted: false });
  }

  // 解析 body 為對應的 source payload；失敗則回 {error} 物件
  function parseSourcePayload(buf, headers, meetingId, sourceIndex) {
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
    // audio / file
    const ext = String(headers['x-audio-ext'] || 'm4a').replace(/[^a-z0-9]/gi, '') || 'm4a';
    const audioFile = sourceIndex == null ? `${meetingId}.${ext}` : `${meetingId}-${sourceIndex}.${ext}`;
    fs.writeFileSync(path.join(RECORDINGS_DIR, audioFile), buf);
    return { kind: 'audio', payload: { audioFile, audioSize: buf.length } };
  }

  if (req.method === 'POST' && url.pathname === '/api/meetings') {
    const chunks = [];
    let total = 0;
    const MAX = 500 * 1024 * 1024;
    req.on('data', c => {
      total += c.length;
      if (total > MAX) { req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      const buf = Buffer.concat(chunks);
      const id = state.nextId++;
      const parsed = parseSourcePayload(buf, req.headers, id, null);
      if (parsed.error) return sendJson(res, 400, { error: parsed.error });
      const meeting = createMeetingFor(id, parsed.kind, parsed.payload);
      sendJson(res, 200, { id, meeting });
      processMeeting(meeting);
    });
    return;
  }

  // 在既有卷宗加新一段 source（任意來源類型，中場休息／追加場景）
  const sourcesMatch = url.pathname.match(/^\/api\/meetings\/(\d+)\/sources$/);
  if (req.method === 'POST' && sourcesMatch) {
    const id = Number(sourcesMatch[1]);
    const m = state.meetings.find(x => x.id === id);
    if (!m) return sendJson(res, 404, { error: 'not found' });
    if (m.stage !== 'done') return sendJson(res, 400, { error: '只有已封印（done）的卷宗能續錄。請先等目前的處理完成。' });
    if (!m.notebookId) return sendJson(res, 400, { error: '此卷宗沒有對應的 NotebookLM 筆記，無法續錄' });

    const chunks = [];
    let total = 0;
    const MAX = 500 * 1024 * 1024;
    req.on('data', c => {
      total += c.length;
      if (total > MAX) { req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      const buf = Buffer.concat(chunks);
      const nextIdx = m.sources.length + 1;
      const parsed = parseSourcePayload(buf, req.headers, id, nextIdx);
      if (parsed.error) return sendJson(res, 400, { error: parsed.error });
      m.cancelled = false;
      m.error = undefined;
      m.sources.push(buildSourceForAppend(parsed.kind, parsed.payload));
      save();
      sendJson(res, 200, { meeting: m });
      appendSourceToMeeting(m, m.sources.length - 1);
    });
    return;
  }

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
    const m = state.meetings.find(x => x.id === id);
    if (m) res.write(`event: stage\ndata: ${JSON.stringify({ stage: m.stage, msg: m.stageMsg })}\n\n`);
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
