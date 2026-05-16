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

// 啟動恢復：上次跑到一半（非 done / error / cancelled）的會議標記為 error
const TERMINAL_STAGES = new Set(['done', 'error', 'cancelled']);
let recovered = 0;
for (const m of state.meetings) {
  if (!TERMINAL_STAGES.has(m.stage)) {
    m.stage = 'error';
    m.stageMsg = '伺服器重啟導致工作中斷，請刪除後重新上傳';
    m.error = m.stageMsg;
    recovered++;
  }
}
if (recovered) { save(); console.log(`已恢復 ${recovered} 筆中斷的會議為 error`); }

// 追蹤每個 meeting 當下 spawn 出去的 child process（取消用）
const activeProcs = {};

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

async function processMeeting(meeting) {
  const audioPath = path.join(RECORDINGS_DIR, meeting.audioFile);
  try {
    checkCancelled(meeting);
    pushStage(meeting, 'creating', '建立 NotebookLM 筆記中…');
    const createRes = await runForMeeting(meeting.id, NLM_BIN, ['notebook', 'create', meeting.title]);
    const notebookId = parseId(createRes.stdout);
    if (!notebookId) throw new Error('無法解析 notebook id: ' + createRes.stdout);
    meeting.notebookId = notebookId;
    save();

    checkCancelled(meeting);
    pushStage(meeting, 'uploading', '上傳音檔到 NotebookLM（請耐心等候，1 小時錄音約需 5 分鐘）…');
    const addRes = await runForMeeting(meeting.id, NLM_BIN, [
      'source', 'add', notebookId,
      '--file', audioPath,
      '--wait', '--wait-timeout', '1800'
    ]);
    const sourceId = parseId(addRes.stdout);
    meeting.sourceId = sourceId;
    save();

    checkCancelled(meeting);
    pushStage(meeting, 'transcribing', '取得逐字稿…');
    if (sourceId) {
      try {
        const tRes = await runForMeeting(meeting.id, NLM_BIN, ['source', 'get', sourceId]);
        meeting.transcript = extractContent(tRes.stdout);
      } catch (e) {
        meeting.transcript = `(無法取得逐字稿: ${e.message})`;
      }
      save();
    }

    checkCancelled(meeting);
    pushStage(meeting, 'summarizing', '產生摘要與反問問題…');
    const prompt = `你是專業會議助理。請完整參考此會議錄音 source，輸出以下 markdown 區塊，**只能輸出這些區塊，不要前後加任何說明**：

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
    const qRes = await runForMeeting(meeting.id, NLM_BIN, ['notebook', 'query', notebookId, prompt]);
    meeting.summary = extractAnswer(qRes.stdout);
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
    // 攔截 NLM 常見的「無法處理音檔」訊號（無語言內容、格式不支援等）
    let friendly = raw;
    if (/no.*language|language.*not.*detected|no.*content|偵測不到|沒有內容/i.test(raw)) {
      friendly = 'NotebookLM 無法從音檔偵測到語言內容（可能太短、太雜訊、或未說話）。請重新錄音或上傳。\n\n原始訊息：\n' + raw;
    } else if (/timeout|逾時/i.test(raw)) {
      friendly = 'NotebookLM 處理逾時。可能音檔過長或服務繁忙，請稍後重試。\n\n原始訊息：\n' + raw;
    }
    meeting.error = friendly;
    pushStage(meeting, 'error', friendly);
    broadcast(meeting.id, 'error', { msg: friendly });
  }
}

// 從音檔建立會議卡並啟動 pipeline（上傳檔案 / 瀏覽器錄音共用）
function createMeetingForFile(id, audioFile, audioSize) {
  const now = new Date();
  const ts = now.toLocaleString('zh-TW', { hour12: false }).replace(/\//g, '-');
  const meeting = {
    id, title: `會議 ${ts}`,
    audioFile, audioSize,
    stage: 'queued', createdAt: now.toISOString()
  };
  state.meetings.unshift(meeting);
  save();
  return meeting;
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
    pushStage(m, 'cancelled', '已取消');
    broadcast(id, 'cancelled', { msg: '已取消' });
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
    try { fs.unlinkSync(path.join(RECORDINGS_DIR, m.audioFile)); } catch {}

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
      const ext = String(req.headers['x-audio-ext'] || 'm4a').replace(/[^a-z0-9]/gi, '') || 'm4a';
      const id = state.nextId++;
      const audioFile = `${id}.${ext}`;
      fs.writeFileSync(path.join(RECORDINGS_DIR, audioFile), buf);
      const meeting = createMeetingForFile(id, audioFile, buf.length);
      sendJson(res, 200, { id, meeting });
      processMeeting(meeting);
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
