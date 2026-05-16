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
function run(cmd, args, { input } = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let out = '', err = '';
    p.stdout.on('data', d => out += d.toString());
    p.stderr.on('data', d => err += d.toString());
    p.on('close', code => {
      if (code === 0) resolve({ stdout: out, stderr: err });
      else reject(new Error(`${cmd} ${args.join(' ')} → exit ${code}\n${err || out}`));
    });
    p.on('error', reject);
    if (input) p.stdin.end(input);
    else p.stdin.end();
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
async function processMeeting(meeting) {
  const audioPath = path.join(RECORDINGS_DIR, meeting.audioFile);
  try {
    pushStage(meeting, 'creating', '建立 NotebookLM 筆記中…');
    const createRes = await run(NLM_BIN, ['notebook', 'create', meeting.title]);
    const notebookId = parseId(createRes.stdout);
    if (!notebookId) throw new Error('無法解析 notebook id: ' + createRes.stdout);
    meeting.notebookId = notebookId;
    save();

    pushStage(meeting, 'uploading', '上傳音檔到 NotebookLM（請耐心等候，1 小時錄音約需 5 分鐘）…');
    const addRes = await run(NLM_BIN, [
      'source', 'add', notebookId,
      '--file', audioPath,
      '--wait', '--wait-timeout', '1800'
    ]);
    const sourceId = parseId(addRes.stdout);
    meeting.sourceId = sourceId;
    save();

    pushStage(meeting, 'transcribing', '取得逐字稿…');
    if (sourceId) {
      try {
        const tRes = await run(NLM_BIN, ['source', 'get', sourceId]);
        meeting.transcript = extractContent(tRes.stdout);
      } catch (e) {
        meeting.transcript = `(無法取得逐字稿: ${e.message})`;
      }
      save();
    }

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
    const qRes = await run(NLM_BIN, ['notebook', 'query', notebookId, prompt]);
    meeting.summary = extractAnswer(qRes.stdout);
    meeting.completedAt = new Date().toISOString();
    pushStage(meeting, 'done', '完成');
    broadcast(meeting.id, 'done', { meeting });
  } catch (e) {
    meeting.error = String(e.message || e);
    pushStage(meeting, 'error', meeting.error);
    broadcast(meeting.id, 'error', { msg: meeting.error });
  }
}

// ---------- QuickTime 錄音（macOS 原生，osascript 控制） ----------
let recordingState = { active: false, startedAt: null };

async function startQuickTimeRecording() {
  if (recordingState.active) throw new Error('已在錄音中');
  const script = `
tell application "QuickTime Player"
  activate
  set newRecording to new audio recording
  tell newRecording to start
end tell`;
  await run('osascript', ['-e', script]);
  recordingState = { active: true, startedAt: Date.now() };
}

async function stopQuickTimeRecording() {
  if (!recordingState.active) throw new Error('沒有正在進行的錄音');
  const id = state.nextId++;
  const audioFile = `${id}.mov`;
  const outputPath = path.join(RECORDINGS_DIR, audioFile);
  const script = `
tell application "QuickTime Player"
  if (count of documents) is 0 then error "QuickTime 沒有開啟的錄音文件"
  tell document 1 to stop
  delay 0.3
  save document 1 in (POSIX file "${outputPath}")
  close document 1 saving no
end tell`;
  await run('osascript', ['-e', script]);
  recordingState = { active: false, startedAt: null };
  return { id, audioFile };
}

// 從音檔建立會議卡並啟動 pipeline（錄音停止 / 上傳檔案共用）
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

  if (req.method === 'DELETE' && idMatch) {
    const id = Number(idMatch[1]);
    const idx = state.meetings.findIndex(x => x.id === id);
    if (idx === -1) return sendJson(res, 404, { error: 'not found' });
    const m = state.meetings[idx];
    state.meetings.splice(idx, 1);
    save();
    try { fs.unlinkSync(path.join(RECORDINGS_DIR, m.audioFile)); } catch {}
    return sendJson(res, 200, { ok: true });
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

  if (req.method === 'GET' && url.pathname === '/api/recording/status') {
    return sendJson(res, 200, {
      active: recordingState.active,
      startedAt: recordingState.startedAt,
      elapsedSec: recordingState.active ? Math.floor((Date.now() - recordingState.startedAt) / 1000) : 0
    });
  }

  if (req.method === 'POST' && url.pathname === '/api/recording/start') {
    startQuickTimeRecording()
      .then(() => sendJson(res, 200, { ok: true, startedAt: recordingState.startedAt }))
      .catch(e => sendJson(res, 500, { error: String(e.message || e) }));
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/recording/stop') {
    stopQuickTimeRecording()
      .then(({ id, audioFile }) => {
        const stats = fs.statSync(path.join(RECORDINGS_DIR, audioFile));
        const meeting = createMeetingForFile(id, audioFile, stats.size);
        sendJson(res, 200, { id, meeting });
        processMeeting(meeting);
      })
      .catch(e => sendJson(res, 500, { error: String(e.message || e) }));
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
