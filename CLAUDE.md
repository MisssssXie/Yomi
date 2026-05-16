# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 這是什麼

Yomi（月讀）是一個本地 web app：瀏覽器 `MediaRecorder` 錄音（或拖上傳音檔）→ 透過 `nlm` CLI 上傳到 NotebookLM → 拿回逐字稿與摘要。主要在 macOS 上測試與使用。

整個專案只有三個檔案：`server.js`、`public/index.html`、`meetings-data.json`。沒有框架、沒有打包、沒有測試。

## 常用指令

- `npm start` — 啟動伺服器（預設 port `3748`）
- `npm run stop` — 殺掉佔用 port 的程序
- `npm run restart` — 重啟

## 執行前提

- 主機上要有 `nlm` CLI 並已登入 NotebookLM（可用 `NLM_BIN` 覆寫路徑）
- 瀏覽器要允許麥克風存取（首次按開始錄音時會跳）

## 架構重點

**Pipeline 階段**（在 `server.js` 的 `processMeeting()`）：
`queued → creating → uploading → transcribing → summarizing → done`（或任何一步變成 `error`）

每階段都會更新記憶體狀態、寫回 `meetings-data.json`、並透過 SSE 推給前端。

**摘要 prompt** 寫死在 `processMeeting()` 裡，產生四個 markdown 區塊（`三句話摘要`、`行動項目`、`決議事項`、`可反問的關鍵問題`）。前端直接渲染，改 prompt 要連帶驗證畫面。

**狀態保存**：整個 app 狀態就是 `meetings-data.json` 一個檔。沒有 migration，加欄位要容忍舊資料缺欄。

**錄音**：瀏覽器端用 `MediaRecorder` 直接錄（優先 `audio/mp4`、再退 `audio/webm;codecs=opus`），錄完透過 `POST /api/meetings`（multipart）上傳到 server，存到 `recordings/<id>.<ext>`。同時只能錄一份。

**前端**：單檔 vanilla JS 狀態機（`idle | recording | processing | summary | error`），透過 `EventSource` 訂閱 `/stream/:id` 即時更新。

## UI 慣例

- 所有使用者文字與註解都用繁體中文
- 設計走「月讀／忍者」風格，階段標籤是 `結印`、`凝練`、`已封印` 這類風格化詞彙
- 改 stage key 時，記得同步前端的 `stageLabel()` 和 `buildSteps()`，否則畫面會掉回原始 key
