# Yomi · 月讀

> 把錄音封進卷宗，讓月讀替你閱讀。

一個跑在自己電腦上的本地 web app：用瀏覽器原生錄音（或拖音檔進來）→ 自動丟給 NotebookLM → 拿回逐字稿與摘要 → 變成你私人的可搜尋卷宗。

整個流程沒有第三方雲端後端，只有：你的麥克風 → 你的電腦 → 你登入的 NotebookLM 帳號。

## 它做什麼

| 你做的事 | Yomi 替你做的事 |
| --- | --- |
| 點一下月兔（或忍貓）開始錄 | 用瀏覽器 `MediaRecorder` 錄音、計時、波形顯示 |
| 結束錄音 | 自動建一份 NotebookLM 卷宗、上傳音檔 |
| 等月讀讀完 | 拿回逐字稿、抓重點、行動項、決議、可反問的問題 |
| 之後想回顧 | 全文搜尋、查源頭、把新音檔追加進同一卷 |

四段式封印：**結印 · 凝練 · 蓋印 · 已封印**（你會看到這四個字輪流亮，那是 pipeline 階段）。

## 適合誰

- 開很多會、做訪談、用語音寫筆記，需要事後可搜尋
- 願意讓 Google NotebookLM 處理音檔（這是核心依賴）
- 用 macOS（主要開發與測試環境；Linux 理論上可用，沒實測過）

## 系統需求

- macOS（主要支援）；Linux 應該也能跑但未驗證
- Node.js 18+
- [`nlm`](https://pypi.org/project/notebooklm-mcp-cli/) CLI，已登入 NotebookLM
- 一個能進 [NotebookLM](https://notebooklm.google.com) 的 Google 帳號
- 支援 `MediaRecorder` 的瀏覽器（Chrome / Safari / Edge / Firefox 都行）

## 一鍵安裝（建議）

如果你裝了 [Claude Code](https://claude.com/claude-code)：

```bash
git clone git@github.com:MisssssXie/Yomi.git
cd Yomi
claude
```

進到 Claude Code 之後輸入：

```
/setup-yomi
```

Claude 會幫你逐步檢查環境、補裝缺的東西、登入 NotebookLM、啟動 server，全程跟你對話確認。

> 註：`/setup-yomi` 是這個 repo 自帶的 [project-level slash command](./.claude/commands/setup-yomi.md)，只在 `cd Yomi` 之後啟動的 Claude Code 才看得到。

## 手動安裝

```bash
# 1. 取得程式碼
git clone git@github.com:MisssssXie/Yomi.git
cd Yomi

# 2. 裝 nlm CLI（推薦用 uv 或 pipx）
brew install uv          # 若沒裝 uv
uv tool install notebooklm-mcp-cli

# 3. 登入 NotebookLM（會開瀏覽器登入 Google）
nlm login

# 4. 確認 nlm 能跑
nlm notebook list

# 5. 啟動 Yomi
npm start
```

開瀏覽器到 <http://localhost:3748> 就會看到月兔等你。

第一次按開始錄音時瀏覽器會跳一個權限視窗請求**麥克風**，按允許就好。如果不小心拒絕了，到瀏覽器網址列旁邊的小鎖頭重新打開。

## 常用指令

| 指令 | 做什麼 |
| --- | --- |
| `npm start` | 啟動伺服器（預設 port `3748`） |
| `npm run stop` | 殺掉佔用 port 的程序 |
| `npm run restart` | 重啟 |

## 你的資料在哪

- `recordings/` — 錄音原檔（已 gitignore，永遠不會被推上 GitHub）
- `meetings-data.json` — 卷宗索引（同上）
- NotebookLM 帳號 — 音檔與逐字稿存在 Google 那邊，要刪的話進 NotebookLM 自己刪

Yomi 不上傳到任何其他地方，沒有 telemetry、沒有第三方分析。

## 架構（給工程師看的）

整個 app 只有三個檔：

- `server.js` — Node http server，封裝 `nlm` CLI 與 QuickTime 控制
- `public/index.html` — 單頁 vanilla JS，靠 SSE 接收狀態
- `meetings-data.json` — 所有狀態就是這一個檔

Pipeline 階段：`queued → creating → uploading → transcribing → summarizing → done`，每一步都會推送 SSE 給前端。

詳細慣例見 [CLAUDE.md](./CLAUDE.md)。

## 風格

介面用月讀／忍術的詞彙：
- 「結印」「凝練」「蓋上月影之印」是錄音、轉錄、摘要的階段
- 「已封印」就是「處理完了」
- 待機畫面的圓圈動物可以在右上角切換月兔／忍貓

純裝飾，不影響功能。如果你不喜歡可以改 `tplIdle()` 跟階段的 `stageLabel()`。

## 授權

MIT，見 [LICENSE](./LICENSE)。
