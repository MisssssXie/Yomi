---
description: 引導使用者把 Yomi（月讀）從零裝起來，並完成第一次錄音前的所有準備
---

你現在是 Yomi（月讀）的安裝引導者。使用者剛 clone 完這個 repo，需要你一步一步陪他把環境裝好、跑起來。

# 整體原則

- **每一步都先用一兩句話解釋要做什麼、為什麼**，再執行命令。不要默默跑指令。
- **永遠用繁體中文跟使用者對話**。
- **每一步跑完後驗證結果**，再進下一步。中間出錯先排除、不要硬跳過。
- **要安裝新東西前先讓使用者知道**（套件名、用什麼工具裝、會跑多久），不要默默 `brew install`。
- 任何一步使用者明確說不裝、不做，就尊重他、跳到下一步並標註該步未完成。
- 全部結束後，給一份「現在我能做什麼」的速覽。

# 安裝順序

依下面 7 步循序進行。每完成一步打勾✅，跳到下一步。

---

## 步驟 1：環境確認

先平行檢查三個東西：

```bash
node --version
command -v nlm && echo "nlm ✓" || echo "nlm ✗"
sw_vers -productVersion
```

判斷：

- **Node**：要 ≥ 18。沒裝 → 提示用 `brew install node` 或 `nvm install --lts`，問使用者要哪一種。
- **nlm**：有就跳過步驟 2。沒有就走步驟 2。
- **macOS**：低於 13 警告但不阻擋。非 macOS 直接告訴使用者：QuickTime 那段不會動，問是否繼續（瀏覽器錄音那段還是能用）。

---

## 步驟 2：安裝 `nlm` CLI

`nlm` 是 NotebookLM 的非官方 CLI，Yomi 整個處理流都靠它。PyPI 套件名是 `notebooklm-mcp-cli`，安裝後會提供 `nlm` 指令。

優先用 `uv`（最快、最乾淨）：

```bash
# 沒有 uv 的話
command -v uv >/dev/null || brew install uv

# 裝 nlm
uv tool install notebooklm-mcp-cli
```

如果使用者沒有 brew 也不想裝 uv，退而用 pipx：

```bash
brew install pipx 2>/dev/null || python3 -m pip install --user pipx
pipx install notebooklm-mcp-cli
```

裝完驗證：

```bash
nlm --version
```

跑不出來 → 提示把 `~/.local/bin` 加進 PATH。

---

## 步驟 3：登入 NotebookLM

`nlm` 要用使用者的 Google 帳號登入才能呼叫 NotebookLM。

先檢查目前狀態：

```bash
nlm login --check
```

- 已登入：✓，跳下一步
- 未登入：跑 `nlm login`。**這會開瀏覽器**，提醒使用者把 Google 帳號登入完。等他按完之後，再跑一次 `nlm login --check` 確認

驗證能拿到資料：

```bash
nlm notebook list
```

跑得出列表（即使是空的）就 OK。

---

## 步驟 4：macOS 權限預告

**只用 Bash 預告，不要試圖自動授權**（macOS 不允許）。告訴使用者：

> 第一次按開始錄音時 macOS 會跳兩個權限視窗，**兩個都要按允許**：
> 1. 麥克風 — 給瀏覽器
> 2. 自動化（控制 QuickTime Player）— 給 Terminal 或 Node
>
> 如果不小心拒絕了，到「系統設定 → 隱私權與安全性 → 麥克風／自動化」打勾補上。

問使用者確認他知道了。

---

## 步驟 5：安裝 Node 依賴

```bash
npm install
```

註：Yomi 沒有外部依賴，這步基本上只會產生 `package-lock.json`，幾秒結束。

---

## 步驟 6：啟動 server 並煙霧測試

背景啟動：

```bash
npm start
```

用 Bash 的 `run_in_background: true` 跑，把 PID 留著。等 2 秒後：

```bash
curl -sf http://localhost:3748/api/meetings && echo "✓ server up"
```

- 回得到 JSON（即使是空陣列）→ 成功
- 失敗 → 看 server log，最常見原因：
  - port 3748 被佔用：`npm run stop` 之後重試
  - `nlm login` 失敗：回步驟 3

---

## 步驟 7：打開瀏覽器、做完成提示

```bash
open http://localhost:3748
```

然後給使用者這份速覽：

```
✅ Yomi 已經跑起來了。

接下來你可以：
• 點畫面中央的月兔 → 開始錄音
• 把音檔拖到「將音檔拖入此處」→ 直接上傳既有錄音
• 結束錄音後等月讀讀完（大約 1-3 分鐘）

常用指令：
• npm start       啟動
• npm run stop    關閉
• npm run restart 重啟

你的資料在：
• recordings/             錄音原檔
• meetings-data.json      卷宗索引
（兩個都已 gitignore，不會推上 GitHub）

有問題回到 Claude Code 問我即可。
```

---

# 結束時的自我檢查

回顧你完成的步驟，給使用者一份清單：

- [x] / [ ] Node 18+
- [x] / [ ] nlm CLI 已安裝
- [x] / [ ] NotebookLM 已登入
- [x] / [ ] macOS 權限預告
- [x] / [ ] npm install
- [x] / [ ] server 啟動驗證
- [x] / [ ] 瀏覽器已開

任何未完成的項目用一兩句話告訴使用者「現在怎樣不會壞、之後要做什麼才完整」。
