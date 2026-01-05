# TrafficPulse 教學 01：Phase 1 — Bootstrap（環境建置與跑起來）

> 本文件會用「超細」的步驟帶你從 0 開始跑起 TrafficPulse：包含建立虛擬環境、設定 TDX 憑證、抓一小段 VD 資料、啟動 API、打開地圖 dashboard。  
> 程式碼本身仍維持 repo 規範（英文），但你會在這份中文文件中看到大量「分段貼程式碼 + 逐段解釋」。

---

## 1) 本階段目標（你最後要驗收什麼？）

完成本階段後，你應該能做到：

1. 在本機建立 Python 虛擬環境並安裝依賴。
2. 建立 `configs/config.yaml` 與 `.env`，並成功拿到 TDX token（不一定要看到 token，但請求要能成功）。
3. 下載一小段「VD（Vehicle Detector）」資料，輸出到 `data/processed/`。
4. 用 FastAPI 啟動後端，並在瀏覽器打開 `http://localhost:8000/` 看見 dashboard。
5.（可選）啟用 DuckDB+Parquet，讓查詢更快、資料更像 data warehouse 的工作流（但不需要外部服務）。

---

## 2) 你需要先準備什麼？

### 2.1 軟體版本

- Python：`>= 3.10`
- 作業系統：Linux / macOS / Windows 皆可（指令會以 bash 為例）

### 2.2 你需要一組 TDX 憑證

TrafficPulse 會呼叫 TDX API，因此你必須有：

- `TDX_CLIENT_ID`
- `TDX_CLIENT_SECRET`

你可以把它理解成「你是誰」與「你的密碼」，用來換取短期可用的 access token。

---

## 3) 安裝與設定（一步一步照做）

> 建議你在 repo root（也就是 `README.md` 同一層）執行這些指令。

### 3.1 建立虛擬環境（venv）並安裝依賴

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

你在做什麼（新手版解釋）：

- `python -m venv .venv`：在本機建立一個「專案專用」的 Python 環境，避免污染全域 Python。
- `source .venv/bin/activate`：把目前 shell 綁定到這個環境（你之後 `pip install` 才會裝在正確位置）。
- `pip install -r requirements.txt`：安裝 FastAPI、pandas、duckdb、httpx... 等套件。

### 3.2 安裝 `trafficpulse` 為可編輯模式（editable）

```bash
pip install -e .
```

為什麼需要這一步？

- `scripts/*.py` 會 `import trafficpulse...`
- 如果你只安裝 requirements，但沒有把本專案當成 package 安裝，Python 可能找不到 `trafficpulse` 套件
- `-e` 代表「你改 code 不用重裝」，適合開發

### 3.3 複製設定檔範本（config templates）

```bash
cp configs/config.example.yaml configs/config.yaml
cp .env.example .env
cp configs/corridors.example.csv configs/corridors.csv
```

你會得到三個「本機可改」的檔案：

- `configs/config.yaml`：主要設定（資料路徑、抓哪些城市、聚合粒度、指標權重...）
- `.env`：放敏感資訊（TDX 憑證），避免寫進 config 或 git
- `configs/corridors.csv`：走廊清單（Phase 2 會用到）

### 3.4 填入 `.env` 的 TDX 憑證

打開 `.env`，你會看到類似：

```bash
TDX_CLIENT_ID=
TDX_CLIENT_SECRET=
```

把值填進去後存檔。

> 為什麼用 `.env`？  
> 因為它適合放「不想寫進 git」的東西。這個 repo 透過 `python-dotenv` 在啟動時自動載入。

---

## 4) 程式碼導讀：設定是怎麼被載入的？（重要觀念）

### 4.1 `src/trafficpulse/settings.py`：`load_config()` 的行為

這段是「初學者一定要懂」的地方，因為之後所有模組都靠它拿設定。

程式碼節錄（請搭配理解，不需要背）：

```python
def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv()

def load_config(config_path: str | Path | None = None) -> AppConfig:
    _maybe_load_dotenv()
    root = project_root()
    candidate = config_path or os.getenv("TRAFFICPULSE_CONFIG", "configs/config.yaml")
    ...
```

逐段解釋（新手版）：

1. `_maybe_load_dotenv()`：如果你有安裝 `python-dotenv`，它會把 `.env` 內的變數灌進環境變數（environment variables）。
2. `TRAFFICPULSE_CONFIG`：你可以用環境變數指定要讀哪個 config 檔。沒指定時預設讀 `configs/config.yaml`。
3. 如果 `configs/config.yaml` 不存在，會 fallback 到 `configs/config.example.yaml`（避免第一次跑就崩）。

> 這種設計叫做「sane defaults（合理預設）」：新手不用先懂一堆就能跑起來；進階使用者再慢慢改。

---

## 5) 跑一次最小可重現流程（MVP）

接下來我們會抓一個很小的時間窗（例如 3 小時）來驗證整條 pipeline。

### 5.1 下載 VD 資料（segments + observations）

```bash
python scripts/build_dataset.py \
  --start 2026-01-01T00:00:00+08:00 \
  --end 2026-01-01T03:00:00+08:00 \
  --cities Taipei
```

你應該看到輸出類似：

- `Saved segments: .../data/processed/segments.csv`
- `Saved observations: .../data/processed/observations_5m.csv`

#### 5.1.1 `scripts/build_dataset.py` 在做什麼？（概念拆解）

這支 script 的責任很單純：

1. 解析你輸入的時間窗（start/end）
2. 呼叫 `TdxTrafficClient.download_vd(...)` 下載資料
3. 把結果存成 CSV（以及可選的 Parquet）

程式碼節錄：

```python
client = TdxTrafficClient(config=config)
try:
    segments, observations = client.download_vd(start=start, end=end, cities=args.cities)
finally:
    client.close()
```

新手重點：

- `try/finally`：確保 HTTP client 一定會被關閉（避免連線資源泄漏）。
- `segments` 與 `observations` 是兩張表：
  - `segments`：每個 VD 的靜態資訊（位置、道路名、方向...）
  - `observations`：每 5 分鐘一筆的速度/流量/占有率

### 5.2（可選）啟用 DuckDB+Parquet（免外部服務）

如果你想先走「DuckDB + Parquet」的路線（你前面有確認），做法是：

1) 打開 `configs/config.yaml`，把 `warehouse.enabled` 改成 `true`：

```yaml
warehouse:
  enabled: true
  parquet_dir: data/processed/parquet
  use_duckdb: true
```

2) 重新跑一次 `scripts/build_dataset.py`

你會看到多印出：

- `Saved segments (Parquet): ...`
- `Saved observations (Parquet): ...`

你在得到什麼（為什麼值得做）：

- CSV 很通用，但查詢效率有限
- Parquet 是分析友善的 columnar 格式
- DuckDB 是嵌入式（embedded）查詢引擎：像 SQLite 一樣不需要外部服務，但對分析查詢更強

### 5.3 聚合資料（5-min → 15-min 或 hourly）

聚合指令：

```bash
python scripts/aggregate_observations.py
```

它會讀：

- `data/processed/observations_5m.csv`

並輸出（例如 target=15）：

- `data/processed/observations_15m.csv`

> 你要聚合成 15 分鐘或 60 分鐘，是由 `configs/config.yaml` 的 `preprocessing.target_granularity_minutes` 決定。

### 5.4 建立可靠度排行（Reliability rankings）

```bash
python scripts/build_reliability_rankings.py --limit 200
```

你會得到：

- `outputs/reports/reliability_segments.csv`（或類似命名，依 script 為準）

概念上你可以先記三個最基本的指標：

- mean speed（平均速度）
- speed std（速度變異）
- congestion frequency（壅塞頻率：速度低於門檻的比例）

這些指標會被加權組合成一個 score，做排序。

---

## 6) 啟動 API 與打開 Dashboard

### 6.1 啟動 API

```bash
python scripts/run_api.py
```

預期結果：

- Console 會顯示 uvicorn 啟動訊息
- 你可以用瀏覽器開：
  - `http://localhost:8000/docs`（Swagger UI）
  - `http://localhost:8000/`（靜態 dashboard）

### 6.2 Dashboard 是怎麼知道 API 位址的？

預設情況下：

- 如果你是用 API 自己 serve dashboard（`http://localhost:8000/`），前端會自動用同一個 origin
- 如果你是自己用 `python -m http.server` serve `web/`（例如 `http://localhost:5173/`），你可以用 query string 指定 API：
  - `http://localhost:5173/?api=http://localhost:8000`

前端程式碼節錄（`web/app.js`）：

```js
const API_BASE = (() => {
  const override = new URLSearchParams(window.location.search).get("api");
  if (override) return override.replace(/\/$/, "");
  if (window.location.port === "8000") return window.location.origin;
  return `${window.location.protocol}//${window.location.hostname}:8000`;
})();
```

新手重點：

- `override`：讓你不改程式碼就能切 API host（非常適合本機開發）
- `replace(/\/$/, "")`：把結尾的 `/` 去掉，避免拼 URL 時出現 `//`

---

## 7) 常見錯誤與排查（新手最常卡的地方）

1) **`ModuleNotFoundError: trafficpulse`**
   - 你可能忘了 `pip install -e .`
   - 或你沒有啟用 venv（`source .venv/bin/activate`）
2) **TDX 授權錯誤（401/403）**
   - `.env` 沒填好或有多餘空白
   - 你的憑證可能沒有權限存取該 endpoint（可先換城市或縮小時間窗）
3) **資料抓不回來 / 很慢**
   - 調小 `ingestion.query_chunk_minutes`
   - 先抓 30 分鐘驗證（不要一開始抓 7 天）
4) **Dashboard 沒有任何路段可選**
   - `data/processed/segments.csv` 不存在或為空
   - 你抓的時間窗可能沒有資料（TDX 有時歷史資料不完整）

---

## 8) 本階段驗收方式（中文說明 + 英文指令）

你可以用下面指令做「可重現驗收」。

1) Python 檔案語法檢查（編譯所有檔案）：

```bash
python -m compileall -q src scripts
```

2) 確認 FastAPI app 可以 import：

```bash
PYTHONPATH=src python -c "from trafficpulse.api.app import app; print('routes=', len(app.routes))"
```

3)（可選）啟動 API 後用 curl 驗證：

```bash
curl -s http://localhost:8000/ui/settings | head
curl -s http://localhost:8000/segments | head
```

> 你如果是在 Windows，`curl` 指令仍可用（PowerShell 通常也有）；或你可以直接開 `http://localhost:8000/docs` 用瀏覽器點。

---

## 9) 下一步（你接下來可以學什麼？）

當你 Phase 1 跑順之後，下一個最自然的學習路線是：

1. 看 `src/trafficpulse/ingestion/tdx_traffic_client.py`，理解 time window chunking、快取（cache）、重試（retry）等資料工程基本功。
2. 看 `src/trafficpulse/api/` 的各個路由模組，理解「把資料/分析結果包成 API」的方式。
3. 再看 `web/app.js`，理解前端如何把地圖互動、查詢、圖表更新串起來（DOM + data flow）。

