# TrafficPulse 教學 00：專案總覽（Overview）

> 這個 repo 的「程式碼 / 註解 / README / commit message」全部使用英文；但本 `docs/` 目錄下的教學文件會用繁體中文，目標是讓初學者也能一步一步看懂整個專案在做什麼、為什麼要這樣設計。

---

## 1) 本階段目標（Phase 0）

這份文件（Phase 0）要達成三件事：

1. **讓你在 10 分鐘內建立專案的心智模型**：你會知道資料從哪裡來、怎麼被處理、最後怎麼被 API 與前端呈現。
2. **把專案的「分層」說清楚**：`ingestion → preprocessing → analytics → api → web`，每一層的責任邊界要明確。
3. **建立後續教學文件的閱讀路徑**：你應該先看哪些檔案、再看哪些檔案，才不會迷路。

> 重要：本階段只做「理解與導覽」，不新增功能、不改行為（behavior），避免一次大爆改。

---

## 2) 你改了哪些檔案？為什麼這樣拆？（本階段變更範圍）

本階段（Phase 0）會新增/修改以下檔案：

- 新增 `docs/00_overview.md`：專案總覽（就是你正在看的這份）。
- 新增 `docs/01_phase1_bootstrap.md`：Phase 1 的環境建置與跑起來（下一份文件）。
- 修改 `src/trafficpulse/api/routes_ui.py`：補上逐行英文註解，讓「前端 Controls 面板」如何取得預設參數更好理解。

為什麼這樣拆？

- **總覽（overview）** 與 **實作步驟（bootstrap）** 是兩種不同的學習任務：前者建立地圖，後者照地圖走路。
- 我們刻意先從一個小而關鍵的模組（`routes_ui.py`）開始加註解，避免「為了加註解而動到大量檔案」造成風險與審查負擔。

---

## 3) 一張圖看懂資料流（Data Flow）

TrafficPulse 的資料流可以用下面這張「管線圖」理解：

```text
TDX (Transport Data eXchange)
        |
        v
ingestion/            (抓資料 + 正規化欄位 + 快取/重試)
        |
        v
preprocessing/        (時間聚合：5-min -> 15-min 或 hourly)
        |
        v
analytics/            (可解釋統計：可靠度、走廊、異常、事件影響)
        |
        v
storage/              (CSV + optional Parquet + DuckDB 查詢)
        |
        v
api/ (FastAPI)        (把資料/指標包成可查詢的 endpoints)
        |
        v
web/ (Leaflet+Plotly) (地圖互動 + 時序圖 + 控制面板)
```

### 3.1 關鍵術語中英對照（新手友善）

- **Ingestion（資料擷取/導入）**：從外部 API 把資料拿回來，轉成我們自己「一致的欄位格式」。
- **Preprocessing（前處理）**：把原始資料整理成後續分析更好用的形式，例如做時間聚合、補缺值策略等。
- **Analytics（分析）**：先做可解釋的統計指標（mean/std/frequency），不要一開始用複雜 ML。
- **API（後端介面）**：把資料與分析結果用 HTTP endpoints 提供給前端或其他工具使用。
- **Web Dashboard（前端儀表板）**：用地圖和圖表把 API 回傳的資料視覺化，並提供互動（點選路段、調參數、快捷鍵）。

---

## 4) 目錄結構導覽（你應該先看哪些資料夾？）

> 這段的目標是：你看到檔案樹時知道「這裡放什麼」，避免把所有東西混在一起。

```text
configs/          # YAML configs (copy examples into real configs)
data/             # Local data (raw/processed/cache) - ignored by git
outputs/          # Reports/figures exported by scripts
scripts/          # CLI helpers to build datasets and derived analytics
src/trafficpulse/ # Python package (ingestion/preprocessing/analytics/api/storage/utils)
web/              # Minimal static dashboard (Leaflet map + Plotly chart)
docs/             # (NEW) Chinese tutorial docs for beginners
```

你可以把它想成「三個面向」：

1. **Config（configs/ + .env）**：所有可調參數都要在 config，避免硬編碼。
2. **Data（data/ + outputs/）**：資料落地的位置（本機），用來重現與除錯。
3. **Code（src/ + scripts/ + web/ + api）**：真正做事的程式。

---

## 5) Config 驅動（Config-driven）設計：為什麼這樣做？

這個專案的核心原則之一是：**時間窗、聚合粒度、權重等參數都放在 config，不要硬編碼。**

這樣做的好處：

- 你想調整分析參數（例如可靠度權重）時，不需要改程式碼 → 風險更低，也更可重現。
- 前端 Controls 面板可以先讀 `GET /ui/settings` 的預設值，再讓使用者「覆寫」成自訂值。
- 對資料工程很重要：同樣的 code + 同樣的 config + 同樣的輸入資料 = 可重現結果（reproducibility）。

### 5.1 `configs/config.example.yaml` 節錄（理解你會調哪些）

下面這段示範了最常會調的幾類設定：

```yaml
preprocessing:
  source_granularity_minutes: 5
  target_granularity_minutes: 15

analytics:
  reliability:
    congestion_speed_threshold_kph: 30
    min_samples: 12
    weights:
      mean_speed: 0.4
      speed_std: 0.3
      congestion_frequency: 0.3

warehouse:
  enabled: false
  parquet_dir: data/processed/parquet
  use_duckdb: true
```

你先用直覺理解即可：

- `preprocessing.target_granularity_minutes`：決定你要把 5 分鐘資料聚合成 15 分鐘或 60 分鐘（hourly）。
- `analytics.reliability.weights`：可靠度指標的加權方式（越高代表越重視）。
- `warehouse.enabled`：是否啟用「Parquet + DuckDB」（免外部服務的資料倉儲風格查詢）。

> 小提醒：`configs/config.example.yaml` 是範本；你通常會複製成 `configs/config.yaml` 再修改。

---

## 6) 後端 FastAPI：路由（routes）怎麼組起來的？

FastAPI 的入口在 `src/trafficpulse/api/app.py`。這支檔案做兩件大事：

1. 設定 CORS（讓瀏覽器允許前端呼叫 API）
2. 把各個路由模組（segments/timeseries/rankings/...）掛到同一個 app 上

### 6.1 `src/trafficpulse/api/app.py` 節錄

```python
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from trafficpulse.api.routes_ui import router as ui_router
from trafficpulse.settings import project_root

def create_app() -> FastAPI:
    app = FastAPI(title="TrafficPulse API", version="0.1.0")
    app.include_router(ui_router, tags=["ui"])

    web_dir = project_root() / "web"
    if web_dir.exists():
        app.mount("/", StaticFiles(directory=str(web_dir), html=True), name="web")
    return app
```

逐段解釋（新手版）：

- `include_router(...)`：把「某一組 endpoints」加進 API。你可以把 router 想成一個「小型子應用」。
- `StaticFiles(..., html=True)`：把 `web/` 當作靜態網站直接服務，所以你跑 API 的同時，瀏覽器打開 `http://localhost:8000/` 就能看到 dashboard。
- 這個設計的好處是 **MVP 很快**：不需要先做 Node build pipeline，也不需要額外部署靜態站。

---

## 7) 前端 Dashboard：為什麼需要 `GET /ui/settings`？

在 Phase 6 加入了 Unity-like Controls 面板：你可以在前端調整可靠度權重、異常偵測門檻、事件影響半徑等。

問題來了：**前端要怎麼知道預設值？**

答案：讓後端提供一個「單一來源的預設設定」（single source of truth）：

- 前端開啟時會呼叫：`GET /ui/settings`
- 後端回傳目前 `configs/config.yaml`（或預設 config）裡的值
- 前端把這些值填進表單的 placeholder，並允許使用者覆寫

### 7.1 `web/app.js` 節錄：前端抓預設設定

```js
uiDefaults = await fetchJson(`${API_BASE}/ui/settings`);
```

### 7.2 `src/trafficpulse/api/routes_ui.py` 節錄：後端提供預設設定

```python
@router.get("/ui/settings", response_model=UiSettings)
def ui_settings() -> UiSettings:
    config = get_config()
    ...
    return UiSettings(...)
```

你可以把它想成：

- `config.yaml` 是「你想要的預設」
- `/ui/settings` 是「把預設交給前端」
- 前端 Controls 面板是「讓人類在 runtime 調整參數」

> 常見坑：如果你在後端新增了 config 欄位，卻忘記更新 `/ui/settings`，前端就會拿不到新欄位的預設值，造成 UI 與後端行為不一致。這也是我們為什麼要把這個 endpoint 寫得清楚、並加上詳細註解。

---

## 8) 常見錯誤與排查（Troubleshooting）

這裡列一些新手最容易卡住的點，後續文件會更詳細教你怎麼排：

1. **TDX 401 / 403（授權失敗）**
   - 檢查 `.env` 是否有填 `TDX_CLIENT_ID`、`TDX_CLIENT_SECRET`
   - 確認你有啟用虛擬環境，並且安裝了 `python-dotenv`
2. **抓資料很慢或 timeout**
   - 調小 `ingestion.query_chunk_minutes`（例如 60 → 30）
   - 先縮短 `--start/--end` 時間窗做小量驗證
3. **前端開得起來但沒有資料**
   - 你可能還沒跑 `scripts/build_dataset.py`，所以 `data/processed/` 沒有 segments/observations
   - 或者 API 讀取路徑設定錯（看 `configs/config.yaml` 的 `paths.processed_dir`）

---

## 9) 本階段驗收方式（可重現 + 可驗收）

你可以用下面的指令快速確認 Phase 0 的內容「不改行為但可跑」。

> 指令用英文列出，符合 repo 規範；說明文字用中文幫你理解。

1) 檢查 Python 檔案語法是否可編譯：

```bash
python -m compileall -q src scripts
```

預期結果：

- 指令沒有輸出錯誤（空白是正常的）
- exit code 為 0

2) 快速確認 FastAPI app 可以被 import（避免啟動才爆炸）：

```bash
PYTHONPATH=src python -c "from trafficpulse.api.app import app; print('routes=', len(app.routes))"
```

預期結果：

- 會印出類似 `routes= ...` 的數字

---

## 10) 下一步你應該看哪一份？

下一份建議閱讀：

- `docs/01_phase1_bootstrap.md`：把專案在你的電腦上「真的跑起來」，包含抓 TDX、輸出資料、啟動 API、打開 dashboard。

