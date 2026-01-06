# TrafficPulse 教學 03：Phase 3 — Ingestion（TDX Traffic Client：時間窗切片、分頁、重試、快取、欄位正規化）

> 這一份教學要帶你讀懂 `src/trafficpulse/ingestion/tdx_traffic_client.py`：它是整個專案的「資料入口」之一，負責把 TDX 的 VD/事件資料抓回來，並整理成我們後續分析與 API 能穩定使用的 DataFrame 格式。
>
> 你會學到的重點，不只是「怎麼呼叫 API」，而是資料工程常見的可靠性設計：
>
> - 以時間窗切片（chunking）避免超大請求
> - OData filter 組字串的技巧與坑
> - 分頁（pagination）用 `$top` / `$skip` 把資料抓完整
> - 重試（retry）與退避（backoff）避免偶發失敗
> - 本地快取（file cache）降低重複抓取成本
> - 欄位正規化（normalization）把不穩定的外部欄位，轉成穩定的內部 schema

---

## 1) 本階段目標（中文）

完成本階段後，你應該能：

1. 清楚知道 `TdxTrafficClient` 在整個資料流中扮演什麼角色。
2. 理解這支 client 如何做到「可重現、可調參數、可除錯」：
   - 時間窗切片（`ingestion.query_chunk_minutes`）
   - 分頁（`paging.page_size`）
   - 重試（`tdx.max_retries` + `tdx.retry_backoff_seconds`）
   - 快取（`cache.ttl_seconds`）
3. 能讀懂：VD / Events 兩種資料的抓取與正規化流程。
4. 理解我們內部使用的「核心表 schema」概念：
   - `segments`（路段/偵測器的靜態資訊）
   - `observations`（時間序列的速度/流量/占有率）
   - `events`（交通事件的起訖時間與位置）

> 重要：本階段只改「註解與文件」，不改行為（behavior），確保既有功能不被破壞。

---

## 2) 本階段改了哪些檔案？為什麼？（中文）

- 修改 `src/trafficpulse/ingestion/tdx_traffic_client.py`
  - 補齊逐行英文註解（line-by-line English comments）。
  - 讓初學者能理解資料工程的可靠性設計：cache/retry/pagination/chunking。
- 新增 `docs/03_phase3_tdx_client.md`
  - 對應此模組的中文超詳細導讀（就是本文件）。

為什麼要先做這個模組？

- ingestion 是整個專案的起點；你不理解它，就很難理解後面 preprocessing / analytics / API 為什麼要這樣寫。
- 這個模組包含「資料工程必修題」：重試、快取、時間處理、欄位不一致的容錯。

---

## 3) 核心概念講解（中文，含術語中英對照）

### 3.1 `TdxTrafficClient` 在資料流的位置

你可以用一句話記住：

- `tdx_auth.py` 負責拿 token  
- `tdx_traffic_client.py` 負責帶 token 去抓資料 + 把資料整理成內部表格

也就是：

```text
.env -> (TDX credentials)
  -> tdx_auth.py (get token)
  -> tdx_traffic_client.py (fetch raw JSON + normalize)
  -> scripts/ (save CSV/Parquet)
  -> api/ (query + serve)
  -> web/ (visualize)
```

### 3.2 OData 是什麼？為什麼我們一直看到 `$filter/$top/$skip`？

TDX 的 Basic API 很多端點採用 OData query 風格，所以你會在 URL query 看到：

- `$filter`：過濾條件（例如時間窗）
- `$top`：每頁回傳幾筆（page size）
- `$skip`：跳過前面幾筆（用來翻頁）
- `$format=JSON`：要求回 JSON

術語中英對照：

- filter（過濾）
- pagination（分頁）
- page size（每頁大小）
- cursor（游標；本模組用 `$skip` 當作游標）

### 3.3 時間窗（time window）與時區（timezone）

資料工程很常爆炸的原因是「時區」。本專案採用一個簡單的規則：

- **所有對外的時間**（TDX 查詢）會轉成 UTC `Z` 格式
- **所有內部 timestamp**（DataFrame）也統一轉成 UTC（`pd.to_datetime(..., utc=True)`）

這樣的好處是：

- 不會因為本機時區/夏令時間造成資料對不齊
- 後續聚合（15-min/hourly）更可控

你會看到 filter 是這種形式（概念）：

```text
DataCollectTime ge 2026-01-01T00:00:00Z and DataCollectTime lt 2026-01-01T01:00:00Z
```

注意：我們用的是 **start inclusive / end exclusive**（`ge` / `lt`）：

- `ge`（>=）：包含 start
- `lt`（<）：不包含 end

這個設計可以避免 chunking 時「邊界重複」造成重複資料。

### 3.4 為什麼要做時間窗切片（chunking）？

如果你一次抓 7 天資料：

- response 可能非常大 → timeout、記憶體爆炸、或被服務端限制
- 失敗重試成本很高（你得重抓整段）

所以我們用 config 控制 chunk 大小：

- `ingestion.query_chunk_minutes: 60`（預設每次抓 1 小時）

好處：

- 單次請求小、可控
- 失敗時只重試小 chunk
- 能逐步落地（更好 debug）

### 3.5 為什麼要做快取（FileCache）？

在開發/除錯時，你會反覆跑同一段時間窗（尤其是短窗）。

如果每次都打 TDX：

- 很慢
- 很容易撞 rate limit
- 失敗原因不容易分辨（是你的 code 壞？還是網路抖？）

所以這個 client 在 `_request_json()` 用 `FileCache` 快取「同一個 endpoint + 同一組 params」的結果。

你可以在 `configs/config.yaml` 控制：

- `cache.enabled`
- `cache.ttl_seconds`

> 常見坑：如果你在抓 live 資料但 TTL 設太長，你可能一直看到舊資料；要調小 TTL 或暫時關閉 cache。

### 3.6 欄位正規化（normalization）是什麼？

外部資料（TDX JSON）常見問題：

- 欄位名稱可能變（或不同端點略不同）
- 欄位型別可能不一致（字串/數字/空值）
- 嵌套結構可能有/可能沒有（例如 VD 的 lane list）

所以我們把「外部欄位」透過 config 定義，並轉成「內部穩定 schema」：

- `segments` 表：`segment_id, name, direction, road_name, link_id, lat, lon, city`
- `observations` 表：`timestamp, segment_id, speed_kph, volume, occupancy_pct`
- `events` 表：`event_id, start_time, end_time, event_type, description, lat, lon, ...`

這樣後面的 preprocessing/analytics/api 就不用一直處理外部格式差異。

---

## 4) 程式碼分區塊貼上 + 逐段解釋（中文）

本段會挑幾個「最重要的段落」貼出來，並用初學者能懂的方式逐段講解。  
完整程式碼請看：`src/trafficpulse/ingestion/tdx_traffic_client.py`

---

### 4.1 `ODataQuery`：把 request 參數做成可快取的 key

```python
@dataclass(frozen=True)
class ODataQuery:
    endpoint: str
    params: dict[str, Any]

    def cache_key(self) -> str:
        return json.dumps({"endpoint": self.endpoint, "params": self.params}, sort_keys=True)
```

你在看什麼？

- `endpoint + params` 決定了一個「唯一的請求」。
- 我們把它 json dump 並 `sort_keys=True`，確保 key 穩定：
  - 同一組 params 就算 dict 順序不同，dump 後也會一致。

為什麼重要？

- File cache 的本質就是：**同一個請求不要重打外部 API**。

---

### 4.2 `_build_time_filter()`：把 datetime 轉成 OData filter 字串

```python
def _isoformat_z(dt: datetime) -> str:
    utc = to_utc(dt)
    return utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _build_time_filter(field: str, start: datetime, end: datetime) -> str:
    start_text = _isoformat_z(start)
    end_text = _isoformat_z(end)
    return f"{field} ge {start_text} and {field} lt {end_text}"
```

重點：

- `_isoformat_z()` 做兩件事：
  1) 轉成 UTC
  2) 把 `+00:00` 換成 `Z`
- filter 用 `ge`/`lt`（start inclusive/end exclusive），避免 chunk 重疊造成重複資料。

常見坑：

- 如果你用 `le`（<=）或 end inclusive，很容易在 chunk 邊界產生重複。

---

### 4.3 `TdxTrafficClient.__init__()`：建立 HTTP client、token provider、cache

你會看到概念像這樣（節錄）：

```python
self._http = http_client or httpx.Client(
    base_url=self.config.tdx.base_url,
    timeout=self.config.tdx.request_timeout_seconds,
    headers={"accept": "application/json"},
)

client_id, client_secret = load_tdx_credentials()
self._auth_http = httpx.Client(timeout=self.config.tdx.request_timeout_seconds)
self._token_provider = TdxTokenProvider(...)

self._cache = FileCache(...)
```

重點：

- `_http`：用來打「資料 API」（base_url 是 `api/basic/v2`）
- `_auth_http`：用來打「token endpoint」（避免混在一起，並且可獨立管理 timeout）
- `_token_provider`：需要 token 時自動 refresh
- `_cache`：快取每個 endpoint+params 的結果

資料工程常見坑：

- 忘記關閉 client → 連線資源泄漏（resource leak）。  
  所以 `TdxTrafficClient.close()` 會把 `_http/_auth_http` 都 close。

---

### 4.4 `_request_json()`：快取 + 授權 header + 重試

這段是 ingestion 可靠性的核心（節錄）：

```python
cached = self._cache.get_json("tdx", query.cache_key())
if isinstance(cached, list):
    return cached

token = self._token_provider.get_access_token()
headers = {"authorization": f"Bearer {token}"}

for attempt in range(max_retries + 1):
    try:
        response = self._http.get(query.endpoint, params=query.params, headers=headers)
        response.raise_for_status()
        payload = response.json()
        items = self._extract_items(payload)
        self._cache.set_json("tdx", query.cache_key(), items)
        return items
    except Exception as exc:
        ...
        time.sleep(backoff_seconds * (2**attempt))
```

你要記住的設計意圖：

- **先看 cache**：開發時同一段時間窗反覆跑很常見。
- **再拿 token**：每個資料 request 都要授權。
- **重試 + 指數退避**：外部 API 偶發失敗很正常；我們要用 config 控制重試策略。
- **成功後寫回 cache**：下次同樣 query 直接讀本地。

常見坑：

- `max_retries` 設太大會拖慢失敗回報；設太小會讓偶發失敗變成 hard fail。  
  這就是為什麼重試參數要放 config。

---

### 4.5 `_fetch_vd_city_raw()`：時間窗切片（chunking）

```python
cursor = start
while cursor < end:
    chunk_end = min(cursor + timedelta(minutes=chunk_minutes), end)
    results.extend(self._fetch_vd_city_chunk_raw(city=city, start=cursor, end=chunk_end))
    cursor = chunk_end
```

你在看什麼？

- `cursor` 從 start 往 end 推進，每次推進 `chunk_minutes`。
- 每個 chunk 都是一個可控、可重試的請求範圍。

常見坑：

- chunk_minutes <= 0 會造成死迴圈或邏輯錯，所以 code 會 `raise ValueError`。

---

### 4.6 `_fetch_paginated()`：用 `$skip` 把所有頁都抓完

```python
items: list[dict[str, Any]] = []
skip = 0
while True:
    params = dict(base_params)
    params["$skip"] = skip
    page = self._request_json(ODataQuery(endpoint=endpoint, params=params))
    items.extend(page)
    if len(page) < page_size:
        break
    skip += page_size
return items
```

重點：

- `page_size` 是 `$top`
- `skip` 每次加 `page_size`，直到最後一頁小於 page_size

常見坑：

- 服務端如果不保證穩定排序，你可能會遇到翻頁重複/遺漏；通常 API 會有固定順序，但如果遇到問題要加上排序參數（TDX 是否支援需查資料）。

---

### 4.7 `download_vd()`：把 raw JSON 變成 `segments` 與 `observations`

你可以把它理解成三步：

1) 對每個 city 抓 raw  
2) 正規化成 DataFrame（欄位一致）  
3) 合併跨 city 的結果，並清理 timestamp

---

## 5) 常見錯誤與排查（中文）

1) 抓不到資料（結果是空 DataFrame）
   - 先確認時間窗是否真的有資料（TDX 歷史資料可能不完整）
   - 改小時間窗驗證（例如 30 分鐘）
2) 401/403
   - `.env` 憑證錯或 token 失效
   - 確認你沒有把 cache 的舊結果誤認為新資料（可暫時關 cache）
3) Timeout
   - 調小 `ingestion.query_chunk_minutes`
   - 調大 `tdx.request_timeout_seconds`
4) schema mismatch（欄位找不到）
   - TDX 端點回傳欄位可能不同，請檢查 `configs/config.yaml` 的欄位 mapping
   - 尤其是 `time_field/segment_id_field/lane_list_field` 等

---

## 6) 本階段驗收方式（中文 + 英文命令）

1) Compile sources:

```bash
python -m compileall -q src scripts
```

Expected:

- No output
- Exit code 0

2) Import the client:

```bash
PYTHONPATH=src python -c "from trafficpulse.ingestion.tdx_traffic_client import TdxTrafficClient; print('ok=', TdxTrafficClient.__name__)"
```

Expected:

- Prints `ok= TdxTrafficClient`
- Exit code 0

3) (Optional, requires network + valid TDX credentials) Run a tiny download:

```bash
python scripts/build_dataset.py --start 2026-01-01T00:00:00+08:00 --end 2026-01-01T00:30:00+08:00 --cities Taipei
```

Expected:

- Writes `data/processed/segments.csv` and `data/processed/observations_5m.csv`
- No exceptions in console

---

## 7) 下一步（中文）

如果你確認本階段 OK，下一個 Phase 我建議做：

- `src/trafficpulse/ingestion/tdx_traffic_client.py` 的「欄位 schema」再進一步文件化（把 `segments/observations/events` 的欄位定義整理成表格）
- 或直接進入 preprocessing：`src/trafficpulse/preprocessing/aggregation.py` 的逐行註解 + 新增對應教學文件

