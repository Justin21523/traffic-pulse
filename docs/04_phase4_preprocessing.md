# TrafficPulse 教學 04：Phase 4 — Preprocessing（時間聚合：5-min → 15-min / Hourly，含 volume-weighted mean）

> 這一份教學對應 `src/trafficpulse/preprocessing/aggregation.py`。  
> 目標是讓初學者能理解：我們如何把「細粒度的觀測」（例如 5 分鐘一筆）聚合成更粗粒度（例如 15 分鐘或 60 分鐘），並且用 config 決定每個欄位的聚合方式（mean/sum/volume-weighted mean...）。

---

## 1) 本階段目標（中文）

完成本階段後，你應該能：

1. 理解「時間聚合（time aggregation）」在交通資料管線中為什麼重要。
2. 知道 TrafficPulse 的聚合行為完全由 config 控制（避免硬編碼）。
3. 讀懂 `aggregate_observations()` 的整體流程：
   - 欄位檢查
   - timestamp 標準化（UTC）
   - bucket（時間桶）計算
   - groupby 聚合
   - volume-weighted mean 的額外處理
4. 知道常見坑：timezone、缺值、volume=0、欄位型別（字串 vs 數字）等。

> 重要：本階段只新增註解與文件，不改行為（behavior）。

---

## 2) 本階段改了哪些檔案？（中文）

- 修改 `src/trafficpulse/preprocessing/aggregation.py`
  - 補上逐行英文註解，讓初學者能理解每個步驟做什麼、為什麼這樣做。
- 新增 `docs/04_phase4_preprocessing.md`
  - 本文件：中文超詳細導讀 + 驗收步驟。

---

## 3) Preprocessing 在資料流中的位置（中文）

你可以把 preprocessing 想成「把資料整理成分析友善的刻度」：

```text
TDX raw 5-min observations
  -> (ingestion) download + normalize to DataFrame
  -> (preprocessing) aggregate to 15-min / hourly (config-driven)
  -> (analytics) reliability/anomalies/event-impact
  -> (api) serve to dashboard
```

為什麼一定要做聚合？

- 地圖 dashboard 與排行通常不需要最細粒度（5-min）才能做決策。
- 聚合後資料量更小 → 查詢更快、API 更穩、前端更順。
- 設定 target granularity（例如 15-min）可以讓不同功能使用一致的時間刻度，避免「同一個問題，用不同刻度算出不同答案」。

---

## 4) Config 驅動：聚合規則從哪裡來？（中文）

聚合的核心配置在 `configs/config.yaml`（或範本 `configs/config.example.yaml`）：

```yaml
preprocessing:
  source_granularity_minutes: 5
  target_granularity_minutes: 15
  aggregation:
    speed_kph: mean
    volume: sum
    occupancy_pct: mean
```

你可以用直覺理解：

- `speed_kph: mean`：在一個 15-min bucket 裡，把 3 個 5-min 的 speed 取平均
- `volume: sum`：把 3 個 5-min 的流量加總成 15-min 流量
- `occupancy_pct: mean`：占有率通常取平均

進階：volume-weighted mean

```yaml
preprocessing:
  aggregation:
    speed_kph: volume_weighted_mean
```

這代表：

- 速度的平均不是每個 lane/每個 time point 等權
- 速度會依 volume 加權（更符合「車多的地方影響更大」的直覺）

> 常見坑：如果你要用 volume-weighted mean，但資料沒有 volume 欄位，程式會丟錯，因為加權平均的分母需要 volume。

---

## 5) 程式碼分段貼上 + 逐段解釋（中文）

對應檔案：

- `src/trafficpulse/preprocessing/aggregation.py`

---

### 5.1 `AggregationSpec`：聚合規格（哪些欄位、用什麼聚合、哪個欄位是 timestamp）

節錄：

```python
@dataclass(frozen=True)
class AggregationSpec:
    target_granularity_minutes: int
    aggregations: dict[str, str]
    timestamp_column: str = "timestamp"
    segment_id_column: str = "segment_id"
    volume_column: str = "volume"
```

解釋：

- 我們把「聚合規則」封裝成一個 spec，避免把很多參數散落在函式參數裡。
- `timestamp_column/segment_id_column` 是必要欄位：
  - timestamp：決定 bucket
  - segment_id：決定每條路段分開算
- `volume_column` 是為了 `volume_weighted_mean` 預留（你可以在不同資料集換欄位名）。

---

### 5.2 `aggregate_observations()`：主流程（清理 → bucket → groupby → 合併加權結果）

你可以把它分成 6 步：

1) 快速退出（空表）
2) spec 驗證（target granularity、aggregation 名稱）
3) 欄位/型別標準化（timestamp/segment_id/numeric）
4) bucket 計算（`dt.floor("15min")`）
5) groupby 聚合（mean/sum/...）
6) volume-weighted mean 補算並 merge 回去

節錄（概念）：

```python
df[bucket_col] = df[ts_col].dt.floor(f"{spec.target_granularity_minutes}min")
aggregated = df.groupby([seg_col, bucket_col], as_index=False).agg(base_agg_map)
weighted = _aggregate_volume_weighted_means(...)
aggregated = aggregated.merge(weighted, on=[seg_col, bucket_col], how="left")
```

重點：

- `dt.floor("15min")` 會把 timestamp 對齊到時間桶的起點
- groupby key 是 `(segment_id, bucket)`，所以「每個路段、每個時間桶」會產生一筆聚合結果

常見坑：

- timestamp 沒有轉成 UTC 或 dtype 不對 → `dt.floor` 可能報錯或產生錯誤結果
- 欄位是字串（例如 `"12.3"`）但你當成數字算 → pandas 會做奇怪的事，所以我們先 `to_numeric(errors="coerce")`

---

### 5.3 `volume_weighted_mean` 是怎麼算的？（核心原理）

加權平均的公式：

```text
weighted_mean = sum(value * weight) / sum(weight)
```

套到我們這裡：

- value = speed_kph
- weight = volume

程式會做：

1. 建立 `wcol = speed * volume`
2. groupby 後把 `volume` 與 `wcol` 各自 sum
3. 用 `wcol_sum / volume_sum` 得到加權平均
4. 如果 volume_sum <= 0，就回傳 NA（避免除以 0）

節錄（概念）：

```python
work[wcol] = work[col] * work[volume_column]
grouped = work.groupby(group_cols, as_index=False).agg({volume_column: "sum", wcol: "sum"})
grouped[col] = grouped[wcol] / grouped[volume_column]
grouped.loc[grouped[volume_column] <= 0, col] = pd.NA
```

常見坑：

- volume 缺值或為 0 → 分母為 0 → 必須回 NA
- speed 缺值 → `speed * volume` 變 NaN → 會自然被 sum 當成 NaN/0（取決於 pandas 行為），所以我們要先 `to_numeric` + 容錯

---

## 6) 常見錯誤與排查（中文）

1) `Missing required columns: timestamp, segment_id`
   - 你輸入的 DataFrame 欄位名不是預設值
   - 解法：用 `build_aggregation_spec(..., timestamp_column="...", segment_id_column="...")` 指定
2) `Unsupported aggregations: ...`
   - 你在 config 寫了不支援的 aggregation 名稱
   - 解法：改成 `mean/sum/min/max/median/volume_weighted_mean`
3) `Requested volume_weighted_mean but missing volume column`
   - 你設定了 `volume_weighted_mean` 但資料沒有 volume 欄位
   - 解法：確保 ingestion 有產生 volume，或調整 `volume_column`
4) 聚合結果時間怪怪的
   - 很多時候是 timezone 沒對齊
   - 解法：確保 timestamp 都是 UTC（本模組會 `to_datetime(..., utc=True)`）

---

## 7) 本階段驗收方式（中文 + 英文命令）

1) Compile sources:

```bash
python -m compileall -q src scripts
```

Expected:

- No output
- Exit code 0

2) Import the preprocessing module:

```bash
PYTHONPATH=src python -c "from trafficpulse.preprocessing.aggregation import aggregate_observations; print('ok=', aggregate_observations.__name__)"
```

Expected:

- Prints `ok= aggregate_observations`
- Exit code 0

3) (Optional) Run the CLI aggregation script (requires you already built a dataset):

```bash
python scripts/aggregate_observations.py
```

Expected:

- Writes aggregated CSV under `data/processed/`
- If `warehouse.enabled: true`, also writes Parquet under `warehouse.parquet_dir`

---

## 8) 下一步（中文）

如果你確認這個 Phase OK，下一個最自然的 Phase 是：

- `analytics/reliability.py` 的逐行英文註解 + 對應教學文件（可靠度指標：mean/std/congestion frequency）

