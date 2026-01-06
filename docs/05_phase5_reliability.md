# TrafficPulse 教學 05：Phase 5 — Analytics（Reliability：mean/std/壅塞頻率 + 加權排行）

> 這份文件對應 `src/trafficpulse/analytics/reliability.py`。  
> 目標是讓初學者能理解：TrafficPulse 的「可靠度（reliability）」指標是怎麼從速度時間序列算出來的，並且如何用可解釋、config-driven 的方式做排行（ranking）。

---

## 1) 本階段目標（中文）

完成本階段後，你應該能：

1. 知道 reliability 這個模組在做什麼：把觀測資料轉成「可解釋指標」與「排序分數」。
2. 理解我們使用的三個 MVP 指標（Explainable, no-ML）：
   - mean speed（平均速度）
   - speed std（速度變異，代表不穩定）
   - congestion frequency（壅塞頻率：速度低於門檻的比例）
3. 理解「為什麼要轉成 penalty 再加權」：
   - 不同指標尺度不同（kph vs ratio）
   - 用 percentile rank 可以把尺度拉到可比較的 0..1
4. 知道 `min_samples` 的意義：避免樣本太少造成排名不可靠。
5. 知道如何用 config 控制門檻與權重，以及 UI 如何透過 API 覆寫。

> 重要：本階段只加註解與文件，不改行為（behavior）。

---

## 2) 本階段改了哪些檔案？（中文）

- 修改 `src/trafficpulse/analytics/reliability.py`
  - 補上逐行英文註解，讓初學者能理解每個步驟做什麼、為什麼要這樣做。
- 新增 `docs/05_phase5_reliability.md`
  - 本文件：中文超詳細導讀 + 驗收方式。

---

## 3) Reliability 在資料流中的位置（中文）

```text
observations (timestamp, segment_id, speed_kph, ...)
  -> compute_reliability_metrics(): 產生每個 segment 的統計指標
  -> add_reliability_score(): 把指標轉成 penalties + 加權分數
  -> compute_reliability_rankings(): 排序 + 產生 rank
  -> API /rankings/reliability: 提供 dashboard 顯示
```

---

## 4) Config 驅動：門檻/權重/樣本數從哪裡來？（中文）

在 `configs/config.yaml` 的 `analytics.reliability`：

```yaml
analytics:
  reliability:
    congestion_speed_threshold_kph: 30
    min_samples: 12
    weights:
      mean_speed: 0.4
      speed_std: 0.3
      congestion_frequency: 0.3
```

直覺理解：

- `congestion_speed_threshold_kph`：小於這個速度就算壅塞（congested）
- `min_samples`：一個 segment 至少要有多少筆時間點才參與排名
- `weights`：三個 penalty 的加權組合

> 注意：權重會被 normalized（總和變成 1.0），避免你填 4/3/3 或 40/30/30 造成比例不同但意義一樣。

---

## 5) 核心指標定義（中文）

### 5.1 mean speed（平均速度）

- 越低代表越慢 → 通常越不可靠（但也要看場景）

### 5.2 speed std（速度標準差）

- 越高代表波動越大 → 代表不穩定（不可靠）

### 5.3 congestion frequency（壅塞頻率）

定義：

```text
congestion_frequency = mean( speed_kph < congestion_threshold )
```

- 如果有 100 個時間點，其中 30 個點速度 < 門檻 → congestion_frequency = 0.30

---

## 6) 為什麼要用 penalty + percentile rank？（中文）

如果你直接加權 mean/std/frequency：

- mean 是 kph（數值可能 10..100）
- std 是 kph（數值可能 0..30）
- frequency 是 ratio（0..1）

尺度不同會讓「某個指標」主宰分數。

因此我們用 percentile rank 轉成 penalty（0..1）：

- mean speed：越慢越糟，所以 penalty = 1 - rank(mean_speed, ascending=True)
- speed std：越大越糟，所以 penalty = rank(std, ascending=True)
- congestion frequency：越大越糟，所以 penalty = rank(freq, ascending=True)

最後加權：

```text
score = w1*penalty_mean_speed + w2*penalty_speed_std + w3*penalty_congestion_frequency
```

這樣的好處：

- 可解釋：每個 penalty 都是「相對於其他路段」的百分位
- 尺度一致：都落在 0..1

---

## 7) 程式碼分段貼上 + 逐段解釋（中文）

對應檔案：

- `src/trafficpulse/analytics/reliability.py`

### 7.1 `ReliabilitySpec` 與 normalized weights

重點：

- spec 會把權重 normalize，避免權重總和不是 1
- 若總和 <= 0，會 fallback 到平均分配（1/3, 1/3, 1/3）

### 7.2 `compute_reliability_metrics()`

重點：

- 先把 timestamp/segment_id 清理成標準格式（UTC + str id）
- 可以用 `start/end` 過濾時間窗（start inclusive / end exclusive）
- 計算：
  - n_samples
  - mean_speed_kph
  - speed_std_kph（空值填 0）
  - congestion_frequency（空值填 0）

### 7.3 `add_reliability_score()`

重點：

- 先過濾 `n_samples >= min_samples`
- 只對 eligible segments 計算 percentile penalties
- merge 回原本 metrics，讓不足樣本的 segment score = NA

### 7.4 `apply_reliability_overrides()`

重點：

- 前端 UI 可以透過 query params 覆寫 threshold/weights/min_samples
- 會做基本合法性檢查（例如 threshold > 0、weights >= 0）
- 覆寫後仍會 normalize weights

---

## 8) 常見錯誤與排查（中文）

1) 排名結果是空的
   - 可能是 `min_samples` 太大，導致 eligible 為空
   - 或你過濾的 `start/end` 時間窗沒有資料
2) congestion_frequency 全是 0
   - 可能 threshold 設太低
   - 或 speed 全是缺值被 drop 掉
3) 速度 std 都是 0
   - 可能每個 segment 在時間窗內只有 1 筆樣本（std 會是 NaN → 我們 fillna(0))

---

## 9) 本階段驗收方式（中文 + 英文命令）

1) Compile sources:

```bash
python -m compileall -q src scripts
```

Expected: no output, exit code 0.

2) Import reliability functions:

```bash
PYTHONPATH=src python -c \"from trafficpulse.analytics.reliability import compute_reliability_rankings; print('ok=', compute_reliability_rankings.__name__)\"
```

Expected: prints `ok= compute_reliability_rankings`, exit code 0.

3) (Optional) Run rankings script if you have observations built:

```bash
python scripts/build_reliability_rankings.py --limit 50
```

Expected: writes ranking outputs under `outputs/` and prints row counts.

---

## 10) 下一步（中文）

如果你確認本階段 OK，下一個 Phase 建議：

- `api/routes_rankings.py` 的逐行英文註解 + 對應教學文件（API 如何接上 reliability 模組）

