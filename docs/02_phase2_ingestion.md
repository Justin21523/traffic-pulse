# TrafficPulse 教學 02：Phase 2 — Ingestion（TDX Auth：拿 Token、快取、避免踩坑）

> 這一份是「資料工程最常踩坑」的地方：**外部 API 的授權（OAuth token）**。  
> 我們會用超初學者視角，帶你理解 TrafficPulse 是怎麼從 `.env` 讀取 TDX 憑證、怎麼用 `client_credentials` 換取 access token、怎麼快取 token、以及有哪些常見錯誤要避開。

---

## 1) 本階段目標（中文）

完成本階段後，你應該能：

1. 清楚知道 `.env` 的 `TDX_CLIENT_ID/TDX_CLIENT_SECRET` 會在哪裡被讀取。
2. 理解 **OAuth 2.0 client credentials flow（用 client id/secret 換 token）** 的基本概念。
3. 理解為什麼要做 **token 快取（cache）** 與 **過期判斷（expiry）**。
4. 可以讀懂 `src/trafficpulse/ingestion/tdx_auth.py` 的每一行在做什麼、為什麼這樣寫。

> 重要：本階段只做「註解與教學」，不改任何功能行為，避免大爆改。

---

## 2) 本階段改了哪些檔案？（中文）

本階段（Phase 2 / Ingestion Auth）變更包含：

- 修改 `src/trafficpulse/ingestion/tdx_auth.py`  
  - 補齊逐行英文註解（line-by-line English comments）。
  - 讓初學者能理解 token 的生命週期與常見坑（例如：token 失效、時間差、資源釋放）。
- 新增 `docs/02_phase2_ingestion.md`  
  - 對應這個模組的中文超詳細導讀（就是你正在看的這份）。

---

## 3) 核心概念講解（中文，含術語中英對照）

### 3.1 什麼是 OAuth 2.0？TrafficPulse 用的是哪一種？

你先用一句話記住：

- **OAuth 2.0**：一套「安全取得 API 存取權」的標準流程。

TrafficPulse 在 TDX 這邊用的是：

- **Client Credentials Flow（客戶端憑證流程）**  
  - 你用 `client_id` + `client_secret` 去換一個短期的 **access token**  
  - 拿到 token 後，你每次呼叫資料 API 都在 HTTP header 帶上：
    - `Authorization: Bearer <token>`

術語中英對照（你會在 code 註解看到）：

- client id（客戶端 ID）
- client secret（客戶端密鑰）
- access token（存取權杖）
- expires_in（有效秒數）
- refresh（更新 token / 重新取 token）

### 3.2 為什麼要快取 token？不能每次都去換嗎？

理論上可以，但「不推薦」：

- 換 token 也是一個 HTTP request → 多一次延遲
- 服務端通常有 rate limit → 你頻繁換 token 可能被限制
- 增加失敗機率（網路抖動就會影響）

所以最佳實務是：

- token 在有效期內重複使用（cache）
- 快過期時再更新（refresh/renew）

### 3.3 為什麼需要「過期緩衝」（expiry buffer）？

真實世界很討厭的地方在於：

- 你的電腦時間跟伺服器時間可能有誤差（clock skew）
- token 可能在你送出 request 的途中就過期（race condition）

所以我們會在 code 裡做：

- `buffer_seconds = 30` 之類的緩衝  
  - **提早**把 token 當作「快過期了」  
  - 這能顯著降低 401/403 的偶發錯誤

---

## 4) 程式碼分區塊貼上 + 逐段解釋（中文）

本階段對應的 code 檔案是：

- `src/trafficpulse/ingestion/tdx_auth.py`

下面我會用「分段貼 code」+「逐段拆解」的方式帶你讀懂。

---

### 4.1 `OAuthToken`：把 token 與過期時間包成一個物件

程式碼節錄：

```python
@dataclass
class OAuthToken:
    access_token: str
    expires_at_epoch_seconds: float

    def is_expired(self, buffer_seconds: int = 30) -> bool:
        return time.time() >= (self.expires_at_epoch_seconds - buffer_seconds)
```

逐段解釋：

- `@dataclass`：Python 的語法糖，幫你自動產生 `__init__` 等 boilerplate，讓你專心在資料結構本身。
- `expires_at_epoch_seconds`：用 **epoch seconds**（從 1970-01-01 起算的秒數）存過期時間，原因是：
  - 比較簡單（就是數字比大小）
  - 不用煩惱時區（timezone）轉換
- `is_expired(buffer_seconds=30)`：這就是前面講的「過期緩衝」：
  - 不等到真正過期才換
  - 提早 30 秒視為過期，降低偶發授權錯誤

常見坑（新手很常中）：

- 你如果完全不加 buffer，可能會遇到「偶爾」401，還很難重現。

---

### 4.2 `load_tdx_credentials()`：從環境變數讀取 `.env` 的密鑰

程式碼節錄：

```python
def load_tdx_credentials() -> tuple[str, str]:
    client_id = os.getenv("TDX_CLIENT_ID", "").strip()
    client_secret = os.getenv("TDX_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise ValueError(
            "Missing TDX credentials. Set TDX_CLIENT_ID and TDX_CLIENT_SECRET in .env or environment variables."
        )
    return client_id, client_secret
```

逐段解釋：

- `os.getenv(...)`：讀環境變數。因為 `settings.py` 會（如果有安裝 `python-dotenv`）自動載入 `.env`，所以你平常只要改 `.env` 就好。
- `.strip()`：把前後空白去掉，避免你在 `.env` 多打一個空白導致授權失敗。
- `raise ValueError(...)`：故意讓錯誤「很早就炸」：
  - 早炸比晚炸好（你不希望跑到下載資料時才發現沒憑證）
  - 錯誤訊息要能指引新手怎麼修（這是可維護性的一部分）

常見坑：

- `.env` 寫了但沒有被載入：通常是你沒有安裝 `python-dotenv` 或沒有在啟動時呼叫 `load_dotenv()`。
- 你在 IDE 設定了環境變數，但命令列沒設定 → 兩邊行為不一致。

---

### 4.3 `TdxTokenProvider`：負責「需要 token 時拿 token」

這個 class 的責任一句話：

- **Token Provider**：當你需要 token，它會給你一個「目前有效」的 token（必要時會自動更新）。

#### 4.3.1 `from_config()`：用設定檔快速建立 provider

程式碼節錄：

```python
@classmethod
def from_config(
    cls, config: Optional[AppConfig] = None, http_client: Optional[httpx.Client] = None
) -> "TdxTokenProvider":
    resolved_config = config or get_config()
    client_id, client_secret = load_tdx_credentials()
    client = http_client or httpx.Client(timeout=resolved_config.tdx.request_timeout_seconds)
    return cls(
        token_url=resolved_config.tdx.token_url,
        client_id=client_id,
        client_secret=client_secret,
        http_client=client,
        timeout_seconds=resolved_config.tdx.request_timeout_seconds,
    )
```

逐段解釋：

- `config or get_config()`：允許你「顯式傳入 config」做測試，也可以走全域預設。
- `http_client or httpx.Client(...)`：允許外部注入（dependency injection）：
  - 測試時你可以傳 mock client
  - 正式環境就用預設 client
- `timeout`：把 timeout 放在 config（config-driven），避免硬編碼。

> 這種「可注入 client」的設計，是資料工程與後端工程很常用的可測試性技巧。

#### 4.3.2 `get_access_token()`：有需要才更新（lazy refresh）

程式碼節錄：

```python
def get_access_token(self) -> str:
    if self._token is None or self._token.is_expired():
        self._token = self._refresh_token()
    return self._token.access_token
```

逐段解釋：

- `self._token is None`：第一次使用，還沒 token → 需要換。
- `self._token.is_expired()`：token 快過期了 → 需要換。
- 其他情況：直接回傳快取 token（比較快、比較穩）。

常見坑：

- 多執行緒同時呼叫可能會「同時 refresh」造成重複請求（這份 MVP 先不做 lock；後續如果需要可再做）。

#### 4.3.3 `_refresh_token()`：真的去 TDX 換一個新的 token

程式碼節錄：

```python
response = self.http_client.post(
    self.token_url,
    data={
        "grant_type": "client_credentials",
        "client_id": self.client_id,
        "client_secret": self.client_secret,
    },
    timeout=self.timeout_seconds,
)
response.raise_for_status()
payload = response.json()
```

逐段解釋：

- `POST token_url`：這就是 OAuth 的 token endpoint。
- `grant_type=client_credentials`：明確告訴服務端你用的是 client credentials flow。
- `raise_for_status()`：如果 4xx/5xx，立刻丟例外，避免你用一個不完整的 payload 硬繼續跑。
- `payload = response.json()`：拿到 response body（通常包含 `access_token`、`expires_in`）。

再來這段是「資料驗證」：

```python
access_token = payload.get("access_token")
if not access_token:
    raise ValueError("TDX token response is missing 'access_token'.")
```

為什麼要這樣寫？

- API 不一定永遠回你想要的格式（可能是錯誤頁面、或 schema 變了）
- 你要把錯誤變成「可理解且可排查」的訊息

最後這段把 `expires_in` 轉成「絕對過期時間」：

```python
expires_in = int(payload.get("expires_in", 1800))
return OAuthToken(
    access_token=access_token, expires_at_epoch_seconds=time.time() + expires_in
)
```

重點：

- `expires_in` 是「秒數」不是時間點
- 所以我們用 `time.time() + expires_in` 得到一個 epoch 形式的「過期時間點」

---

## 5) `tdx_auth.py` 在整個 ingestion pipeline 的位置（中文）

你可以用一句話記住它的角色：

- **`tdx_auth.py` 負責拿 token；`tdx_traffic_client.py` 負責帶 token 去抓資料。**

在 `src/trafficpulse/ingestion/tdx_traffic_client.py` 你會看到它被用在這裡（節錄概念）：

```python
client_id, client_secret = load_tdx_credentials()
...
self._token_provider = TdxTokenProvider(
    token_url=self.config.tdx.token_url,
    client_id=client_id,
    client_secret=client_secret,
    http_client=self._auth_http,
    timeout_seconds=self.config.tdx.request_timeout_seconds,
)
```

這樣拆的好處：

- Auth 與資料抓取分離（single responsibility）
- 以後如果 token 流程變了，你只需要改 auth module
- 新手讀 code 也比較不會被一個超大檔案嚇到

---

## 6) 常見錯誤與排查（中文）

### 6.1 `Missing TDX credentials`（最常見）

原因：

- `.env` 沒填
- `.env` 有填，但沒有被載入（沒裝 `python-dotenv` 或沒啟動 venv）
- 環境變數有空白或換行

排查步驟（你可以直接在 shell 測）：

```bash
python -c "import os; print('TDX_CLIENT_ID=', bool(os.getenv('TDX_CLIENT_ID')))"
python -c "import os; print('TDX_CLIENT_SECRET=', bool(os.getenv('TDX_CLIENT_SECRET')))"
```

### 6.2 401/403（授權失敗）

原因：

- 憑證錯
- token 過期但你沒有 refresh（本專案已做 expiry buffer 降低機率）
- token endpoint 暫時性錯誤或 rate limit

排查方式：

- 先縮短時間窗，避免你在 debug 時同時面對「大流量抓資料」與「授權」兩種問題。

### 6.3 Timeout / Network errors

原因：

- 網路不穩或 token endpoint 壅塞
- timeout 設太小

建議：

- 調整 `configs/config.yaml` 的 `tdx.request_timeout_seconds`
- 先用小流程驗證再跑長時間窗

---

## 7) 本階段驗收方式（中文 + 英文命令）

> 本階段主要是「註解與文件」，所以驗收重點是：code 仍然可 import、可編譯、沒有語法錯誤。

1) Compile all Python files:

```bash
python -m compileall -q src scripts
```

Expected:

- No output (silent is OK)
- Exit code 0

2) Import the auth module (should not require credentials just to import):

```bash
PYTHONPATH=src python -c "from trafficpulse.ingestion.tdx_auth import TdxTokenProvider; print('ok=', TdxTokenProvider.__name__)"
```

Expected:

- Prints `ok= TdxTokenProvider`
- Exit code 0

---

## 8) 下一步（中文）

如果你確認這個 Phase OK，下一個最自然的 Phase 會是：

- 針對 `src/trafficpulse/ingestion/tdx_traffic_client.py` 做逐行英文註解  
- 並新增 `docs/03_phase3_tdx_client.md`（或同一套命名規則下的下一份文件）  
  - 解釋：時間窗切 chunk、OData filter、快取、重試、欄位正規化（segments/observations schema）

