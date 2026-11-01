# Game Price Trend Forecasting — Data Engineering Pipeline

เอกสารนี้กำหนดโครงสร้างและแนวทางพัฒนาส่วน Data Engineering สำหรับระบบพยากรณ์แนวโน้มราคาเกมและโอกาสที่ราคาจะลดลงถึงเป้าหมาย โดยยึดตามเอกสารโครงงานและข้อเสนอ Pipeline ที่แนบมา

> **สถานะ:** เริ่มพัฒนา vertical slice สำหรับ Steam ingestion, quality gate, PostgreSQL/SQLite storage, feature generation และ execution logs แล้ว ส่วน ITAD, model training, FastAPI และ drift monitoring ยังไม่อยู่ใน implementation ปัจจุบัน

## เป้าหมาย

- รวบรวม metadata และ daily price snapshots เพื่อสะสมประวัติราคาของเกม
- เก็บข้อมูลต้นฉบับไว้ตรวจสอบย้อนหลัง และแยกข้อมูลที่ไม่ผ่านคุณภาพออกจากข้อมูลพร้อมใช้งาน
- สร้างข้อมูลสะอาดและ feature ที่มีขอบเขตเวลาชัดเจน เพื่อส่งต่อให้ model และ API
- ทำให้ pipeline รันซ้ำได้ บันทึกสถานะ และกู้คืนเมื่อดึงข้อมูลไม่สำเร็จ

## แหล่งข้อมูลและขอบเขต

| แหล่งข้อมูล | ข้อมูลหลัก | การใช้งาน |
| --- | --- | --- |
| Steam Store appdetails | `app_id`, ชื่อเกม, genres, publisher, release date, ราคาเริ่มต้น และราคาปัจจุบัน | Metadata และ daily price snapshot (implementation ปัจจุบันรองรับ US/USD) |
| IsThereAnyDeal (ITAD) API | ประวัติราคา, ร้านค้า, ราคาปัจจุบัน/ราคาเดิม, ส่วนลด และ historical low | ประวัติราคาและข้อมูลดีล |
| Steam Store / SteamDB public pages | ทางเลือกสำรองตามที่ระบุในเอกสาร เมื่อ API ให้ข้อมูลไม่ครบ | ใช้เมื่อจำเป็นและต้องตรวจสอบข้อกำหนดการเข้าถึงก่อน |

หน่วยข้อมูลที่แนะนำคือ **เกม-ร้านค้า-วัน (Game-Store-Day)** โดยใช้ `app_id`, `shop_id` และวันที่สังเกตราคาเป็นตัวระบุหลัก เอกสารสำรวจข้อมูลระบุตัวอย่างข้อมูลย้อนหลังตั้งแต่ `2022-01-01` ถึง `2026-10-31` และชุดเริ่มต้นประมาณ 1,000 เกม; ตัวเลขนี้เป็นค่าประมาณจากเอกสาร ไม่ใช่ข้อจำกัดของระบบ

## ภาพรวมสถาปัตยกรรม

```text
Steam Store appdetails ─┐
               ├─> Daily Ingestion ─> Raw JSON snapshots
ITAD API ──────┘                            │
                                            v
                                  Schema & Quality Gate
                                    │              │
                               valid data       invalid data
                                    │              └─> Quarantine
                                    v
                           Transform / Clean / Deduplicate
                                    │
                                    v
                       PostgreSQL (games, prices, features)
                                    │
                    ┌───────────────┴────────────────┐
                    v                                v
            Model / MLflow                    FastAPI / Dashboard
                    │
                    v
           Drift & Performance Monitoring
```

ไม่จำเป็นต้องประมวลผลแบบ real-time ระดับวินาที เป้าหมายตามเอกสารคืออัปเดตราคาอย่างน้อยวันละครั้งและให้ข้อมูลสดไม่เก่ากว่า 24 ชั่วโมง

## โครงสร้าง repository ที่เสนอ

```text
.
├── README.md
├── .env.example
├── .gitignore
├── requirements.txt
├── pyproject.toml
├── docker-compose.yml
├── configs/
│   └── pipeline.yaml
├── data/                         # ข้อมูล runtime; ไม่ commit เข้า Git
│   ├── raw/
│   ├── quarantine/
│   └── processed/
├── src/
│   └── game_price_pipeline/
│       ├── cli.py
│       ├── __main__.py
│       ├── models.py
│       ├── config.py
│       ├── steam.py
│       ├── quality.py
│       ├── storage.py
│       ├── features.py
│       ├── orchestrator.py
│       └── alerts.py
└── tests/
    └── test_pipeline.py
```

schema ถูกประกาศจาก SQLAlchemy models และสร้างเมื่อเริ่ม pipeline โครงสร้างนี้ใช้ Cron เรียก CLI ได้ และสามารถพิจารณา Apache Airflow เมื่อ workflow มี dependency เพิ่มขึ้น

## เริ่มใช้งาน

ต้องใช้ Python 3.11 ขึ้นไป และ Docker Compose สำหรับ PostgreSQL:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
Copy-Item .env.example .env
docker compose up -d postgres
```

แก้ `DATABASE_URL` และ `STEAM_APP_IDS` ใน `.env` ก่อนรัน ค่าเริ่มต้นของตัวอย่างใช้ Steam app ID `570` และราคาประเทศ US (USD) เท่านั้น; pipeline หน่วง request ตาม `STEAM_REQUEST_INTERVAL_SECONDS` (ค่าเริ่มต้น 1 วินาที) เพื่อลดโอกาสถูกจำกัดการเรียก API ห้ามนำรหัสผ่านตัวอย่างไปใช้กับ service ที่เปิดให้เข้าถึงจากภายนอก

```powershell
python -m game_price_pipeline
```

หรือส่ง app IDs โดยตรง:

```powershell
python -m game_price_pipeline --app-ids 570,730
```

รัน unit/integration tests กับ SQLite ชั่วคราว โดยไม่ต้องเปิด PostgreSQL หรือเรียก Steam จริง:

```powershell
python -m unittest discover -s tests -v
```

Pipeline สร้าง snapshot แบบไม่เขียนทับใน `data/raw/`, ตารางข้อมูลเมื่อเริ่มครั้งแรก, quarantine สำหรับ record ที่ผิด และ execution log สถานะ `success`, `partial` หรือ `failed` การ retry API สูงสุด 3 ครั้ง และส่ง Discord alert เมื่อกำหนด webhook ใน `.env` การรันซ้ำในวันเดียวกัน upsert แถวเดิมด้วย unique key `(app_id, shop_id, observed_date)`.

ข้อมูล metadata จะถูกดึงเมื่อเพิ่มเกมเข้าระบบครั้งแรกและ refresh ตาม `STEAM_METADATA_REFRESH_DAYS` (ค่าเริ่มต้น 30 วัน) จากนั้นใช้ Steam `price_overview` filter สำหรับ snapshot รายวันเพื่อลดขนาด response; metadata ที่ cache ไว้ใช้ตรวจสอบเกมฟรีและสร้าง feature

Feature cutoff ตั้งที่เที่ยงคืนของ `PIPELINE_TIMEZONE` โดยใช้เฉพาะ snapshot ที่ดึงข้อมูลก่อนหรือ ณ cutoff; snapshot ที่ดึงหลัง cutoff จะถูกบันทึก แต่ยังไม่ใช้สร้าง feature ของรอบนั้น `STEAM_SALE_DATES` รับวันที่เริ่มงาน sale ในรูปแบบ `YYYY-MM-DD` คั่นด้วย comma หากไม่มีปฏิทินจะบันทึก `days_to_next_steam_sale` เป็นค่าว่างแทนการเดาวันจัดงาน

### ขอบเขตที่ยังไม่เชื่อมต่อ

- **ITAD:** ยังไม่มี API client หรือการดึงข้อมูลจาก ITAD เนื่องจาก [ข้อกำหนด API ของ ITAD](https://docs.isthereanydeal.com/) จำกัดการใช้เพื่อสร้างแอปที่แข่งขันกับบริการของเขา โครงงานพยากรณ์ดีลอาจอยู่ในขอบเขตนี้ ควรตรวจสอบและได้รับอนุญาตก่อนเพิ่ม connector
- **Model, FastAPI และ monitoring/drift:** ตาราง feature พร้อมส่งต่อ แต่ยังไม่มีขั้น train/serve, model registry หรือ alert สำหรับ model drift
- **Feature เพิ่มเติม:** `publisher_avg_discount` และการนับ promotion จากแหล่งประวัติราคาเต็ม ยังรอ dataset ที่มี coverage ย้อนหลัง
- **Fallback:** เมื่อ API ใช้งานไม่ได้ จะบันทึก execution เป็น `partial`/`failed` และส่ง alert (ถ้ากำหนด) โดยยังไม่ใช้ราคาวันก่อนหน้าแทน observation จริง

## ขั้นตอนการทำงาน

| ขั้น | Input → Output | แนวทาง |
| --- | --- | --- |
| 1. Ingest | Steam/ITAD API → Raw JSON | Python และ HTTP client; เก็บ response พร้อม `fetched_at` และ timestamp ในชื่อไฟล์ ไม่เขียนทับ snapshot เดิม |
| 2. Validate | Raw JSON → ผ่าน / quarantine | ตรวจ HTTP status, response ว่าง, schema, ชนิดข้อมูล, critical fields และกฎราคา ก่อนนำไปใช้ |
| 3. Transform | Raw JSON → clean staging | ทำมาตรฐานชื่อ field, วันเวลา/เขตเวลา, สกุลราคา และ deduplicate; ใช้ Pandas/SQL ตามขนาดข้อมูล |
| 4. Load | staging → PostgreSQL | ใช้ upsert และ unique key เพื่อให้รันซ้ำแล้วไม่สร้างแถวซ้ำ |
| 5. Build features | ข้อมูลสะอาด → feature tables | คำนวณเฉพาะวันที่ยังไม่ประมวลผล และใช้เฉพาะข้อมูลที่มีอยู่ ณ cutoff time |
| 6. Publish | feature tables → model/API | ส่งต่อชุดข้อมูลให้ batch training, incremental update และ serving layer |
| 7. Monitor | execution, actual prices, predictions → logs/alerts | ตรวจ freshness และคุณภาพข้อมูล; เปรียบเทียบผลพยากรณ์กับราคาจริงเมื่อมีข้อมูลใหม่ |

### ตารางข้อมูลหลักที่แนะนำ

- `games` — metadata ระดับเกม โดยมี `app_id` เป็นรหัสเกม
- `shops` — ร้านค้าที่ใช้ติดตามราคา; implementation ปัจจุบันเก็บ `shop_id=steam` โดยยังไม่มี lookup table แยก
- `game_price_history` — ราคาต่อเกม/ร้านค้า/วัน เช่น `price_actual`, `price_old`, `cut`, `observed_date` และ `fetched_at`
- `game_features` — feature ที่สร้างจากข้อมูลอดีต เช่น lag, rolling metrics และข้อมูลฤดูกาล
- `pipeline_execution_logs` — เวลาเริ่ม/จบ, API status, จำนวน record, อัตรา null, สถานะ และข้อมูล fallback
- `data_quality_quarantine` — record ที่ไม่ผ่าน validation พร้อมเหตุผลและแหล่งที่มา

กำหนด unique key ของประวัติราคาเป็น `(app_id, shop_id, date)` หากพบข้อมูลซ้ำให้เลือก record ที่มีเวลาดึงข้อมูลล่าสุดตาม data contract

## ตารางเวลาและการกู้คืน

- รัน ingestion วันละครั้งหลัง cutoff เช่น `00:05`; ตั้งค่า timezone ให้ชัดเจนและสอดคล้องกับ cutoff `00:00`
- Retry การเรียก API ได้สูงสุด 3 ครั้ง หากยังไม่สำเร็จให้บันทึก error และส่ง alert ไป Discord/Slack
- เก็บ snapshot แต่ละรอบด้วย timestamp เพื่อป้องกันไฟล์ทับกัน และทำให้ตรวจสอบย้อนหลังได้
- หาก ingestion ล้มเหลว เอกสารเสนอให้ใช้ราคาล่าสุดของวันก่อนหน้าเป็น fallback ชั่วคราว ต้องบันทึก `fallback_data_used = true` และแยกจาก observation จริง โดยไม่ปะปนเป็น training label
- หยุด pipeline เมื่อข้อมูล stale เกิน 24 ชั่วโมงหรือ quality gate ที่สำคัญไม่ผ่าน จนกว่าจะตรวจสอบสาเหตุ

## Data contract และ Quality Gate

1. **Schema:** ตรวจชื่อ field, data type และ timezone; ราคาเป็นตัวเลข วันที่เป็น datetime และ metadata เป็นชนิดที่กำหนด
2. **Completeness:** `price_actual`, `initial_price` และ `date` เป็น critical fields; หากขาดให้ reject record และลองดึง record นั้นใหม่
3. **Uniqueness:** ไม่มี key ซ้ำใน `(app_id, shop_id, date)`; deduplicate ด้วย `fetched_at` ล่าสุด
4. **Freshness:** ราคาที่ใช้งานต้องอัปเดตภายใน 24 ชั่วโมง
5. **Validity:** `cut` อยู่ในช่วง 0–100%; ตรวจ `price_actual` เทียบกับ `initial_price` ตามกฎในเอกสาร
6. **Free price / API noise:** ราคา 0 ต้องยืนยันว่าเป็นเกมฟรีจากข้อมูลที่เชื่อถือได้ มิฉะนั้นให้ flag หรือ quarantine ไม่ถือเป็นราคาจริง
7. **Leakage:** สร้าง feature จากข้อมูลก่อน cutoff เท่านั้น; ห้ามใช้ราคาหลัง cutoff หรือข้อมูลผลลัพธ์อนาคตเป็น feature และห้าม random split สำหรับข้อมูลตามเวลา

ข้อมูลที่ผิดช่วงราคาหรือไม่ผ่านกฎให้ติด flag พร้อมเหตุผล และไม่นำเข้า training set จนกว่าจะตรวจสอบแล้ว

## Feature และการส่งต่อให้ Model

ตัวอย่าง feature ที่ระบุในเอกสาร:

- **Lifecycle:** `days_since_release`
- **Seasonality:** `days_to_next_steam_sale`, `is_major_sale_month`
- **Price history:** `historical_low_cut`, `rolling_avg_discount_90d`, `discount_frequency_year`
- **Publisher/game metadata:** `publisher_avg_discount` และกลุ่มเกม เช่น AAA/Indie

ผลลัพธ์ของ data pipeline เป็นข้อมูลพร้อมให้ model ประเมินความน่าจะเป็นที่ราคาจะถึง Target Price, ราคาต่ำสุดที่คาดการณ์ และเวลาที่คาดว่าจะถึงเป้าหมาย การแบ่ง train/validation/test ต้องเป็น temporal split เช่น Train `2022–2024`, Test `2025–2026`; fit imputer/scaler/encoding จาก train set เท่านั้น

## Observability และความปลอดภัย

- บันทึก `timestamp`, API status, record count, null percentage, stage status และ error ลง `pipeline_execution_logs`
- ติดตาม data freshness, schema/quality failures, duplicate rate, fallback usage และ model drift/performance
- ส่ง alert เมื่อ API ล้มเหลว, ข้อมูลเก่าเกินเกณฑ์ หรือพบ drift; เครื่องมือตามข้อเสนอ ได้แก่ Evidently AI และ Discord/Slack
- เก็บ API keys ใน environment variables; ใช้ `.env.example` แสดงเฉพาะชื่อตัวแปร และห้าม commit `.env`, secrets หรือข้อมูล raw
- ข้อมูลในขอบเขตนี้เป็นข้อมูลเกมและราคา ไม่มี PII ตามเอกสารข้อเสนอ

## การทดสอบ

ชุดทดสอบปัจจุบันครอบคลุม idempotent upsert, quality checks สำหรับราคา/ส่วนลด/เกมฟรี, การกรองข้อมูลหลัง cutoff, metadata cache/refresh, API retry และ price-only request โดยใช้ SQLite และ HTTP mocks ไม่เรียก Steam จริง

การทดสอบต่อยอดที่แนะนำ:

- Unit tests สำหรับ schema, validity rules, normalization, deduplication และ feature cutoff
- Integration tests สำหรับ API client และ PostgreSQL โดยใช้ fixtures หรือ test database
- Idempotency test: รันข้อมูลชุดเดิมซ้ำแล้วจำนวน record ไม่เพิ่มและผลลัพธ์ไม่เปลี่ยน
- Failure tests: API error, response ว่าง, critical null, stale data, duplicate และราคา 0 ที่ไม่ได้ยืนยันว่าเป็นเกมฟรี
- Leakage test: ยืนยันว่าไม่มีข้อมูลหลัง cutoff อยู่ใน features

## แหล่งอ้างอิง

- `Game Price Trend ForecastingPipeline.pdf` — ภาพรวม architecture, ingestion, quality gate, features, model serving และ monitoring (หน้า 1–5)
- `GamePriceTrendForecasting_Proposal.pdf` — data sources/contract (หน้า 1–2, 6–8), data audit และ leakage checks (หน้า 10–12), Data Engineering design และ pipeline specification (หน้า 13–14)
- [IsThereAnyDeal API terms and documentation](https://docs.isthereanydeal.com/) — โปรดตรวจเงื่อนไขการใช้ข้อมูลก่อนเพิ่ม integration
