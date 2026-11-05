# NewsTrader (เวอร์ชันฟรี: GitHub Actions + Discord Webhook)

ไม่ต้องเปิดคอมทิ้งไว้ ไม่ต้องเช่าเซิร์ฟเวอร์ GitHub จะปลุกบอทขึ้นมาทุกชั่วโมงให้เอง:
ดึงข่าว → ให้ Gemini วิเคราะห์ → โพสต์ LONG/SHORT ลง Discord → บันทึกผลไว้ในไฟล์ `newsbot.db` ใน repo

สิ่งที่โพสต์อัตโนมัติ:
- **ทุกชั่วโมง** ข่าวสำคัญพร้อมหุ้นที่ได้รับผลกระทบ (ถ้ารอบนั้นไม่มีข่าวสำคัญ จะไม่โพสต์)
- **เมื่อสัญญาณครบกำหนด** สรุปว่าตัวไหนถูก ✅ ตัวไหนผิด ❌ พร้อม % กำไร/ขาดทุน
- **ทุกวันจันทร์ 08:07 น.** สถิติความแม่นยำรวม แยกตามระดับความมั่นใจ

---

## ขั้นตอนติดตั้ง (ทำครั้งเดียว ~15 นาที)

### 1. สร้าง Webhook ใน Discord
1. ไปที่ช่อง `#news-feed` → ⚙️ **Edit Channel** → **Integrations** → **Webhooks** → **New Webhook**
2. ตั้งชื่อและรูปโปรไฟล์ได้ตามใจ
3. กด **Copy Webhook URL** เก็บไว้ (ห้ามแชร์ ใครได้ลิงก์นี้ไปก็โพสต์ลงช่องคุณได้)

### 2. สร้าง repository บน GitHub
1. สมัคร/ล็อกอินที่ https://github.com
2. มุมขวาบน กด **+** → **New repository**
3. ตั้งชื่อ เช่น `newsbot` → เลือก **Private** → กด **Create repository**

### 3. อัปโหลดไฟล์
1. ในหน้า repo กดลิงก์ **uploading an existing file**
2. ลากไฟล์ทั้งหมดในโฟลเดอร์นี้ไปวาง: `run.py`, `feeds.py`, `requirements.txt`, `.gitignore`, `README.md` **และโฟลเดอร์ `.github`**
3. กด **Commit changes**

> ⚠️ เช็กว่าโฟลเดอร์ `.github/workflows/newsbot.yml` ขึ้นไปด้วย ถ้าไม่ขึ้น ให้กด **Add file → Create new file**
> พิมพ์ชื่อ `.github/workflows/newsbot.yml` แล้วก๊อปเนื้อหาจากไฟล์ในเครื่องไปวาง → Commit

> ถ้าอยากเก็บสถิติจากบอทตัวเก่าในเครื่อง ให้อัปโหลดไฟล์ `newsbot.db` ขึ้นไปด้วย

### 4. ใส่คีย์ลับ (Secrets)
ในหน้า repo: **Settings** → **Secrets and variables** → **Actions** → แท็บ **Secrets** → **New repository secret**
เพิ่มทีละตัว:

| Name | Secret |
|---|---|
| `DISCORD_WEBHOOK_URL` | ลิงก์ webhook จากข้อ 1 |
| `GEMINI_API_KEY` | คีย์ `AIza...` จาก aistudio.google.com |

ค่าใน Secrets ถูกเข้ารหัส คนอื่นและตัว log ก็มองไม่เห็น

### 5. (ไม่บังคับ) ปรับแต่ง
หน้าเดียวกัน แท็บ **Variables** → **New repository variable**:

| Name | ตัวอย่าง | ความหมาย |
|---|---|---|
| `MARKETS` | `US,TH,JP` | ตลาดที่ให้เน้น (ค่าเริ่มต้น `US,TH`) |
| `MAX_EVENTS` | `3` | จำนวนข่าวสูงสุดต่อชั่วโมง |
| `LANGUAGE` | `Thai` | ภาษาของสรุป |
| `BOT_NAME` | `News Trader` | ชื่อที่แสดงในช่อง |
| `PROVIDER` | `gemini` | `gemini` / `groq` / `claude` (ต้องเพิ่ม Secret ของเจ้านั้นด้วย) |
| `MODEL` | `gemini-2.5-flash` | ระบุรุ่นเอง ถ้ารุ่นค่าเริ่มต้นใช้ไม่ได้ |

### 6. ทดสอบรัน
1. แท็บ **Actions** (ถ้ามีปุ่มให้เปิดใช้ ให้กดเปิด)
2. เลือก **NewsTrader** ทางซ้าย → **Run workflow** → mode `news` → **Run workflow**
3. รอ 1–3 นาที ถ้าขึ้นเครื่องหมาย ✅ สีเขียว และมีโพสต์ใน Discord = เสร็จ
   กดเข้าไปดู log ได้ที่ขั้น **Run bot**

จากนั้นมันจะรันเองทุกชั่วโมง **อย่าลืมปิดบอทตัวเก่าในเครื่อง** ไม่อย่างนั้นข่าวจะโพสต์ซ้ำสองที่

---

## แก้ปัญหา
- **ขั้น Save state ขึ้นสีแดง / `Permission denied`** → Settings → Actions → General → Workflow permissions → เลือก **Read and write permissions** → Save
- **log ขึ้น `model ... failed`** → ใส่ Variable `MODEL` เป็นชื่อรุ่นที่ใช้ได้ (ดูใน AI Studio)
- **สัญญาณขึ้น ⏳ รอราคาใหม่ ตลอด** → Yahoo Finance อาจบล็อกเซิร์ฟเวอร์ของ GitHub ชั่วคราว รอบถัด ๆ ไปจะลองดึงราคาใหม่เอง
- **อยากหยุดบอท** → Actions → NewsTrader → ปุ่ม `···` มุมขวา → **Disable workflow**

## ข้อควรรู้
- GitHub อาจปลุกบอทช้ากว่าเวลาที่ตั้งไว้ 5–30 นาทีในช่วงที่คนใช้เยอะ เป็นเรื่องปกติ
- repo แบบ Private ได้เวลารันฟรีประมาณ 2,000 นาที/เดือน บอทใช้ราว 1,000–1,500 นาที ถ้าเกิน GitHub จะหยุดรันจนถึงเดือนถัดไป (ไม่ตัดเงิน เพราะไม่ได้ผูกบัตร)
- **สัญญาณจาก AI ไม่ใช่คำแนะนำการลงทุน** ดูสถิติวันจันทร์สักหลายสัปดาห์ก่อนตัดสินใจใช้เงินจริง
