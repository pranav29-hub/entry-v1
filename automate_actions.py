import asyncio
import json
import os
import csv
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright

# ==========================================
# CONFIGURATION
# ==========================================
DATA_CSV = "chunk_2_rows_5001_to_10000.csv"
PROCESSED_FILE = "processed-chunk-2.json"
LOG_DIR = "logs"
TARGET_URL = "https://tataminiessay.com/?utm_source=paprika&utm_medium=PIL_HIT10"

# Detect if running in GitHub Actions (CI)
IS_CI = os.getenv("CI") == "true"

# Timeout Safety: Stop script after 5 hours 50 minutes (21000 seconds)
# This ensures we have 10 minutes to commit/push before GitHub kills the runner at 6 hours.
MAX_RUNTIME_SECONDS = 5 * 3600 + 50 * 60 

CONFIG = {
    "headless": True if IS_CI else False,
    "minInterSubmissionDelayMs": 2000,
    "maxInterSubmissionDelayMs": 4000,
    "typingDelayMin": 80,
    "typingDelayMax": 200,
    "FAST_typingDelayMin": 30,
    "FAST_typingDelayMax": 70,
    "maxRetriesPerRow": 2,
    "takeScreenshotOnError": True,
    "screenshotDir": LOG_DIR,
    "saveHtmlOnError": True,
    "pauseBetweenFieldsMin": 800,
    "pauseBetweenFieldsMax": 1000,
}

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def sleep_ms(ms):
    return asyncio.sleep(ms / 1000)

def rand_int(min_val, max_val):
    return random.randint(min_val, max_val)

def load_csv_rows(csv_path):
    """
    Reads a CSV without headers.
    Column 0 = Phone
    Column 1 = Pledge
    """
    rows = []
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            print(f"   ℹ️ Attempting to read CSV with encoding: {encoding}")
            with open(csv_path, mode='r', encoding=encoding) as f:
                reader = csv.reader(f)
                for line_num, line in enumerate(reader):
                    if not line: continue
                    
                    # Expecting at least 2 columns
                    if len(line) >= 2:
                        rows.append({
                            "phone": line[0].strip(),
                            "pledge": line[1].strip()
                        })
                
                print(f"   ✅ Successfully loaded {len(rows)} rows using {encoding}.")
                return rows
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"   ❌ Error reading CSV with {encoding}: {e}")
            break
            
    print("   ❌ Failed to read CSV with all attempted encodings.")
    return []

def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return {}
    try:
        with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_processed(data_map):
    with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
        json.dump(data_map, f, indent=2)

# ==========================================
# AUTOMATION FUNCTIONS
# ==========================================

async def detect_captcha(page):
    iframes = page.frames
    has_recaptcha_iframe = any("recaptcha" in (f.url or "").lower() for f in iframes)
    has_captcha_element = await page.evaluate("""() => {
        const texts = Array.from(document.querySelectorAll("body *")).slice(0, 200);
        return texts.some((el) => {
            const t = (el.innerText || "").toLowerCase();
            return t.includes("captcha") || t.includes("verify you are human") || t.includes("i'm not a robot");
        });
    }""")
    return has_recaptcha_iframe or has_captcha_element

async def find_phone_input_selector(page):
    if await page.query_selector("#phone"): return "#phone"
    return await page.evaluate("""() => {
        const inputs = Array.from(document.querySelectorAll("input"));
        const phone = inputs.find(i => i.name === "phone" || i.type === "tel");
        return phone ? "input[name='phone']" : null;
    }""")

async def find_pledge_selector(page):
    if await page.query_selector("#pledge"): return "#pledge"
    return "textarea"

async def fast_human_type(page, selector, text):
    await page.focus(selector)
    await page.evaluate(f"document.querySelector('{selector}').value = ''")
    await page.type(selector, text, delay=rand_int(CONFIG["FAST_typingDelayMin"], CONFIG["FAST_typingDelayMax"]))

async def randomize_selects(page):
    selects = await page.query_selector_all("select")
    for sel in selects:
        try:
            options = await sel.eval_on_selector_all("option", "opts => opts.map(o => o.value).filter(v => v !== '' && v.length > 0)")
            if options:
                await sel.select_option(random.choice(options))
                await sleep_ms(rand_int(200, 500))
        except: pass

async def check_terms_checkbox(page):
    try:
        checkbox = await page.query_selector("#terms")
        if checkbox and not await checkbox.is_checked():
            await checkbox.click()
            return True
    except: pass
    return await page.evaluate("""() => {
        const ch = document.querySelector("input[type=checkbox]");
        if (ch && !ch.checked) { ch.click(); return true; }
        return false;
    }""")

async def click_submit(page):
    print("   → 🖱 Attempting Human Click on #submitBtn...")
    selector = "#submitBtn"
    try:
        await page.wait_for_selector(selector, state="visible", timeout=5000)
        btn = await page.query_selector(selector)
        await btn.scroll_into_view_if_needed()
        await sleep_ms(500)
        
        box = await btn.bounding_box()
        if box:
            await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            await sleep_ms(250)
            await page.mouse.down()
            await sleep_ms(150)
            await page.mouse.up()
            print("   → 🖱 Mouse click sent.")
            try:
                await page.wait_for_selector("#submitLoading", state="visible", timeout=2000)
                print("   → ✅ Loader appeared! Click confirmed.")
                return True
            except:
                print("   ⚠ Loader did not appear (might be fast).")
    except Exception as e:
        print(f"   ⚠ Human click failed: {e}")
    
    print("   → 🔧 Trying JS Click fallback...")
    return await page.evaluate(f"document.querySelector('{selector}') ? (document.querySelector('{selector}').click(), true) : false")

# ==========================================
# MAIN LOGIC
# ==========================================

async def main():
    # 1. Start the Timer
    execution_start_time = time.time()
    
    if not os.path.exists(DATA_CSV):
        print(f"Missing {DATA_CSV}")
        return

    rows = load_csv_rows(DATA_CSV)
    print(f"\n📋 Loaded {len(rows)} rows from {DATA_CSV}\n")
    processed = load_processed()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=CONFIG["headless"],
            args=["--start-maximized", "--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(viewport=None)
        page = await context.new_page()
        
        # API Interception state
        state = {"last_submission_success": False}

        async def handle_response(response):
            if '/api/entry.php?action=submit' in response.url:
                try:
                    json_data = await response.json()
                    if json_data.get('success') is True:
                        state["last_submission_success"] = True
                except: pass
        page.on("response", handle_response)

        page_loaded = False
        serial_no = 1

        for i, row in enumerate(rows):
            # 2. CHECK TIMEOUT
            if time.time() - execution_start_time > MAX_RUNTIME_SECONDS:
                print("\n\n⏳ TIME LIMIT REACHED (5h 50m). Stopping gracefully to save progress.")
                print("   This ensures GitHub Actions can commit data before the hard 6h timeout.\n")
                break

            pledge, phone = row.get("pledge"), row.get("phone")
            unique_key = f"{phone}::{pledge[:40]}"
            
            if unique_key in processed and processed[unique_key].get("status") == "success":
                print(f"[{i + 1}/{len(rows)}] ⏭  Skipping processed row")
                continue

            if not pledge or not phone: continue

            attempt = 0
            success = False
            while attempt < CONFIG["maxRetriesPerRow"] and not success:
                attempt += 1
                print(f"\n[{i + 1}/{len(rows)}] 🔄 Submitting #{serial_no} (attempt {attempt}) phone={phone}")
                try:
                    if not page_loaded:
                        try: await page.goto(TARGET_URL, wait_until="networkidle", timeout=45000)
                        except: await page.reload(wait_until="networkidle")
                        await sleep_ms(1000)
                        page_loaded = True

                    if await detect_captcha(page):
                        print("❌ Captcha detected. Exiting.")
                        return

                    await randomize_selects(page)
                    await fast_human_type(page, await find_pledge_selector(page), pledge)
                    await fast_human_type(page, await find_phone_input_selector(page), phone)
                    await check_terms_checkbox(page)
                    await sleep_ms(500)

                    state["last_submission_success"] = False
                    await click_submit(page)

                    print("   → Waiting for success...")
                    start_time = time.time()
                    while time.time() - start_time < 15:
                        if state["last_submission_success"]: break
                        if await page.evaluate("document.getElementById('pledgeSuccess') && document.getElementById('pledgeSuccess').style.display !== 'none'"): break
                        if "pledge submitted successfully" in await page.evaluate("document.body.innerText.toLowerCase()"): break
                        await sleep_ms(500)
                    else:
                        raise Exception("Success confirmation not found.")

                    print(f"   ✅ Success!")
                    processed[unique_key] = {"serialNo": serial_no, "status": "success", "timestamp": datetime.now().isoformat(), "phone": phone}
                    serial_no += 1
                    save_processed(processed)
                    
                    page_loaded = False
                    success = True
                    await sleep_ms(rand_int(CONFIG["minInterSubmissionDelayMs"], CONFIG["maxInterSubmissionDelayMs"]))

                except Exception as err:
                    print(f"   ❌ Error: {err}")
                    stamp = f"{int(time.time())}-row{i+1}"
                    try:
                        await page.screenshot(path=os.path.join(CONFIG["screenshotDir"], f"error-{stamp}.jpg"))
                        html = await page.content()
                        with open(os.path.join(CONFIG["screenshotDir"], f"error-{stamp}.html"), "w", encoding="utf-8") as f: f.write(html)
                    except: pass
                    page_loaded = False
                    await asyncio.sleep(3)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
