from prefect import flow, task, get_run_logger

from playwright.sync_api import sync_playwright

from openai import OpenAI
from dotenv import load_dotenv
import os
import hashlib
import json

load_dotenv()

@task(name="scrape-hatla2ee", retries=2)
def scrape(max_pages: int = 1):
    logger = get_run_logger()
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        for page_num in range(1, max_pages + 1):
            url = f"https://eg.hatla2ee.com/ar/car/search?page={page_num}"
            page.goto(url)
            page.wait_for_timeout(8000)

            cards = page.query_selector_all("div[data-slot='card']")

            for card in cards:
                text = card.inner_text()
                if "جنيه" not in text and "EGP" not in text:
                    continue
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                results.append({"raw_lines": lines, "page": page_num})

            logger.info(f"Page {page_num}: extracted {len(results)} total listings so far")

        browser.close()

    return results

@task
def clean(raw_data):
    logger = get_run_logger()
    cleaned = []

    for listing in raw_data:
        lines = listing.get("raw_lines", [])

        price_raw = next((l for l in lines if "جنيه" in l or "EGP" in l), None)
        year = next((l for l in lines if l.isdigit() and len(l) == 4), None)
        mileage = next((l for l in lines if "کم" in l or "كم" in l), None)
        transmission = next((l for l in lines if "أتوماتيك" in l or "مانيوال" in l), None)
        fuel = next((l for l in lines if l in ["بنزين", "ديزل", "كهرباء", "هجين"]), None)
        location = next((l for l in lines if "," in l and "جنيه" not in l and "کم" not in l and "كم" not in l), None)


        title_line = next((l for l in lines if any(c.isdigit() for c in l) and len(l) > 6 and "کم" not in l and "جنيه" not in l and "عرض" not in l and "Next" not in l and "/" not in l), None)

        price_num = None
        if price_raw:
            price_num = int(price_raw.replace(",", "").replace("جنيه", "").replace("EGP", "").strip())

        cleaned.append({
            "title": title_line,
            "price": price_num,
            "year": int(year) if year else None,
            "mileage": mileage,
            "transmission": transmission,
            "fuel": fuel,
            "location": location,
            "page": listing.get("page")
        })

    logger.info(f"Cleaned {len(cleaned)} listings")
    logger.info(str(cleaned[0]))
    return cleaned


@task
def enrich(clean_data):
    logger = get_run_logger()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    db = mongo_client["hatla2ee"]
    setup_collection(db)
    collection = db["listings"]

    to_enrich = []
    for listing in clean_data:
        doc_id = make_id(listing)
        if not collection.find_one({"_id": doc_id}):
            to_enrich.append(listing)

    mongo_client.close()
    logger.info(f"Skipping {len(clean_data) - len(to_enrich)} already stored, enriching {len(to_enrich)} new")


    enriched = []

    for listing in to_enrich:
        prompt = f"""
Given this car listing: {listing}
Return a JSON object with these fields:
- condition: "new" or "used"
- price_category: "budget", "mid-range", or "premium"
- city: extracted city name in English
- make: car brand in English
- model: car model in English
- value_for_money: a score from 1 to 10 based on price, year, mileage, and condition. 10 means excellent deal, 1 means overpriced.
- value_reasoning: one sentence explaining the score in English
Return only valid JSON, no explanation.
"""
        response = client.chat.completions.create(
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": prompt}]
        )

        import json
        gpt_data = json.loads(response.choices[0].message.content)
        enriched.append({**listing, **gpt_data})

    logger.info(f"Enriched {len(enriched)} listings")
    logger.info(str(enriched[0]))
    return enriched

from pymongo import MongoClient

@task
def store(enriched_data):
    logger = get_run_logger()

    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["hatla2ee"]
    collection = db["listings"]

    upserted = 0
    for listing in enriched_data:
        listing["_id"] = make_id(listing)
        result = collection.update_one(
            {"_id": listing["_id"]},
            {"$set": listing},
            upsert=True
        )
        if result.upserted_id:
            upserted += 1

    logger.info(f"Stored {upserted} new listings, {len(enriched_data) - upserted} already existed")
    client.close()

@flow(name="hatla2ee-etl")
def hatla2ee_pipeline(max_pages: int = 5):
    raw = scrape(max_pages)
    cleaned = clean(raw)
    enriched = enrich(cleaned)
    store(enriched)
    
    
    
def setup_collection(db):
    if "listings" not in db.list_collection_names():
        db.create_collection("listings", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["title", "price", "year"],
                "properties": {
                    "title":        {"bsonType": "string"},
                    "price":        {"bsonType": "int"},
                    "year":         {"bsonType": "int"},
                    "mileage":      {"bsonType": ["string", "null"]},
                    "transmission": {"bsonType": ["string", "null"]},
                    "fuel":         {"bsonType": ["string", "null"]},
                    "location":     {"bsonType": ["string", "null"]},
                    "condition":    {"bsonType": ["string", "null"]},
                    "price_category": {"bsonType": ["string", "null"]},
                    "city":         {"bsonType": ["string", "null"]},
                    "make":         {"bsonType": ["string", "null"]},
                    "model":        {"bsonType": ["string", "null"]},
                }
            }
        })
        
        
def make_id(listing):
    key = {
        "title": listing.get("title"),
        "price": listing.get("price"),
        "year": listing.get("year"),
        "mileage": listing.get("mileage"),
        "location": listing.get("location")
    }
    return hashlib.md5(json.dumps(key, sort_keys=True, ensure_ascii=False).encode()).hexdigest()