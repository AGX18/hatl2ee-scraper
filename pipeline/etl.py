import re

from prefect import flow, task, get_run_logger

from playwright.sync_api import sync_playwright

from openai import OpenAI
from dotenv import load_dotenv
import os
import hashlib
import json

load_dotenv()

import re
from prefect import task, get_run_logger
from playwright.sync_api import sync_playwright

# Known multi-word car brands in the Egyptian market
MULTI_WORD_BRANDS = ["Land Rover", "Alfa Romeo", "Aston Martin", "Great Wall"]

@task(name="scrape-hatla2ee", retries=2)
def scrape(max_pages: int = 1):
    logger = get_run_logger()
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        for page_num in range(1, max_pages + 1):
            url = f"https://eg.hatla2ee.com/en/car/search?page={page_num}"
            page.goto(url)
            page.wait_for_timeout(8000)

            cards = page.query_selector_all("div[data-slot='card']")

            for card in cards:
                text = card.inner_text()
                if "EGP" not in text:
                    continue

                lines = [l.strip() for l in text.splitlines() if l.strip()]

                a_tag = card.query_selector("a")
                href = a_tag.get_attribute("href") if a_tag else None
                listing_url = "https://eg.hatla2ee.com" + href if href else None
                title_attr = a_tag.get_attribute("title") if a_tag else ""
                
                # FIX 1: Greedy match (.*) forces the regex to find the LAST valid year (19xx or 20xx)
                # This prevents models like "Peugeot 3008" or "Peugeot 2008" from breaking the split.
                pattern = r"Picture\s+(?P<make_model>.*)\s+(?P<year>19[7-9]\d|20[0-2]\d)\s+(?P<location_color>.*)"
                match = re.search(pattern, title_attr)
                
                make_from_title = None
                model_from_title = None
                year_from_title = None
                color = None
                location_from_title = None

                if match:
                    make_model = match.group("make_model").strip()
                    year_from_title = match.group("year")
                    
                    # FIX 2: Handle compound colors (e.g., "Dark red", "Light blue")
                    location_color = match.group("location_color").strip().split()
                    if len(location_color) >= 2 and location_color[-2].lower() in ["dark", "light"]:
                        color = " ".join(location_color[-2:])
                        location_from_title = " ".join(location_color[:-2]) if len(location_color) > 2 else None
                    elif location_color:
                        color = location_color[-1]
                        location_from_title = " ".join(location_color[:-1]) if len(location_color) > 1 else None
                    
                    # FIX 3: Safely extract multi-word brands
                    for brand in MULTI_WORD_BRANDS:
                        if make_model.startswith(brand):
                            make_from_title = brand
                            model_from_title = make_model[len(brand):].strip()
                            break
                    
                    # Fallback for standard single-word brands
                    if not make_from_title:
                        make_parts = make_model.split()
                        make_from_title = make_parts[0] if make_parts else None
                        model_from_title = " ".join(make_parts[1:]) if len(make_parts) > 1 else None

                results.append({
                    "raw_lines": lines,
                    "page": page_num,
                    "listing_url": listing_url,
                    "title_attr": title_attr,
                    "make": make_from_title,
                    "model": model_from_title,
                    "year": int(year_from_title) if year_from_title else None,
                    "color": color,
                    "location": location_from_title,
                })

        browser.close()

    return results


@task
def clean(raw_data):
    logger = get_run_logger()
    cleaned = []

    for listing in raw_data:
        lines = listing.get("raw_lines", [])
        full_text = " ".join(lines)

        price_match = re.search(r'([\d,]+)\s*EGP', full_text)
        mileage_match = re.search(r'([\d,]+)\s*KM', full_text, re.IGNORECASE)
        transmission_match = re.search(r'\b(Automatic|Manual)\b', full_text, re.IGNORECASE)
        fuel_match = re.search(r'\b(Gas|Diesel|Electric|Hybrid)\b', full_text, re.IGNORECASE)

        price_num = int(price_match.group(1).replace(",", "")) if price_match else None
        
        # FIX 4: Sanitize outlier prices (remove 1,000 EGP spam listings)
        if price_num is not None and price_num < 20000:
            continue

        raw_mileage_str = mileage_match.group(1).replace(",", "") if mileage_match else None
        mileage_num = int(raw_mileage_str) if raw_mileage_str else None
        
        # FIX 5: Standardize impossible mileage inputs
        # If a car is older than 2024 and has < 1000 KM, the user likely dropped the thousands (e.g., 150 = 150,000)
        year = listing.get("year")
        if mileage_num is not None and year is not None:
            if mileage_num < 1000 and year < 2024:
                mileage_num = mileage_num * 1000
        
        # Format back to metric string standard
        formatted_mileage = f"{mileage_num:,} KM" if mileage_num is not None else None

        transmission = transmission_match.group(0) if transmission_match else None
        fuel = fuel_match.group(0) if fuel_match else None

        cleaned.append({
            "title": listing.get("title_attr"),
            "make": listing.get("make"),
            "model": listing.get("model"),
            "year": year,
            "color": listing.get("color"),
            "location": listing.get("location"),
            "listing_url": listing.get("listing_url"),
            "price": price_num,
            "mileage": mileage_num,
            "transmission": transmission,
            "fuel": fuel,
            "page": listing.get("page")
        })

    logger.info(f"Cleaned {len(cleaned)} listings")
    if cleaned:
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
    Evaluate the following car listing data:
    {listing}

    Return a valid JSON object with exactly these fields:
    - "condition": "new" or "used".
    - "price_category": "budget", "mid-range", or "premium".
    - "body_type": "sedan", "suv", "hatchback", "coupe", "truck", "van", or "unknown".
    - "value_for_money": Integer from 1 to 10 assessing the price against the year, mileage, and model.
    - "value_reasoning": One short sentence explaining the value score.

    Return strictly valid JSON. Do not include markdown formatting, code blocks, or explanations.
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
        print(listing)
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


    
from pymongo import ASCENDING

def setup_collection(db):
    if "listings" not in db.list_collection_names():
        db.create_collection("listings", validator={
            "$jsonSchema": {
                "bsonType": "object",
                # Added listing_url to required fields to guarantee uniqueness tracking
                "required": ["title", "price", "year", "listing_url"],
                "properties": {
                    "_id":              {},
                    "title":            {"bsonType": "string"},
                    "price":            {"bsonType": "int"},
                    "year":             {"bsonType": "int"},
                    
                    # Core Scraped Fields
                    "mileage":          {"bsonType": ["int", "null"]}, # Changed from string to int
                    "transmission":     {"bsonType": ["string", "null"]},
                    "fuel":             {"bsonType": ["string", "null"]},
                    "location":         {"bsonType": ["string", "null"]},
                    "city":             {"bsonType": ["string", "null"]},
                    "make":             {"bsonType": ["string", "null"]},
                    "model":            {"bsonType": ["string", "null"]},
                    "color":            {"bsonType": ["string", "null"]},
                    "listing_url":      {"bsonType": "string"},
                    "page":             {"bsonType": ["int", "null"]}, # Tracking field
                    
                    # LLM Enriched Fields
                    "body_type":        {"bsonType": ["string", "null"]},
                    "condition":        {"bsonType": ["string", "null"]},
                    "price_category":   {"bsonType": ["string", "null"]},
                    "value_for_money":  {"bsonType": ["int", "null"]},
                    "value_reasoning":  {"bsonType": ["string", "null"]},
                    
                    # Operational Metadata
                    "scraped_at":       {"bsonType": ["date", "null"]} 
                }
            }
        })
        
        # Prevent database duplication across recurring scrape jobs
        db.listings.create_index([("listing_url", ASCENDING)], unique=True)
        
def make_id(listing: dict) -> str:
    url = listing.get("listing_url", "")
    return hashlib.md5(url.encode()).hexdigest()