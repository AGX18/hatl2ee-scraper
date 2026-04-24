import re

from prefect import flow, task, get_run_logger

from playwright.sync_api import sync_playwright

from openai import OpenAI
from dotenv import load_dotenv
import os
import hashlib
from datetime import datetime
from pymongo import MongoClient, UpdateOne

load_dotenv()

import re
from prefect import task, get_run_logger
from playwright.sync_api import sync_playwright

BODY_TYPE_LOOKUP = {
    # NEW SEDANS
    "tonale": "suv", # Note: Alfa Romeo Tonale is an SUV, moving to correct category below
    "a3": "sedan", "a4": "sedan", "a6": "sedan", "avatr 12": "sedan",
    "218 i": "sedan", "340": "sedan", "523": "sedan", "525": "sedan", "740": "sedan", "i7": "sedan", "m3": "sedan", "m5": "sedan",
    "f3": "sedan", "han": "sedan", "l3": "sedan", "seal": "sedan",
    "arrizo 6": "sedan", "envy": "sedan",
    "c elysee": "sedan", "c4x": "sedan",
    "nubira": "sedan", "nubira 2": "sedan",
    "charger": "sedan", "shine": "sedan", "shahin": "sedan",
    "escort": "sedan", "mondeo": "sedan", "s7": "sedan", "emgrand": "sedan",
    "accord": "sedan", "elantra cn7": "sedan", "elantra hd": "sedan", "verna": "sedan",
    "j7": "sedan", "e5": "sedan", "forte": "sedan", "grand cerato": "sedan", "k3": "sedan", "pride": "sedan", "rio": "sedan",
    "granta": "sedan", "3": "sedan", # Mazda 3
    "190": "sedan", "280": "sedan", "500": "sedan", "cla 180": "sedan", "cla 300l": "sedan", "cla 45 amg": "sedan", "cls class": "sedan", "e 180": "sedan", "e 200 amg": "sedan", "e 250": "sedan", "e 350": "sedan", "eqe 300": "sedan", "eqe 350": "sedan", "eqs 450+": "sedan", "eqs 500": "sedan", "eqs 580": "sedan", "maybach": "sedan", "s 500": "sedan", "s 560": "sedan",
    "350": "sedan", "lancer crystala": "sedan", "lancer puma": "sedan", "n7": "sedan", "406": "sedan", "taycan": "sedan", "saga": "sedan", "toledo": "sedan", "octavia a8": "sedan", "superb": "sedan",
    "a516": "sedan", "a620": "sedan", "impreza": "sedan", "ciaz": "sedan", "swift dzire": "sedan", "s60": "sedan",
    
    # NEW SUVS & CROSSOVERS
    "tonale": "suv", "q5 e-tron": "suv", "q8": "suv", "bj30": "suv",
    "ix": "suv", "ix3": "suv", "x4": "suv", "x4m": "suv", "x5 m": "suv", "x6 m": "suv",
    "leopard 5": "suv", "leopard 7": "suv", "sealion 7": "suv", "song l": "suv",
    "cs 55": "suv", "cs75": "suv", "cs75 plus": "suv",
    "tiggo 7 pro": "suv", "tiggo 8 pro": "suv",
    "c5": "suv", "terramar": "suv", "terios": "suv", "008": "suv", "ds7": "suv", "ex 7": "suv", "500 x": "suv",
    "ecosport": "suv", "kuga": "suv", "emgrand x7": "suv",
    "h6": "suv", "h7": "suv", "jolion": "suv", "crv": "suv", "ix 35": "suv", "kona": "suv",
    "trooper": "suv", "s2": "suv", "f-pace": "suv", "grand cherokee": "suv", "renegade": "suv", "wrangler": "suv",
    "t1": "suv", "t2": "suv", "x70": "suv", "x70 plus": "suv", "x70s": "suv", "x3 pro": "suv", "xceed": "suv",
    "defender": "suv", "lx": "suv", "09": "suv", "scorpio": "suv", "grecale": "suv",
    "eqa 260": "suv", "eqb": "suv", "gla 200": "suv", "gle 450": "suv", "glk 300": "suv", "gls": "suv",
    "one": "suv", "cooper paceman": "suv", "pajero": "suv", "xtrail": "suv", "macan electric": "suv", "01": "suv", "arona": "suv",
    "1": "suv", "3": "suv", "s 06": "suv", "xv": "suv", "vitara": "suv", "model x": "suv", "c-hr": "suv", "urban cruiser": "suv",
    "taos": "suv", "tayron": "suv", "xc40 recharge": "suv", "xc90": "suv", "yu7": "suv", "7x": "suv",

    # NEW HATCHBACKS
    "c3": "hatchback", "c4": "hatchback", "matiz": "hatchback",
    "bravo": "hatchback", "palio": "hatchback", "polonez": "hatchback", "punto evo": "hatchback",
    "grand i10": "hatchback", "i30": "hatchback", "matrix": "hatchback",
    "picanto": "hatchback", "kalina": "hatchback", "samara": "hatchback",
    "02": "hatchback", 
    "a 180": "hatchback", "b 150": "hatchback", 
    "cooper": "hatchback", "cooper f55": "hatchback",
    "mirage": "hatchback", "a113": "hatchback", "a213": "hatchback", "maruti": "hatchback",
    "golf 6": "hatchback", "golf 7": "hatchback",

    # NEW COUPES
    "vantage": "coupe", "a5": "coupe", "rs5": "coupe",
    "420": "coupe", "m4": "coupe", "m850": "coupe",
    "camaro": "coupe", "corvette": "coupe", "emira": "coupe",
    "c63 amg": "coupe", "cle": "coupe", "cle 200": "coupe", "cle 53": "coupe", "gt 43": "coupe",
    "cascada": "coupe", "911": "coupe", "boxster": "coupe", "cayman": "coupe",

    # NEW VANS / MPVS
    "new star": "van", "n200": "van", "n300": "van", "pacifica": "van", "grand c4 spacetourer": "van",
    "m70": "van", "carnival": "van", "eqv 300": "van", "viano": "van", 
    "x30": "van", "van": "van", "turan": "van", "oasi 540": "van",

    # NEW TRUCKS
    "k01": "truck", "hummer ev": "truck", "sierra": "truck",
    # Sedans
    "elantra": "sedan", "accent": "sedan", "sonata": "sedan", "azera": "sedan", 
    "excel": "sedan", "avante": "sedan", "elantra md": "sedan", "elantra ad": "sedan", "accent rb": "sedan",
    "corolla": "sedan", "camry": "sedan", "yaris": "sedan", "cressida": "sedan",
    "logan": "sedan", "symbol": "sedan", "scala": "sedan", "megane": "sedan", "taliant": "sedan", "fluence": "sedan",
    "lancer": "sedan", "galant": "sedan", "lancer ex shark": "sedan", "attrage": "sedan",
    "320": "sedan", "318": "sedan", "520": "sedan", "528": "sedan", "730": "sedan", "320i": "sedan",
    "c200": "sedan", "c300": "sedan", "e200": "sedan", "e300": "sedan", "c 180": "sedan", "c 200": "sedan", "e 200": "sedan", "e 300": "sedan", "s 400": "sedan", "s 450": "sedan", "200": "sedan", "cla 200": "sedan", "a 200": "sedan",
    "a4": "sedan", "a6": "sedan",
    "jetta": "sedan", "passat": "sedan", "bora": "sedan", "id7": "sedan",
    "octavia": "sedan", "rapid": "sedan", "octavia a4": "sedan", "fantasia": "sedan",
    "vectra": "sedan", "insignia": "sedan", 
    "408": "sedan", "301": "sedan", "508": "sedan", "405": "sedan", "407": "sedan",
    "cerato": "sedan", "optima": "sedan", "k5": "sedan", "spectra": "sedan",
    "sunny": "sedan", "sentra": "sedan",
    "optra": "sedan", "lanos": "sedan", "lanos 2": "sedan", "aveo": "sedan", "cruze": "sedan",
    "tipo": "sedan", "siena": "sedan", "regata": "sedan", "128": "sedan", "131": "sedan",
    "arrizo": "sedan", "arrizo 5": "sedan", "a15": "sedan",
    "u5 plus": "sedan", "alsvin": "sedan", "emgrand 7": "sedan",
    "civic": "sedan", "city": "sedan",
    "mg 6": "sedan", "mg 5": "sedan", "mg 7": "sedan", "mg gt": "sedan",
    
    # Hatchbacks
    "i10": "hatchback", "i20": "hatchback", "getz": "hatchback",
    "charade": "hatchback", "sirion": "hatchback",
    "polo": "hatchback", "golf": "hatchback", "golf 3": "hatchback", "golf 5": "hatchback", "id3": "hatchback",
    "fabia": "hatchback", "felicia": "hatchback",
    "208": "hatchback", "206": "hatchback", "207": "hatchback", "308 sw": "hatchback",
    "clio": "hatchback", "sandero": "hatchback", "sandero step way": "hatchback",
    "spark": "hatchback", "frv": "hatchback",
    "uno": "hatchback", "punto": "hatchback", "grand punto": "hatchback", "127": "hatchback",
    "leon": "hatchback", "ibiza": "hatchback", "leon cupra": "hatchback",
    "astra": "hatchback", "corsa": "hatchback",
    "swift": "hatchback", "celerio": "hatchback", "alto": "hatchback",
    "x pandino": "hatchback", "benni mini": "hatchback", "smart 1": "hatchback", "smart 3": "hatchback",

    # SUVs & Crossovers
    "tucson": "suv", "tucson gdi": "suv", "santa fe": "suv", "creta": "suv", "venue": "suv",
    "sportage": "suv", "sorento": "suv", "seltos": "suv", "ev5": "suv",
    "rav4": "suv", "land cruiser": "suv", "rush": "suv", "fortuner": "suv",
    "tiguan": "suv", "touareg": "suv", "t-roc": "suv",
    "glc": "suv", "gle": "suv", "gla": "suv", "glk": "suv", "glc 200": "suv", "glc 300": "suv", "glc 43": "suv", "g63": "suv", "macan": "suv", "cayenne s": "suv",
    "x1": "suv", "x3": "suv", "x5": "suv", "ix1": "suv", "i3": "suv",
    "q3": "suv", "q5": "suv", "q7": "suv", "q4 e-tron": "suv",
    "forester": "suv", "outback": "suv",
    "cx-5": "suv", "cx-3": "suv",
    "kadjar": "suv", "duster": "suv", "captur": "suv", "austral": "suv", "kardian": "suv",
    "2008": "suv", "3008": "suv", "5008": "suv",
    "kodiaq": "suv", "karoq": "suv",
    "eclipse cross": "suv", "asx": "suv", "outlander": "suv",
    "x55 plus": "suv", "x7": "suv",
    "tiggo": "suv", "tiggo 3": "suv", "tiggo 7": "suv", "tiggo 8": "suv",
    "mg hs": "suv", "mg zs": "suv", "mg rx5": "suv", "rx5 plus": "suv",
    "sealion": "suv", "leopard 8": "suv",
    "grandland": "suv", "mokka": "suv", "crossland": "suv",
    "jetour x70": "suv", "jetour x90": "suv",
    "geely emgrand x7": "suv", "cool ray": "suv", "gx3 pro": "suv",
    "formentor": "suv", "tarraco": "suv",
    "range rover": "suv", "range rover sport": "suv", "range rover evoque": "suv", "range rover vogue": "suv", "velar": "suv",
    "escalade": "suv", "bentayga": "suv", "patrol": "suv", "qashqai": "suv", "juke": "suv", "cherokee": "suv",
    "t5 evo": "suv", "s 07": "suv", "s 05": "suv", "eagle 580": "suv", "captiva": "suv", "model y": "suv",

    # Coupes
    "mustang": "coupe", "z4": "coupe", "m2": "coupe", "c63": "coupe", "cls": "coupe", "tt": "coupe",

    # Vans/MPVs
    "h1": "van", "starex": "van", "berlingo": "van", "doblo": "van", "carens": "van", "xpander": "van", "v 250": "van", "town & country": "van", "voyager": "van", "g50": "van",

    # Trucks
    "hilux": "truck", "ranger": "truck", "navara": "truck", "l200": "truck", "cybertruck": "truck",
    
    # ADDITIONAL SEDANS
    "giulia": "sedan", "12": "sedan", "316": "sedan", "518": "sedan", 
    "a11": "sedan", "cielo": "sedan", "lacetti": "sedan", "leganza": "sedan", "nubira 1": "sedan", 
    "ec 7": "sedan", "focus": "sedan", "fusion": "sedan", "imperial": "sedan", "c30": "sedan", 
    "accent hci": "sedan", "q": "sedan", "s-type": "sedan", "pegas": "sedan", "sephia": "sedan", 
    "929": "sedan", "180": "sedan", "c 250": "sedan", "e 280": "sedan", "eqs 450": "sedan", 
    "s 320": "sedan", "5": "sedan", "6": "sedan", "7": "sedan", "gt": "sedan", 
    "bluebird": "sedan", "505": "sedan", "octavia a5": "sedan", "avensis": "sedan", 
    "corona": "sedan", "su7": "sedan",

    # ADDITIONAL SUVS
    "stelvio": "suv", "x35": "suv", "x2": "suv", "x3 m": "suv", "x6": "suv", 
    "sealion 6": "suv", "song plus": "suv", "tiggo 4 pro": "suv", "tiggo 8 pro max": "suv", 
    "vx": "suv", "t5": "suv", "starray": "suv", "pilot": "suv", "js4": "suv", 
    "s4": "suv", "komodo": "suv", "liberty": "suv", "discovery sport": "suv", 
    "hs": "suv", "rx5": "suv", "zs": "suv", "country man": "suv", "outlander phev": "suv", 
    "outlander sport": "suv", "ateca": "suv", "kamiq": "suv", "dx8s coupe": "suv", 
    "s 09": "suv", "fronx": "suv", "id 4": "suv", "id 6": "suv", "id unyx": "suv", 
    "xc60": "suv", "g6": "suv", "g9": "suv",

    # ADDITIONAL HATCHBACKS
    "giulietta": "hatchback", "1 series": "hatchback", "frv cross": "hatchback", 
    "seagull": "hatchback", "juliet": "hatchback", "vita": "hatchback", "126": "hatchback", 
    "pandino": "hatchback", "b 180": "hatchback", "b 200": "hatchback", "tiida": "hatchback", 
    "307": "hatchback", "gen 2": "hatchback", "beetle": "hatchback",

    # ADDITIONAL COUPES
    "cerato koup": "coupe", "cle 300": "coupe",

    # ADDITIONAL VANS
    "santamo": "van", "q22": "van", "vito": "van",

    # ADDITIONAL TRUCKS
    "pickup": "truck", "d max": "truck", "new boarding": "truck",
}
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

        for page_num in range(0, max_pages + 1):
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
    logger.info("cleaning data...")
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


def enrich_listing_locally(listing: dict) -> dict | None:
    logger = get_run_logger()
    price = listing.get("price")
    mileage = listing.get("mileage")
    year = listing.get("year")
    model = listing.get("model")

    # 1. Determine Condition
    condition = "new" if mileage == 0 and year >= datetime.now().year - 1 else "used"


    
    # 2. Determine Price Category
    price_category = "unknown"
    if price is not None:
        if price < 1_000_000:
            price_category = "budget"
        elif 1_000_000 <= price <= 3_000_000:
            price_category = "mid-range"
        else:
            price_category = "premium"
            
    model = listing.get("model", "")
    model_key = str(model).lower().strip()


            
    # 3. Determine Body Type via Lookup
    body_type = BODY_TYPE_LOOKUP.get(model_key, "unknown")

    # 4. Algorithmic Value for Money Score (1 to 10)
    value_score = None
    value_reasoning = None
    
    if price and mileage is not None and year:
        # Base score out of 10
        score = 7.0 
        
        # Age penalty (subtract 0.2 points for every year old)
        age = 2024 - year
        score -= (age * 0.2)
        
        # Mileage penalty (subtract 0.5 points for every 50,000 KM)
        score -= (mileage / 50_000) * 0.5
        
        # Price logic (If it's an old car but still highly priced, heavily penalize)
        if age > 10 and price > 500_000:
            score -= 2.0
            
        # Floor and ceiling the score between 1 and 10
        value_score = max(1, min(10, int(round(score))))
        
        # Generate dynamic reasoning
        if value_score >= 8:
            value_reasoning = f"Excellent value: Good balance of age ({year}) and mileage ({mileage:,} KM)."
        elif 4 <= value_score <= 7:
            value_reasoning = f"Fair market value for a {year} model with {mileage:,} KM."
        else:
            value_reasoning = f"Poor value: High asking price relative to its age ({year}) and heavy usage ({mileage:,} KM)."

    # Merge and return
    return {
        **listing,
        "condition": condition,
        "price_category": price_category,
        "body_type": body_type,
        "value_for_money": value_score,
        "value_reasoning": value_reasoning
    }

@task
def enrich_locally(clean_data):
    logger = get_run_logger()
    logger.info("enriching data...")
    to_enrich = clean_data  # For local enrichment, we enrich all cleaned data without checking the database
    logger = get_run_logger()
    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    db = mongo_client["hatla2ee"]
    setup_collection(db)
    collection = db["listings"]
    to_enrich = []
    for listing in clean_data:
        if not listing.get("model") or not listing.get("price") or not listing.get("year"):
            logger.warning(f"Skipping enrichment for listing due to missing critical fields: {listing.get('listing_url')}")
            continue
        doc_id = make_id(listing)
        if not collection.find_one({"_id": doc_id}):
            to_enrich.append(listing)
    mongo_client.close()
    logger.info(f"Skipping {len(clean_data) - len(to_enrich)} already stored, enriching {len(to_enrich)} new listings locally without API calls")
    enriched = [enrich_listing_locally(listing) for listing in to_enrich]
    logger.info(f"Enriched {len(enriched)} listings locally without API calls")
    if enriched:
        logger.info(str(enriched[0]))
    return enriched

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
    return enriched


@task
def store(enriched_data):
    logger = get_run_logger()

    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["hatla2ee"]
    collection = db["listings"]

    upserted = 0
    for listing in enriched_data:
        logger.info(f"{listing['title']} - {listing['body_type']}")
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

@task(name="train-price-model")
def train_price_model():
    logger = get_run_logger()
    logger.info("Retraining AI price model with new data...")
    import sys
    import os
    sys.path.append(os.getcwd())
    try:
        from train_model import main as train
        train()
        logger.info("AI model retrained successfully!")
    except Exception as e:
        logger.error(f"Failed to retrain AI model: {e}")

@flow(name="hatla2ee-etl")
def hatla2ee_pipeline(max_pages: int = 5):
    raw = scrape(max_pages)
    cleaned = clean(raw)
    enriched = enrich_locally(cleaned)
    store(enriched)
    train_price_model()


    
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


def backfill_unknown_body_types():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["hatla2ee"]
    collection = db["listings"]

    # Target only listings that are currently unknown or null
    query = {"body_type": {"$in": ["unknown", None]}}
    unknown_listings = collection.find(query)

    operations = []
    matched_count = 0

    for listing in unknown_listings:
        model = listing.get("model")
        if not model:
            continue

        # Re-apply the standardization logic
        model_key = str(model).lower().strip()
        
        # Check the updated dictionary
        new_body_type = BODY_TYPE_LOOKUP.get(model_key, "unknown")

        # If a match is found in the new dictionary, prep an update
        if new_body_type != "unknown":
            matched_count += 1
            operations.append(
                UpdateOne(
                    {"_id": listing["_id"]},
                    {"$set": {"body_type": new_body_type}}
                )
            )

    # Execute all updates in a single batch network request
    if operations:
        print(f"Found {matched_count} listings to update. Executing bulk write...")
        result = collection.bulk_write(operations)
        print(f"Successfully updated {result.modified_count} listings.")
    else:
        print("No matches found to update.")
        
        
if __name__ == "__main__":
    # Update the URI and Database name as needed
    backfill_unknown_body_types()