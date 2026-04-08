from prefect import flow, task

import httpx

@task(name="scrape-hatla2ee", retries=2)
def scrape(max_pages: int = 5):
    # Send request → validate response → parse JSON → safely extract listings
    results = []
    page_size = 24

    for page_num in range(max_pages):
        query = {
            "from": page_num * page_size,
            "size": page_size,
            "query": {"bool": {}},
            "sort": [{"created_at": "desc"}]
        }

        response = httpx.post(
            "https://eg.hatla2ee.com/api/search/hatla2ee_eg_listings_live/_search",
            json=query,
            timeout=10
        )
        
        response.raise_for_status()
        
        data = response.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        results.extend(hits)
        print(f"Page {page_num + 1}: fetched {len(hits)} listings")

    return results

@task
def clean(raw_data):
    pass

@task
def enrich(clean_data):
    pass

@task
def store(enriched_data):
    pass

@flow(name="hatla2ee-etl")
def noon_pipeline(max_pages: int = 5):
    raw = scrape(max_pages)
    cleaned = clean(raw)
    enriched = enrich(cleaned)
    store(enriched)