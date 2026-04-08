from prefect import flow, task, get_run_logger

from playwright.sync_api import sync_playwright


@task(name="scrape-hatla2ee", retries=2)
def scrape(max_pages: int = 5):
    logger = get_run_logger()
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://eg.hatla2ee.com/ar/car/search")
        page.wait_for_timeout(8000)

        cards = page.query_selector_all("div[data-slot='card']")
        for card in cards:
            text = card.inner_text()
            if "جنيه" not in text and "EGP" not in text:
                continue
            
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            results.append({"raw_lines": lines})

        logger.info(f"Extracted {len(results)} listings")

        browser.close()

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