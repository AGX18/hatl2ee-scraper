from prefect import flow, task

@task
def scrape():
    pass

@task
def clean(raw_data):
    pass

@task
def enrich(clean_data):
    pass

@task
def store(enriched_data):
    pass

@flow(name="noon-etl")
def noon_pipeline(search_term: str):
    raw      = scrape()
    cleaned  = clean(raw)
    enriched = enrich(cleaned)
    store(enriched)