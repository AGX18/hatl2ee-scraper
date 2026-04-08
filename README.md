# Noon project

So the ETL flow will be:
Task 1: scrape()      → raw product data
Task 2: clean()       → remove nulls, normalize prices
Task 3: enrich()      → GPT-5.4-mini classifies products  
Task 4: store()       → save to MongoDB

start the server
`uv run prefect server start`

## ETL pipeline

`Hatla2ee links → CarCrawler → structured car data → storage`
