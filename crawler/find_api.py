from playwright.sync_api import sync_playwright


def find_api():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()


        def log_response(response):
            if "_search" in response.url:
                try:
                    body = response.json()
                    if body.get("hits", {}).get("total", {}).get("value", 0) > 0:
                        print(body)
                except:
                    pass

        page.on("response", log_response)
        page.goto("https://eg.hatla2ee.com/ar/car/search")
        page.wait_for_timeout(10000)
        browser.close()

find_api()
