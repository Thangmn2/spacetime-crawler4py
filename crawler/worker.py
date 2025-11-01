from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from urllib.parse import urlparse

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # Handle politeness policy
        self._host_next_time = {}
        self._per_host_delay = getattr(self.config, "time_delay", 1.0)



        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            # Run until frontier_queue is empty
            tbd_url = self.frontier.get_tbd_url() #Pop & get next url
            
            self.handle_politeness(tbd_url)  # Politeness policy
            
            if not tbd_url:
                # If queue is empty, end worker
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger) #Download url from cache
            self.logger.info( f"Downloaded {tbd_url}, status <{resp.status}>, using cache {self.config.cache_server}.")

            scraped_urls = scraper.scraper(tbd_url, resp) #Get new urls from scrapper
            for scraped_url in scraped_urls:

                self.frontier.add_url(scraped_url) #Add new urls from scrapper
                
            self.frontier.mark_url_complete(tbd_url) #Mark url as complete

            # time.sleep(self.config.time_delay) #Sleep for politeness

    def handle_politeness(self, url):
        host = (urlparse(url).hostname or "").lower()
        now = time.monotonic()
        ready_at = self._host_next_time.get(host, 0.0)
        if now < ready_at:
            time.sleep(ready_at - now)  # wait until host is allowed again
            # print out to check
            self.logger.debug(f"Waiting {wait_time:.2f}s for politeness on host {host}")
            time.sleep(wait_time)
        # update next allowed time
        self._host_next_time[host] = time.monotonic() + self._per_host_delay


# If handle_politeness does not work, comment it out (line 32) and uncomment general policy (line 48)