from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            # Run until frontier_queue is empty
            tbd_url = self.frontier.get_tbd_url() #Pop & get next url
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

            
            time.sleep(self.config.time_delay) #Sleep for politeness
 