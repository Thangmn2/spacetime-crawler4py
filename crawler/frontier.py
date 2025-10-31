import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty
import threading

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list() # List of urls to be downloaded (queue)
        
        # Handle restarting vs resuming crawler
        if not os.path.exists(self.config.save_file) and not restart: # Save file does not exist, but request to load save.
            self.logger.info( f"Did not find save file {self.config.save_file}, starting from seed." )
        elif os.path.exists(self.config.save_file) and restart: # Save file does exists, but request to start from seed.
            self.logger.info( f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        # Load existing save file, or create one if it does not exist.
        self.thread_local = threading.local() 
        self.save_file = self.config.save_file
        self.save = shelve.open(self.save_file, writeback=True)
        
        # Restart from the seed_urls
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        # Resume
        else:
            # Populate the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save: # If save file is empty, populate using seed urls
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        # Used to populate from resume

        save = self.get_save() #new
        total_count = len(self.save)
        tbd_count = 0
        #Iterate through save and populate with non-completed urls
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url) 
                tbd_count += 1
        self.logger.info( f"Found {tbd_count} urls to be downloaded from {total_count} total urls discovered.")


    def get_tbd_url(self):
        # Remove & return next url in tbd_queue
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        # Add url to tbd_queue
        url = normalize(url)
        urlhash = get_urlhash(url)
        save = self.get_save()
        if urlhash not in save:
            save[urlhash] = (url, False)
            save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        save = self.get_save()
        if urlhash not in save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        save[urlhash] = (url, True)
        save.sync()

    def get_save(self):
        """Ensure each thread has its own shelve connection."""
        if not hasattr(self.thread_local, "save"):
            self.thread_local.save = self.save
        return self.thread_local.save