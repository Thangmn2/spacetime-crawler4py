from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
   


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()

    from scraper import reportData

    unique_pages, (longest_url, longest_wc), top50, subs = reportData()
    print(f"unique pages: {unique_pages}")
    print(f"longest page: {longest_url}  (words: {longest_wc})")
    print("\ntop 50 words:")
    for w, c in top50:
        print(f"{w}: {c}")
    print("\nsubdomains:")
    for sub, n in subs:
        print(f"{sub}, {n}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
