from glob import iglob
from bs4 import BeautifulSoup
from typing import Iterator
import pathlib
from pathlib import Path
import more_itertools
from more_itertools import first, ilen
import io
import re
from bot_api import (BatchSyncComplete, BatchSyncStatus, BatchSynced, BotEvents, BatchCompleted, BatchCompletionStatus)
import os
import sys
import configargparse

class AdFile:
    def __init__(self, filename: pathlib.Path):
        (self.bot_name,
        self.try_num,
        self.ad_seen_at,
        self.video_watched) = filename.stem.split("#")

class AdStorage:
    def __init__(self, ad_dir: pathlib.Path):
        self.path = ad_dir
        self.run_id = int(self.path.name)
        (self.host_hostname, self.hostname) = self.path.parents[0].name.split("#")
        self.location = self.path.parents[1].name

def count_xml_files(ad_dir: pathlib.Path) -> int:
    return ilen(xml_files(ad_dir))

def xml_files(ad_dir: pathlib.Path) -> Iterator[pathlib.Path]:
    return ad_dir.glob("*.xml")

def html_files(ad_dir: pathlib.Path) -> Iterator[pathlib.Path]:
    return ad_dir.glob("*.html")

def extract_ip_from_html_player(html_filehandle: io.TextIOWrapper):
    """Returns 0.0.0.0 if no ip address is found in the html file"""
    html_file = html_filehandle.read()
    ip_containing = re.search(r"(?:\\u0026v|%26|%3F)ip(?:%3D|=)(.*?)(?:,|;|%26|\\u0026)", html_file, re.DOTALL)
    if ip_containing is None:
        return "0.0.0.0"
    return ip_containing.group(1)

def count_non_ad_requests(ad_dir: pathlib.Path) -> int:
    return ilen(ad_dir.joinpath("noAds.csv").open())

def last_request_time(ad_dir: pathlib.Path) -> int:
    """Returns -1 for a request time if there were no requests with no ads"""
    ad_filenames = ad_dir.joinpath("noAds.csv").open().read().splitlines()[-10:]
    for ad_filename in reversed(ad_filenames):
        try:
            abs_ad_filepath: pathlib.Path = ad_dir / ad_filename
            return int(AdFile(abs_ad_filepath).ad_seen_at)
        except (AttributeError, ValueError) as e:
            # Possible file corruption
            print(abs_ad_filepath)
            continue
            abs_ad_filepath: pathlib.Path = ad_dir / ad_filename
            return int(AdFile(abs_ad_filepath).ad_seen_at)
            print(ad_filename)
    else:
        return -1

    
if __name__ == "__main__":
    p = configargparse.ArgumentParser()

    p.add('--ad-dir',
          required=True,
          help='Base directory where all ad source files are stored',
          env_var='SOURCE_AD_STORAGE_DIR')

    args = p.parse_args()
    source_ad_dir = args.ad_dir
    base_dir = Path(source_ad_dir)
    new_data_dirs = [x.parent for x in base_dir.glob("*/*#*/*/noAds.csv")]

    for ad_dir in new_data_dirs:
        ad_count = count_xml_files(ad_dir)
        non_ads = count_non_ad_requests(ad_dir)
        try:
            first_html = first(html_files(ad_dir))
            external_ip = extract_ip_from_html_player(first_html.open())
            if external_ip == "0.0.0.0":
                print(first_html)
        except ValueError:
            # No html files present
            external_ip = "0.0.0.0"
            print(ad_dir)
        total_requests = ad_count + non_ads
        last_request = last_request_time(ad_dir)
        ad = AdStorage(ad_dir)
        completion_msg = BatchCompleted(status=BatchCompletionStatus.COMPLETE, hostname=ad.hostname, run_id=ad.run_id,
                                       external_ip=external_ip, bots_in_batch=8,
                                       requests=total_requests, host_hostname=ad.host_hostname,
                                       location=ad.location, ads_found=ad_count, timestamp=last_request)
        sync_msg = BatchSynced(completion_msg, BatchSyncComplete())
        print(sync_msg.to_json())
        print(f"total_requests: {total_requests}, ads: {ad_count}, non_ads: {non_ads}, ip: {external_ip}")
