#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import json
import multiprocessing
import sys

import requests

PY3 = sys.version_info[0] == 3

COUB_URL = 'http://coub.com'
SEARCH_URL = COUB_URL + '/api/v2/search'
WEEKLY_DIGESTS_URL = COUB_URL + '/api/v2/weekly_digests'


def get_audio_track_titles(media_blocks):
    """Get audio track titles"""
    if not media_blocks:
        return '', ''
    audio_track_title = media_blocks.get('audio_track', {}).get('title', '')
    external_video_title = media_blocks.get('external_video', {}).get('title', '')
    return audio_track_title, external_video_title


def get_weekly_digests():
    """Get weekly digests"""
    req = requests.get(WEEKLY_DIGESTS_URL)
    return json.loads(req.text).get('weekly_digests', [])


def get_weekly_digest_coubs_on_page(index, page_id):
    """Get weekly digest coubs on defined page"""
    req = requests.get('{url}/{index}/coubs?page={page_id}'.format(
        url=WEEKLY_DIGESTS_URL, index=index, page_id=page_id))
    return json.loads(req.text)


def get_all_weekly_digest_coubs(index):
    """Get all weekly digest coubs (from all pages)"""
    req = requests.get('{url}/{index}/coubs'.format(url=WEEKLY_DIGESTS_URL, index=index))
    page_data = req.json()
    page = page_data.get('page')
    total_pages = page_data.get('total_pages')
    all_coubs = [page_data]
    for page_id in range(1, total_pages + 1):
        # skip current page
        if page_id == page:
            continue
        page_id_data = get_weekly_digest_coubs_on_page(index, page_id)
        all_coubs.append(page_id_data)
    return all_coubs


def main():
    print('-' * 80)
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() * 4)
    weekly_digests = get_weekly_digests()
    weekly_digest_ids = [weekly_digest.get('id') for weekly_digest in weekly_digests]
    all_weekly_coubs_pages_res = pool.map(get_all_weekly_digest_coubs, weekly_digest_ids)
    if PY3:
        csvfile = open('output.csv', 'w', encoding='utf-8')
    else:
        csvfile = open('output.csv', 'w')
    writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for all_weekly_coubs_pages in all_weekly_coubs_pages_res:
        for coubs_page in all_weekly_coubs_pages:
            coubs = coubs_page.get('coubs', [])
            for coub in coubs:
                audio_track_titles = get_audio_track_titles(coub.get('media_blocks'))
                if PY3:
                    writer.writerow(audio_track_titles)
                else:
                    writer.writerow([title.encode('utf-8') for title in audio_track_titles])
    csvfile.close()
    print('-' * 80)


if __name__ == '__main__':
    main()
