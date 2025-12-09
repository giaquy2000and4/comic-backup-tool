import asyncio
import json
import re
import sqlite3
import random  # <--- Mới thêm: Để tạo thời gian ngẫu nhiên
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
from typing import List, Dict, Optional
import argparse


class DatabaseManager:
    def __init__(self, db_path: str = "nhentai_data.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
                  CREATE TABLE IF NOT EXISTS galleries
                  (
                      id
                      TEXT
                      PRIMARY
                      KEY,
                      title_english
                      TEXT,
                      title_japanese
                      TEXT,
                      tags
                      TEXT,
                      pages
                      INTEGER,
                      uploaded_date
                      TEXT,
                      downloaded
                      BOOLEAN
                      DEFAULT
                      0,
                      torrent_path
                      TEXT,
                      scraped_at
                      TIMESTAMP
                  )
                  ''')
        conn.commit()
        conn.close()

    def add_gallery_id(self, gallery_id: str):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO galleries (id) VALUES (?)', (gallery_id,))
            conn.commit()
        finally:
            conn.close()

    def update_gallery_metadata(self, gallery_id: str, metadata: dict, downloaded: bool, torrent_path: str):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute('''
                      UPDATE galleries
                      SET title_english  = ?,
                          title_japanese = ?,
                          tags           = ?,
                          pages          = ?,
                          downloaded     = ?,
                          torrent_path   = ?,
                          scraped_at     = ?
                      WHERE id = ?
                      ''', (
                          metadata.get('title_english'),
                          metadata.get('title_japanese'),
                          json.dumps(metadata.get('tags', [])),
                          metadata.get('pages', 0),
                          downloaded,
                          str(torrent_path),
                          datetime.now().isoformat(),
                          gallery_id
                      ))
            conn.commit()
        finally:
            conn.close()

    def is_downloaded(self, gallery_id: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT downloaded FROM galleries WHERE id = ?', (gallery_id,))
        result = c.fetchone()
        conn.close()
        return result[0] == 1 if result else False

    def export_to_json(self, output_file: str):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('SELECT * FROM galleries WHERE title_english IS NOT NULL')
        rows = [dict(row) for row in c.fetchall()]

        for row in rows:
            if row['tags']:
                try:
                    row['tags'] = json.loads(row['tags'])
                except:
                    pass

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Exported metadata to {output_file}")
        conn.close()


class NHentaiTorrentDownloader:
    def __init__(self, cookies_file: str, output_dir: str = "./torrents", db_path: str = "nhentai_data.db"):
        self.cookies_file = cookies_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.base_url = "https://nhentai.net"
        self.db = DatabaseManager(db_path)

    def parse_netscape_cookies(self) -> List[Dict]:
        cookies = []
        try:
            with open(self.cookies_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 7:
                        cookie = {
                            'name': parts[5], 'value': parts[6], 'domain': parts[0],
                            'path': parts[2], 'secure': parts[3] == 'TRUE',
                            'httpOnly': False, 'sameSite': 'Lax'
                        }
                        if parts[4] and parts[4] != '0':
                            cookie['expires'] = int(parts[4])
                        cookies.append(cookie)
            print(f"Loaded {len(cookies)} cookies.")
            return cookies
        except Exception as e:
            print(f"Error parsing cookies: {e}")
            return []

    async def wait_for_cloudflare(self, page):
        print("Waiting for Cloudflare check...")
        try:
            await page.wait_for_selector('.container', state='visible', timeout=10000)
            # Thêm một chút random wait sau khi qua CF cho an toàn
            await asyncio.sleep(random.uniform(1.0, 2.5))
            return True
        except:
            content = await page.content()
            if "Just a moment" in content or "Verify you are human" in content:
                print("Stuck at Cloudflare. Waiting longer...")
                await page.wait_for_timeout(5000)
                return True
            return True

    async def get_favourites_pages(self, page) -> int:
        await page.goto(f"{self.base_url}/favorites/", wait_until="domcontentloaded")
        await self.wait_for_cloudflare(page)

        if "login" in page.url:
            raise Exception("Not logged in! Check cookies.")

        try:
            pagination = await page.query_selector('.pagination')
            if pagination:
                last_page = await pagination.query_selector('a.last')
                if last_page:
                    href = await last_page.get_attribute('href')
                    match = re.search(r'page=(\d+)', href)
                    if match:
                        return int(match.group(1))
        except:
            pass
        return 1

    async def get_gallery_ids_from_page(self, page, page_num: int) -> List[str]:
        url = f"{self.base_url}/favorites/?page={page_num}"
        await page.goto(url, wait_until="domcontentloaded")
        await self.wait_for_cloudflare(page)

        gallery_ids = []
        elements = await page.query_selector_all('.gallery > a')

        for el in elements:
            href = await el.get_attribute('href')
            match = re.search(r'/g/(\d+)/', href)
            if match:
                gallery_ids.append(match.group(1))

        return gallery_ids

    async def process_gallery(self, page, gallery_id: str, metadata_only: bool = False) -> bool:
        """Truy cập trang gallery, lấy metadata và tải torrent (tùy chọn)"""
        download_path = self.output_dir / f"{gallery_id}.torrent"

        # Nếu file đã tồn tại và DB báo đã tải xong, thì bỏ qua
        if self.db.is_downloaded(gallery_id) and download_path.exists():
            print(f"  [Skip] {gallery_id} already downloaded.")
            return True

        url = f"{self.base_url}/g/{gallery_id}/"
        try:
            # Random wait nhẹ trước khi load trang
            await asyncio.sleep(random.uniform(0.5, 1.5))

            await page.goto(url, wait_until="domcontentloaded", timeout=45000)

            # Scrape Metadata
            metadata = {
                'title_english': '',
                'title_japanese': '',
                'tags': [],
                'pages': 0
            }

            title_h1 = await page.query_selector('h1.title')
            title_h2 = await page.query_selector('h2.title')
            if title_h1: metadata['title_english'] = await title_h1.inner_text()
            if title_h2: metadata['title_japanese'] = await title_h2.inner_text()

            tag_elements = await page.query_selector_all('.tag-container.field-name')
            for tag_el in tag_elements:
                text = await tag_el.inner_text()
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if not lines: continue

                category = lines[0].replace(':', '').strip().lower()
                tag_names = await tag_el.query_selector_all('.name')
                for name_el in tag_names:
                    tag_val = await name_el.inner_text()
                    metadata['tags'].append(f"{category}:{tag_val}")

                if 'pages' in category:
                    try:
                        metadata['pages'] = int(lines[1])
                    except:
                        pass

            print(f"  Metadata: {metadata['title_english'][:50]}... ({metadata['pages']} pages)")

            # Download Torrent (Only if not metadata_only)
            downloaded = False

            if not metadata_only:
                try:
                    async with page.expect_download(timeout=15000) as download_info:
                        download_btn = await page.query_selector('a[href*="/download"]')
                        if not download_btn:
                            await page.evaluate(f'window.location.href = "{url}download"')
                        else:
                            await download_btn.click()

                        download = await download_info.value
                        await download.save_as(download_path)
                        downloaded = True
                        print(f"  Downloaded: {gallery_id}.torrent")
                except Exception as e:
                    print(f"  Download failed for {gallery_id}: {str(e)[:50]}")
            else:
                print(f"  [Metadata Only] Skipped torrent download for {gallery_id}")

            # Update DB
            self.db.update_gallery_metadata(gallery_id, metadata, downloaded, str(download_path))
            return True

        except Exception as e:
            print(f"  Error processing {gallery_id}: {e}")
            return False

    async def run(self, max_galleries: int = None, start_page: int = 1, only_single_page: bool = False,
                  skip_scrape: bool = False, metadata_only: bool = False):
        cookies = self.parse_netscape_cookies()
        if not cookies: return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=['--start-maximized',
                                                                    '--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(viewport=None)
            await context.add_cookies(cookies)
            page = await context.new_page()

            all_ids = []

            # Giai đoạn 1: Thu thập IDs
            if not skip_scrape:
                print("\n=== Phase 1: Scraping Gallery IDs ===")
                try:
                    total_pages = await self.get_favourites_pages(page)
                    print(f"Total favorites pages: {total_pages}")

                    if only_single_page:
                        end_page = start_page
                    else:
                        end_page = total_pages

                    for i in range(start_page, end_page + 1):
                        print(f"Scraping list page {i}/{end_page}...")
                        ids = await self.get_gallery_ids_from_page(page, i)
                        for gid in ids:
                            self.db.add_gallery_id(gid)
                        all_ids.extend(ids)

                        if max_galleries and len(all_ids) >= max_galleries:
                            all_ids = all_ids[:max_galleries]
                            break

                        # ANTI-BAN: Random delay giữa các trang list
                        delay = random.uniform(2.5, 5.0)
                        print(f"  Waiting {delay:.1f}s before next page...")
                        await asyncio.sleep(delay)

                except Exception as e:
                    print(f"Error scraping lists: {e}")
            else:
                print("\n=== Skipping Phase 1 (List Scraping) ===")

            print(f"\nCollected {len(all_ids)} IDs in this session.")

            # Giai đoạn 2: Xử lý từng Gallery
            mode_str = "Metadata Only" if metadata_only else "Metadata & Download"
            print(f"\n=== Phase 2: Processing Galleries ({mode_str}) ===")

            count = 0
            processed_count = 0

            for gid in all_ids:
                if max_galleries and count >= max_galleries: break

                print(f"[{count + 1}/{len(all_ids)}] Processing {gid}...")
                success = await self.process_gallery(page, gid, metadata_only=metadata_only)

                count += 1
                if success:
                    processed_count += 1

                # ANTI-BAN: Cơ chế nghỉ giải lao (Cooldown)
                # Cứ 20 truyện thì nghỉ dài 1 lần
                if processed_count % 20 == 0 and processed_count > 0:
                    long_break = random.uniform(20.0, 40.0)
                    print(f"\n  [ANTI-BAN] Taking a long break for {long_break:.1f}s...\n")
                    await asyncio.sleep(long_break)
                else:
                    # Delay ngẫu nhiên thông thường giữa các truyện
                    short_delay = random.uniform(3.0, 6.0)
                    # print(f"  Waiting {short_delay:.1f}s...")
                    await asyncio.sleep(short_delay)

            # Giai đoạn 3: Export
            print("\n=== Phase 3: Exporting Data ===")
            self.db.export_to_json(self.output_dir / "galleries_metadata.json")

            await browser.close()


async def main():
    parser = argparse.ArgumentParser(description='NHentai Downloader with Database')
    parser.add_argument('cookies_file', help='Cookies file path')
    parser.add_argument('-o', '--output', default='./torrents', help='Output directory')
    parser.add_argument('-m', '--max', type=int, help='Max items to process')
    parser.add_argument('-s', '--start-page', type=int, default=1, help='Start page')
    parser.add_argument('--only-page', action='store_true', help='Only process start page')
    parser.add_argument('--skip-list', action='store_true', help='Skip scraping list pages')
    parser.add_argument('--metadata-only', action='store_true', help='Only scrape metadata, do not download torrents')

    args = parser.parse_args()

    downloader = NHentaiTorrentDownloader(args.cookies_file, args.output)
    await downloader.run(
        max_galleries=args.max,
        start_page=args.start_page,
        only_single_page=args.only_page,
        skip_scrape=args.skip_list,
        metadata_only=args.metadata_only
    )


if __name__ == "__main__":
    asyncio.run(main())