import asyncio
import json
import re
import sqlite3
import csv
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
        # Bảng lưu thông tin gallery
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
        """Chỉ thêm ID vào DB nếu chưa tồn tại"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO galleries (id) VALUES (?)', (gallery_id,))
            conn.commit()
        finally:
            conn.close()

    def update_gallery_metadata(self, gallery_id: str, metadata: dict, downloaded: bool, torrent_path: str):
        """Cập nhật metadata và trạng thái download"""
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

        # Parse tags back from JSON string
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
            # Chờ một element đặc trưng của nhentai xuất hiện
            await page.wait_for_selector('.container', state='visible', timeout=10000)
            return True
        except:
            # Nếu timeout, kiểm tra nội dung
            content = await page.content()
            if "Just a moment" in content or "Verify you are human" in content:
                print("Stuck at Cloudflare. Waiting longer...")
                await page.wait_for_timeout(5000)
                return True  # Hy vọng nó qua
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

    async def process_gallery(self, page, gallery_id: str) -> bool:
        """Truy cập trang gallery, lấy metadata VÀ tải torrent"""
        download_path = self.output_dir / f"{gallery_id}.torrent"

        # Kiểm tra DB xem đã tải chưa
        if self.db.is_downloaded(gallery_id) and download_path.exists():
            print(f"  [Skip] {gallery_id} already downloaded.")
            return True

        url = f"{self.base_url}/g/{gallery_id}/"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)

            # 1. Scrape Metadata
            metadata = {
                'title_english': '',
                'title_japanese': '',
                'tags': [],
                'pages': 0
            }

            # Get Titles
            title_h1 = await page.query_selector('h1.title')
            title_h2 = await page.query_selector('h2.title')
            if title_h1: metadata['title_english'] = await title_h1.inner_text()
            if title_h2: metadata['title_japanese'] = await title_h2.inner_text()

            # Get Tags
            tag_elements = await page.query_selector_all('.tag-container.field-name')
            for tag_el in tag_elements:
                text = await tag_el.inner_text()
                # Clean text lines
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if not lines: continue

                category = lines[0].replace(':', '').strip().lower()
                # Tags are usually in spans with class 'name'
                tag_names = await tag_el.query_selector_all('.name')
                for name_el in tag_names:
                    tag_val = await name_el.inner_text()
                    metadata['tags'].append(f"{category}:{tag_val}")

                # Get Page Count specifically
                if 'pages' in category:
                    try:
                        metadata['pages'] = int(lines[1])
                    except:
                        pass

            print(f"  Metadata: {metadata['title_english'][:50]}... ({metadata['pages']} pages)")

            # 2. Download Torrent
            downloaded = False
            try:
                async with page.expect_download(timeout=15000) as download_info:
                    # Find download button (sometimes hidden in dropdown or specific link)
                    download_btn = await page.query_selector('a[href*="/download"]')
                    if not download_btn:
                        # Fallback: force navigate
                        await page.evaluate(f'window.location.href = "{url}download"')
                    else:
                        await download_btn.click()

                    download = await download_info.value
                    await download.save_as(download_path)
                    downloaded = True
                    print(f"  Downloaded: {gallery_id}.torrent")
            except Exception as e:
                print(f"  Download failed for {gallery_id}: {str(e)[:50]}")

            # 3. Update DB
            self.db.update_gallery_metadata(gallery_id, metadata, downloaded, str(download_path))
            return downloaded

        except Exception as e:
            print(f"  Error processing {gallery_id}: {e}")
            return False

    async def run(self, max_galleries: int = None, start_page: int = 1, only_single_page: bool = False,
                  skip_scrape: bool = False):
        cookies = self.parse_netscape_cookies()
        if not cookies: return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=['--start-maximized',
                                                                    '--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(viewport=None)
            await context.add_cookies(cookies)
            page = await context.new_page()

            all_ids = []

            # Giai đoạn 1: Thu thập IDs (Scraping list)
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

                        # Lưu ngay vào DB để tránh mất mát
                        for gid in ids:
                            self.db.add_gallery_id(gid)

                        all_ids.extend(ids)

                        # Logic giới hạn số lượng (nếu cần dừng sớm việc lấy list)
                        if max_galleries and len(all_ids) >= max_galleries:
                            all_ids = all_ids[:max_galleries]
                            break

                        await asyncio.sleep(1)  # Nghỉ nhẹ

                except Exception as e:
                    print(f"Error scraping lists: {e}")
            else:
                print("\n=== Skipping Phase 1 (List Scraping) ===")
                # Nếu skip scrape, ta có thể load IDs từ DB chưa hoàn thành (TODO: logic mở rộng)
                # Hiện tại đơn giản là lấy IDs từ tham số hoặc scrape lại nếu cần.
                pass

            print(f"\nCollected {len(all_ids)} IDs in this session.")

            # Giai đoạn 2: Xử lý từng Gallery (Metadata + Download)
            print("\n=== Phase 2: Processing Galleries (Metadata & Download) ===")

            # Nếu đã có IDs từ phase 1, dùng nó. Nếu không (do skip), có thể query DB các item chưa download
            # Ở đây ta ưu tiên list vừa scrape được.

            count = 0
            for gid in all_ids:
                if max_galleries and count >= max_galleries: break

                print(f"[{count + 1}/{len(all_ids)}] Processing {gid}...")
                success = await self.process_gallery(page, gid)
                if success:
                    count += 1

                await asyncio.sleep(2)  # Rate limit protection

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
    parser.add_argument('--skip-list', action='store_true', help='Skip scraping list pages, use DB check (advanced)')

    args = parser.parse_args()

    downloader = NHentaiTorrentDownloader(args.cookies_file, args.output)
    await downloader.run(
        max_galleries=args.max,
        start_page=args.start_page,
        only_single_page=args.only_page,
        skip_scrape=args.skip_list
    )


if __name__ == "__main__":
    asyncio.run(main())