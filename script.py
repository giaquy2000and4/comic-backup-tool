import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright
from typing import List, Dict
import argparse


class NHentaiTorrentDownloader:
    def __init__(self, cookies_file: str, output_dir: str = "./torrents"):
        self.cookies_file = cookies_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.base_url = "https://nhentai.net"

    def parse_netscape_cookies(self) -> List[Dict]:
        """Parse Netscape format cookies file"""
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
                            'name': parts[5],
                            'value': parts[6],
                            'domain': parts[0],
                            'path': parts[2],
                            'secure': parts[3] == 'TRUE',
                            'httpOnly': False,
                            'sameSite': 'Lax'
                        }
                        if parts[4] and parts[4] != '0':
                            cookie['expires'] = int(parts[4])
                        cookies.append(cookie)
            print(f"✓ Loaded {len(cookies)} cookies from {self.cookies_file}")
            return cookies
        except Exception as e:
            print(f"✗ Error parsing cookies: {e}")
            return []

    async def wait_for_cloudflare(self, page):
        """Wait for Cloudflare challenge to complete"""
        print("⏳ Waiting for Cloudflare challenge...")

        max_wait = 30  # seconds
        start_time = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start_time) < max_wait:
            try:
                # Check if we're past Cloudflare
                content = await page.content()

                if "Just a moment" in content or "Verify you are human" in content:
                    await asyncio.sleep(1)
                    continue

                # Check if we're on the actual page
                if "favorites" in page.url.lower() or "gallery" in content.lower():
                    print("✓ Cloudflare challenge passed!")
                    return True

                await asyncio.sleep(1)
            except:
                await asyncio.sleep(1)

        print("⚠️  Cloudflare challenge timeout")
        return False

    async def get_favourites_pages(self, page) -> int:
        """Get total number of pages in favourites"""
        await page.goto(f"{self.base_url}/favorites/", wait_until="domcontentloaded", timeout=60000)

        # Wait for Cloudflare
        if not await self.wait_for_cloudflare(page):
            raise Exception("Could not bypass Cloudflare protection")

        await page.wait_for_timeout(3000)

        # Check if logged in
        if "login" in page.url:
            raise Exception("Not logged in! Please check your cookies.")

        # Get total pages
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
        """Extract gallery IDs from a favourites page"""
        url = f"{self.base_url}/favorites/" if page_num == 1 else f"{self.base_url}/favorites/?page={page_num}"

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for Cloudflare if needed
        await self.wait_for_cloudflare(page)

        # Wait for content
        await page.wait_for_timeout(3000)

        gallery_ids = []

        # Strategy: Find all links with /g/ pattern
        all_links = await page.query_selector_all('a[href*="/g/"]')

        for link in all_links:
            try:
                href = await link.get_attribute('href')
                if href and '/g/' in href and '/download' not in href:
                    match = re.search(r'/g/(\d+)/?', href)
                    if match:
                        gallery_id = match.group(1)
                        if gallery_id not in gallery_ids:
                            gallery_ids.append(gallery_id)
            except:
                continue

        return gallery_ids

    async def download_torrent(self, page, gallery_id: str) -> bool:
        """Download torrent file for a specific gallery"""
        try:
            download_path = self.output_dir / f"{gallery_id}.torrent"

            # Skip if already exists
            if download_path.exists():
                print(f"  ⊘ Skipped: {gallery_id}.torrent (already exists)")
                return True

            # Go to gallery page first
            gallery_url = f"{self.base_url}/g/{gallery_id}/"
            await page.goto(gallery_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1000)

            # Find and click download button with event listener ready
            async with page.expect_download(timeout=30000) as download_info:
                # Try to find download link/button
                download_link = await page.query_selector('a[href*="/download"]')
                if not download_link:
                    # Alternative: construct download URL and use it
                    torrent_url = f"{self.base_url}/g/{gallery_id}/download"
                    await page.evaluate(f'window.location.href = "{torrent_url}"')
                else:
                    await download_link.click()

                download = await download_info.value
                await download.save_as(download_path)

            print(f"  ✓ Downloaded: {gallery_id}.torrent")
            return True

        except asyncio.TimeoutError:
            print(f"  ✗ Failed {gallery_id}: Download timeout")
            return False
        except Exception as e:
            error_msg = str(e).split('\n')[0][:80]
            print(f"  ✗ Failed {gallery_id}: {error_msg}")
            return False

    async def run(self, max_galleries: int = None, start_page: int = 1, only_single_page: bool = False):
        """Main execution function"""
        cookies = self.parse_netscape_cookies()
        if not cookies:
            print("No valid cookies found!")
            return

        async with async_playwright() as p:
            # Launch browser with more realistic settings
            browser = await p.chromium.launch(
                headless=False,  # Set to False to see what's happening
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )

            # Add cookies
            await context.add_cookies(cookies)

            page = await context.new_page()

            # Hide automation
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)

            try:
                print("\n🔍 Fetching favourites...")
                total_pages = await self.get_favourites_pages(page)
                print(f"✓ Found {total_pages} page(s) of favourites")

                if start_page > total_pages:
                    print(f"❌ Start page {start_page} is greater than total pages {total_pages}")
                    return

                all_gallery_ids = []

                # --- START UPDATE LOGIC ---
                if only_single_page:
                    print(f"🎯 Mode: Only downloading page {start_page}")
                    end_page = start_page
                else:
                    # Collect gallery IDs normally
                    end_page = min(total_pages, start_page + 50) if max_galleries else total_pages

                for page_num in range(start_page, end_page + 1):
                    print(f"\n📄 Scraping page {page_num}...")
                    ids = await self.get_gallery_ids_from_page(page, page_num)
                    all_gallery_ids.extend(ids)
                    print(f"  Found {len(ids)} galleries")

                    if len(ids) == 0 and page_num == start_page:
                        content = await page.content()
                        debug_file = self.output_dir / f"debug_page{page_num}.html"
                        with open(debug_file, 'w', encoding='utf-8') as f:
                            f.write(content)
                        print(f"  ⚠️  Saved debug HTML to {debug_file}")

                    # Optimization: Break scraping loop if we already have enough items
                    if max_galleries and len(all_gallery_ids) >= max_galleries:
                        break
                # --- END UPDATE LOGIC ---

                print(f"\n📊 Total galleries found: {len(all_gallery_ids)}")

                if len(all_gallery_ids) == 0:
                    print("❌ No galleries found. Please check:")
                    print("   1. Cookies are valid and up-to-date")
                    print("   2. You have favourites in your account")
                    print("   3. Check debug HTML file for errors")
                    return

                # Limit if specified
                if max_galleries:
                    all_gallery_ids = all_gallery_ids[:max_galleries]
                    print(f"⚠️  Limiting to first {max_galleries} galleries")

                # Download torrents
                print(f"\n⬇️  Downloading torrents to: {self.output_dir}/")
                success_count = 0

                for i, gallery_id in enumerate(all_gallery_ids, 1):
                    print(f"\n[{i}/{len(all_gallery_ids)}] Gallery {gallery_id}...")
                    if await self.download_torrent(page, gallery_id):
                        success_count += 1

                    # Delay to avoid rate limiting
                    await asyncio.sleep(2)

                print(f"\n{'=' * 50}")
                print(f"✅ Download complete!")
                print(f"📥 Successfully downloaded: {success_count}/{len(all_gallery_ids)} torrents")
                print(f"📂 Files saved to: {self.output_dir.absolute()}")

            except Exception as e:
                print(f"\n❌ Error: {e}")
                import traceback
                traceback.print_exc()
            finally:
                await browser.close()


async def main():
    parser = argparse.ArgumentParser(
        description='Download torrent files from nhentai.net favourites'
    )
    parser.add_argument(
        'cookies_file',
        help='Path to Netscape format cookies file'
    )
    parser.add_argument(
        '-o', '--output',
        default='./torrents',
        help='Output directory for torrent files (default: ./torrents)'
    )
    parser.add_argument(
        '-m', '--max',
        type=int,
        help='Maximum number of torrents to download'
    )
    parser.add_argument(
        '-s', '--start-page',
        type=int,
        default=1,
        help='Start from page number (default: 1)'
    )
    # New argument added here
    parser.add_argument(
        '--only-page',
        action='store_true',
        help='Only download content from the specific start page provided in -s'
    )

    args = parser.parse_args()

    downloader = NHentaiTorrentDownloader(args.cookies_file, args.output)

    # Pass the new argument to the run function
    await downloader.run(
        max_galleries=args.max,
        start_page=args.start_page,
        only_single_page=args.only_page
    )


if __name__ == "__main__":
    asyncio.run(main())