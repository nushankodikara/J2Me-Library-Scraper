import requests
from bs4 import BeautifulSoup
import time
import sqlite3
from urllib.parse import urljoin
from datetime import datetime
import os
import re
import argparse

class PhonekyGamesScraper:
    def __init__(self, download=False):
        self.base_url = 'https://phoneky.com/games/'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.db_name = 'phoneky_games.db'
        self.should_download = download
        
        if self.should_download:
            self.download_folder = 'JARs'
            os.makedirs(self.download_folder, exist_ok=True)
            
        self.setup_database()

    def setup_database(self):
        """Create the database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                image_url TEXT,
                category TEXT,
                size TEXT,
                screen_size TEXT,
                game_file_url TEXT,
                local_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_number INTEGER,
                status TEXT,
                message TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def get_game_details(self, game_url):
        """Get additional details from game's page"""
        try:
            # Extract game ID from URL
            game_id = game_url.split('id=')[1]  # Gets j4j42203 from the URL
            
            # Construct download URL
            game_file_url = f"https://phoneky.com/games/?p=download-item&id={game_id}&tt=181"
            
            # Get screen size from game page
            response = requests.get(game_url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Get screen size
            screen_size = None
            meta_div = soup.select_one('.prd-meta')
            if meta_div:
                dt_elements = meta_div.find_all('dt')
                for dt in dt_elements:
                    if 'Screen:' in dt.text:
                        screen_dd = dt.find_next('dd')
                        if screen_dd:
                            screen_size = screen_dd.text.strip()
                        break
            
            # If we couldn't find the screen size, use a default value
            if not screen_size:
                screen_size = "unknown"
                
            print(f"Found details for {game_url}: Screen={screen_size}, Download URL={game_file_url}")
            
            return screen_size, game_file_url
            
        except Exception as e:
            print(f"Error getting details for {game_url}: {str(e)}")
            return "unknown", None

    def download_game(self, game_file_url, title, screen_size):
        """Download game file and save to JARs folder"""
        try:
            response = requests.get(game_file_url, headers=self.headers)
            
            # Clean title for filename
            clean_title = re.sub(r'[^\w\s-]', '', title)
            clean_title = re.sub(r'\s+', '-', clean_title)
            
            # Create filename
            filename = f"{clean_title}-{screen_size}.jar"
            filepath = os.path.join(self.download_folder, filename)
            
            # Save file
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            return filename
        except Exception as e:
            print(f"Error downloading game {title}: {str(e)}")
            return None

    def parse_games_list(self, soup):
        """Parse games from a single page"""
        games = []
        game_items = soup.select('ul.prd-details li')
        
        for item in game_items:
            try:
                title = item.select_one('h3.title').text.strip()
                url = item.select_one('a[title]')['href']
                image = item.select_one('img.photoThumb')['src']
                category = item.select_one('.id-num a').text.strip()
                size = item.select_one('.fsize').text.strip()
                
                # Get additional details
                print(f"\nGetting details for: {title}")
                screen_size, game_file_url = self.get_game_details(urljoin(self.base_url, url))
                
                # Download game only if should_download is True
                local_name = None
                if self.should_download and game_file_url:
                    local_name = self.download_game(game_file_url, title, screen_size)
                
                game = {
                    'title': title,
                    'url': urljoin(self.base_url, url),
                    'image_url': image,
                    'category': category,
                    'size': size,
                    'screen_size': screen_size,
                    'game_file_url': game_file_url,
                    'local_name': local_name
                }
                
                games.append(game)
                time.sleep(1)  # Delay between requests
                
            except Exception as e:
                print(f"Error parsing game item: {str(e)}")
                continue
        
        return games

    def save_games_to_db(self, games, page_number):
        """Save games to database"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        try:
            for game in games:
                cursor.execute('''
                    INSERT OR IGNORE INTO games 
                    (title, url, image_url, category, size, screen_size, game_file_url, local_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    game['title'], game['url'], game['image_url'], 
                    game['category'], game['size'], game['screen_size'],
                    game['game_file_url'], game['local_name']
                ))
            
            cursor.execute('''
                INSERT INTO scraping_log (page_number, status, message)
                VALUES (?, ?, ?)
            ''', (
                page_number, 'success', f'Scraped {len(games)} games'
            ))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            cursor.execute('''
                INSERT INTO scraping_log (page_number, status, message)
                VALUES (?, ?, ?)
            ''', (
                page_number, 'error', str(e)
            ))
            conn.commit()
            raise e
        
        finally:
            conn.close()

    def scrape(self, start_page=None, end_page=528):
        """Main scraping method"""
        if start_page is None:
            start_page = self.get_last_scraped_page() + 1
        
        print(f"Starting scrape from page {start_page} to {end_page}")
        
        for page in range(start_page, end_page + 1):
            try:
                print(f"Scraping page {page}...")
                
                response = requests.get(
                    f"{self.base_url}?page={page}",
                    headers=self.headers
                )
                soup = BeautifulSoup(response.content, 'html.parser')
                
                games = self.parse_games_list(soup)
                self.save_games_to_db(games, page)
                
                print(f"Successfully scraped page {page} - Found {len(games)} games")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"Error on page {page}: {str(e)}")
                continue

    def get_last_scraped_page(self):
        """Get the last successfully scraped page number"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT MAX(page_number) 
                FROM scraping_log 
                WHERE status = 'success'
            ''')
            
            result = cursor.fetchone()[0]
            return result if result is not None else 0
            
        except Exception as e:
            print(f"Error getting last scraped page: {str(e)}")
            return 0
            
        finally:
            conn.close()



def main():
    # Add argument parsing
    parser = argparse.ArgumentParser(description='Scrape Phoneky Games')
    parser.add_argument('--download', action='store_true', help='Download games while scraping')
    parser.add_argument('--start-page', type=int, help='Starting page number')
    parser.add_argument('--end-page', type=int, help='Ending page number')
    args = parser.parse_args()

    # Initialize scraper with download option
    scraper = PhonekyGamesScraper(download=args.download)
    
    # Start scraping
    scraper.scrape(
        start_page=args.start_page,
        end_page=args.end_page
    )

if __name__ == "__main__":
    main()
