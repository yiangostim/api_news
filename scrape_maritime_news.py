#!/usr/bin/env python3
"""
Maritime News Scraper for GitHub Actions
Focuses on dry bulk shipping and general maritime news for sentiment analysis
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MaritimeNewsScraper:
    def __init__(self):
        self.api_key = os.getenv('NEWS_API_KEY')
        if not self.api_key:
            raise ValueError("NEWS_API_KEY environment variable not set")
        
        self.base_url = "https://newsapi.org/v2"
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Comprehensive maritime and dry bulk keywords
        self.keywords = [
            "dry bulk shipping",
            "bulk carrier",
            "capesize",
            "panamax",
            "handymax",
            "handysize",
            "baltic dry index",
            "iron ore",
            "steel production",
            "coal demand",
            "grain exports",
            "soybean exports",
            "wheat exports",
            "fertilizer exports",
            "bauxite",
            "nickel ore",
            "copper concentrate",
            "shipping rates",
            "freight rates",
            "charter rates",
            "shipbroking",
            "maritime transport",
            "cargo shipping",
            "vessel charter",
            "shipping market",
            "Suez Canal",
            "Panama Canal",
            "Black Sea shipping",
            "Red Sea shipping",
            "Strait of Hormuz",
            "shipping sanctions",
            "export bans",
            "import tariffs",
            "trade restrictions",
            "newbuilding orders",
            "ship demolition",
            "vessel scrapping",
            "secondhand bulk carrier sales",
            "lay-up vessels",
            "ship congestion",
            "port congestion",
            "berthing delays",
            "canal delays",
            "bunker prices",
            "VLSFO",
            "IFO380",
            "LNG bunkering",
            "IMO regulations",
            "EU ETS shipping",
            "CII compliance",
            "EEXI",
            "shipping emissions",
            "carbon levy",
            "piracy",
            "houthi attacks",
            "gulf of aden",
            "malacca strait security",
            "port strikes",
            "dockworkers strike",
            "shipping disruption",
            "port closures",
            "extreme weather",
            "hurricanes",
            "cyclones",
            "typhoons",
            "El Niño shipping",
            "shipping IPO",
            "shipping finance",
            "ship mortgage",
            "private equity shipping",
            "charter party disputes",
            "freight derivatives",
            "FFAs",
            "Baltic Exchange",
            "Baltic Forward Assessments"
        ]
        
        # Relevant sources for maritime news
        self.sources = [
            "reuters",
            "bloomberg", 
            "financial-times",
            "the-wall-street-journal"
        ]

    def create_article_id(self, article):
        """Create unique ID for article based on title and URL"""
        content = f"{article.get('title', '')}{article.get('url', '')}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def process_article(self, article, keyword):
        """Process article to match RSS feed structure"""
        processed = {
            'article_id': self.create_article_id(article),
            'title': article.get('title', ''),
            'link': article.get('url', ''),
            'creator': article.get('author', ''),
            'pubdate': article.get('publishedAt', ''),
            'category': keyword,  # Use search keyword as category
            'description': article.get('description', ''),
            'source': article.get('source', {}).get('name', ''),
            'scrape_timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        return processed
        """Fetch news articles from NewsAPI"""
        """Fetch news articles from NewsAPI"""
        params = {
            'q': query,
            'from': from_date,
            'to': to_date,
            'sortBy': 'publishedAt',
            'pageSize': page_size,
            'apiKey': self.api_key,
            'language': 'en'
        }
        
        try:
            'q': query,
            'from': from_date,
            'to': to_date,
            'sortBy': 'publishedAt',
            'pageSize': page_size,
            'apiKey': self.api_key,
            'language': 'en'
        }
        
    def fetch_news(self, query, from_date, to_date, page_size=100):
            response = requests.get(f"{self.base_url}/everything", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching news for query '{query}': {e}")
            return None

    def scrape_all_keywords(self, days_back=7):
        """Scrape news for all maritime keywords"""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        from_date = start_date.strftime('%Y-%m-%d')
        to_date = end_date.strftime('%Y-%m-%d')
        
        all_articles = []
        seen_ids = set()
        
        logger.info(f"Scraping maritime news from {from_date} to {to_date} (UTC)")
        
        for keyword in self.keywords:
            logger.info(f"Fetching news for: {keyword}")
            
            # Try general search first
            news_data = self.fetch_news(keyword, from_date, to_date)
            
            if news_data and news_data.get('articles'):
                articles = news_data['articles']
                for article in articles:
                    processed = self.process_article(article, keyword)
                    article_id = processed['article_id']
                    
                    # Only add if not seen before
                    if article_id not in seen_ids:
                        seen_ids.add(article_id)
                        all_articles.append(processed)
                
                logger.info(f"Found {len([a for a in articles if self.create_article_id(a) not in seen_ids])} unique articles for '{keyword}'")
            
            # Also try with specific sources
            for source in self.sources:
                source_query = f"{keyword} source:{source}"
                news_data = self.fetch_news(source_query, from_date, to_date, page_size=50)
                
                if news_data and news_data.get('articles'):
                    articles = news_data['articles']
                    for article in articles:
                        processed = self.process_article(article, f"{keyword} (source: {source})")
                        article_id = processed['article_id']
                        
                        # Only add if not seen before
                        if article_id not in seen_ids:
                            seen_ids.add(article_id)
                            all_articles.append(processed)
        
        logger.info(f"Total unique articles found: {len(all_articles)}")
        return all_articles

    def save_data(self, articles):
        """Save scraped data to files"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Save as JSON
        json_file = self.data_dir / f"maritime_news_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        
        # Save as CSV for easier analysis
        df = pd.DataFrame(articles)
        csv_file = self.data_dir / f"maritime_news_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        # Save latest as well (for consistent filename)
        latest_json = self.data_dir / "maritime_news_latest.json"
        latest_csv = self.data_dir / "maritime_news_latest.csv"
        
        with open(latest_json, 'w', encoding='utf-8') as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        
        df.to_csv(latest_csv, index=False, encoding='utf-8')
        
        logger.info(f"Saved {len(articles)} articles to {json_file} and {csv_file}")
        
        # Print summary statistics
        self.print_summary(articles)

    def print_summary(self, articles):
        """Print summary of scraped data"""
        logger.info("=== SCRAPING SUMMARY ===")
        logger.info(f"Total articles: {len(articles)}")
        
        if len(articles) > 0:
            df = pd.DataFrame(articles)
            logger.info(f"Date range: {df['pubdate'].min()} to {df['pubdate'].max()}")
            logger.info(f"Top sources: {df['source'].value_counts().head().to_dict()}")
            logger.info(f"Top categories: {df['category'].value_counts().head().to_dict()}")
            
            # Show UTC scrape timestamp
            logger.info(f"Scraped at: {articles[0]['scrape_timestamp']} (UTC)")

def main():
    """Main execution function"""
    try:
        scraper = MaritimeNewsScraper()
        
        # Scrape news from the last 7 days
        articles = scraper.scrape_all_keywords(days_back=7)
        
        # Save the data
        scraper.save_data(articles)
        
        logger.info("Maritime news scraping completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
