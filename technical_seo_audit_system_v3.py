"""
Automated Technical SEO Audit System
Comprehensive technical SEO monitoring using Google APIs and web crawling
"""

import requests
import json
import time
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests.utils
import pandas as pd
import logging
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TechnicalSEOIssue:
    url: str
    issue_type: str
    severity: str  # 'Critical', 'High', 'Medium', 'Low'
    category: str  # 'Crawlability', 'Performance', 'Mobile', 'Schema', 'Content'
    description: str
    recommendation: str
    date_detected: datetime
    status: str  # 'New', 'Existing', 'Fixed'
    impact_score: int  # 1-100

@dataclass
class GSCMetrics:
    url: str
    date: str
    coverage_status: str  # 'Valid', 'Error', 'Warning', 'Excluded'
    error_type: Optional[str]
    mobile_usability_issues: List[str]
    page_experience_signals: Dict
    crawl_stats: Dict

@dataclass 
class CrawlMetrics:
    url: str
    status_code: int
    response_time: float
    title: str
    meta_description: str
    h1_tags: List[str]
    canonical_url: Optional[str]
    robots_meta: str
    internal_links: int
    external_links: int
    images_without_alt: int
    page_size: int

class GoogleSearchConsoleAPI:
    def __init__(self, credentials_path: str):
        """Initialize Google Search Console API client with OAuth2 credentials
        
        Args:
            credentials_path: Path to the service account JSON file
        """
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        )
        
        # Build the service using googleapiclient
        from googleapiclient.discovery import build
        self.service = build('searchconsole', 'v1', credentials=self.credentials)
    
    def get_sites(self) -> List[str]:
        """Get all verified sites in GSC using OAuth2"""
        try:
            sites = self.service.sites().list().execute()
            return [site['siteUrl'] for site in sites.get('siteEntry', [])]
        except Exception as e:
            logger.error(f"Error fetching sites: {e}")
            return []
    
    def get_coverage_issues(self, site_url: str, days_back: int = 30, top_pages_limit: int = 10) -> List[GSCMetrics]:
        """Get search analytics data from GSC using OAuth2 with top pages by impressions"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        try:
            request_body = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': ['page'],
                'rowLimit': 1000
            }
            
            response = self.service.searchanalytics().query(
                siteUrl=site_url,
                body=request_body
            ).execute()
            
            # Process search analytics data
            coverage_metrics = []
            for row in response.get('rows', []):
                page_url = row.get('keys', [''])[0]
                clicks = row.get('clicks', 0)
                impressions = row.get('impressions', 0)
                ctr = row.get('ctr', clicks/impressions if impressions > 0 else 0)
                position = row.get('position', 0)
                
                coverage_metrics.append(GSCMetrics(
                    url=page_url,
                    date=end_date.strftime('%Y-%m-%d'),
                    coverage_status='Valid',  # Assume valid if showing in search analytics
                    error_type=None,
                    mobile_usability_issues=[],
                    page_experience_signals={
                        'clicks': clicks,
                        'impressions': impressions,
                        'ctr': ctr,
                        'position': position,
                        'domain': site_url
                    },
                    crawl_stats={}
                ))
            
            # Sort by impressions and return top pages
            coverage_metrics.sort(key=lambda x: x.page_experience_signals.get('impressions', 0), reverse=True)
            logger.info(f"Found {len(coverage_metrics)} pages for {site_url}, returning top {top_pages_limit}")
            
            return coverage_metrics[:top_pages_limit]
            
        except Exception as e:
            logger.error(f"Error fetching search analytics data for {site_url}: {e}")
            return []
    
    def get_multi_domain_coverage(self, site_urls: List[str], days_back: int = 30, top_pages_per_domain: int = 10) -> Dict[str, List[GSCMetrics]]:
        """Get coverage issues for multiple GSC domain properties"""
        all_coverage_data = {}
        
        for site_url in site_urls:
            logger.info(f"Fetching GSC data for {site_url}...")
            coverage_data = self.get_coverage_issues(site_url, days_back, top_pages_per_domain)
            all_coverage_data[site_url] = coverage_data
            
        return all_coverage_data
    
    def get_mobile_usability_issues(self, site_url: str) -> List[Dict]:
        """Get mobile usability issues using OAuth2"""
        try:
            # Note: Mobile usability API might not be available in all GSC API versions
            # This is a placeholder implementation
            logger.info(f"Mobile usability check for {site_url} - using search analytics data")
            return []  # Return empty for now, as mobile usability API has limited access
        except Exception as e:
            logger.error(f"Error fetching mobile usability issues: {e}")
            return []

class WebCrawler:
    def __init__(self, max_workers: int = 5, timeout: int = 30):
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SEO-Audit-Bot/1.0 (+https://yoursite.com/bot)'
        })
    
    def crawl_url(self, url: str) -> Optional[CrawlMetrics]:
        """Crawl a single URL and extract technical SEO data"""
        try:
            start_time = time.time()
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            response_time = time.time() - start_time
            
            # Parse HTML content
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract SEO elements
            title = soup.find('title').get_text().strip() if soup.find('title') else ''
            
            meta_desc = ''
            meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_desc_tag:
                meta_desc = meta_desc_tag.get('content', '').strip()
            
            h1_tags = [h1.get_text().strip() for h1 in soup.find_all('h1')]
            
            canonical_url = None
            canonical_tag = soup.find('link', attrs={'rel': 'canonical'})
            if canonical_tag:
                canonical_url = canonical_tag.get('href')
            
            robots_meta = ''
            robots_tag = soup.find('meta', attrs={'name': 'robots'})
            if robots_tag:
                robots_meta = robots_tag.get('content', '')
            
            # Count links
            internal_links = len([a for a in soup.find_all('a', href=True) 
                                if self._is_internal_link(a['href'], url)])
            external_links = len([a for a in soup.find_all('a', href=True) 
                                if not self._is_internal_link(a['href'], url)])
            
            # Count images without alt text
            images_without_alt = len([img for img in soup.find_all('img') 
                                    if not img.get('alt', '').strip()])
            
            return CrawlMetrics(
                url=url,
                status_code=response.status_code,
                response_time=response_time,
                title=title,
                meta_description=meta_desc,
                h1_tags=h1_tags,
                canonical_url=canonical_url,
                robots_meta=robots_meta,
                internal_links=internal_links,
                external_links=external_links,
                images_without_alt=images_without_alt,
                page_size=len(response.content)
            )
            
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return None
    
    def _is_internal_link(self, href: str, base_url: str) -> bool:
        """Check if a link is internal"""
        if href.startswith('http'):
            return urlparse(href).netloc == urlparse(base_url).netloc
        return True  # Relative links are internal
    
    def crawl_sitemap(self, sitemap_url: str) -> List[str]:
        """Extract URLs from XML sitemap"""
        try:
            response = self.session.get(sitemap_url, timeout=self.timeout)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            urls = []
            
            # Handle different sitemap formats
            for url_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                loc = url_elem.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                if loc is not None:
                    urls.append(loc.text)
            
            return urls
            
        except Exception as e:
            logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
            return []

class PageSpeedCollector:
    """Built-in PageSpeed Insights collector"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    
    def get_page_speed_data(self, url: str, strategy: str = 'mobile') -> Dict:
        """Get PageSpeed Insights data for a URL"""
        params = {
            'url': url,
            'key': self.api_key,
            'strategy': strategy,
            'category': ['performance'],
            'locale': 'en'
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Extract key metrics
            lighthouse_result = data.get('lighthouseResult', {})
            audits = lighthouse_result.get('audits', {})
            categories = lighthouse_result.get('categories', {})
            
            # Performance score
            performance_score = None
            if 'performance' in categories:
                performance_score = int(categories['performance'].get('score', 0) * 100)
            
            # Core Web Vitals
            lcp = audits.get('largest-contentful-paint', {}).get('numericValue', 0) / 1000
            fid = audits.get('max-potential-fid', {}).get('numericValue', 0) / 1000
            cls = audits.get('cumulative-layout-shift', {}).get('numericValue', 0)
            
            return {
                'url': url,
                'strategy': strategy,
                'performance_score': performance_score,
                'lcp': round(lcp, 1),
                'fid': round(fid, 3),
                'cls': round(cls, 3),
                'opportunities': self._extract_opportunities(audits)
            }
            
        except Exception as e:
            logger.error(f"Error fetching PageSpeed data for {url}: {e}")
            return {'url': url, 'error': str(e)}
    
    def _extract_opportunities(self, audits: Dict) -> List[Dict]:
        """Extract performance optimization opportunities"""
        opportunity_keys = [
            'render-blocking-resources',
            'unused-css-rules',
            'unused-javascript',
            'modern-image-formats',
            'offscreen-images'
        ]
        
        opportunities = []
        for key in opportunity_keys:
            audit = audits.get(key, {})
            if audit.get('score', 1) < 1:  # Has issues
                opportunities.append({
                    'audit': key,
                    'title': audit.get('title', ''),
                    'description': audit.get('description', ''),
                    'savings_ms': audit.get('details', {}).get('overallSavingsMs', 0)
                })
        
        return opportunities

class SchemaValidator:
    def __init__(self):
        self.schema_types = [
            'Organization', 'WebSite', 'WebPage', 'BreadcrumbList',
            'Product', 'Review', 'Person', 'Article', 'LocalBusiness'
        ]
    
    def validate_structured_data(self, url: str) -> Dict:
        """Validate structured data using Google's Structured Data Testing Tool"""
        api_url = "https://search.google.com/test/rich-results"
        
        # Note: Google's Rich Results Test doesn't have a direct API
        # This would need to use web scraping or alternative validation
        
        # Alternative: Use schema.org validator
        return self._validate_with_schemaorg(url)
    
    def _validate_with_schemaorg(self, url: str) -> Dict:
        """Validate using schema.org principles"""
        try:
            response = requests.get(url, timeout=30)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find JSON-LD structured data
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            schema_data = []
            
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    schema_data.append(data)
                except json.JSONDecodeError:
                    continue
            
            # Find microdata
            microdata_items = soup.find_all(attrs={'itemscope': True})
            
            return {
                'url': url,
                'json_ld_count': len(json_ld_scripts),
                'microdata_count': len(microdata_items),
                'schema_types_found': self._extract_schema_types(schema_data),
                'validation_errors': [],  # Would need actual validation logic
                'recommendations': self._generate_schema_recommendations(schema_data)
            }
            
        except Exception as e:
            logger.error(f"Error validating schema for {url}: {e}")
            return {'url': url, 'error': str(e)}
    
    def _extract_schema_types(self, schema_data: List[Dict]) -> List[str]:
        """Extract schema types from JSON-LD data"""
        types = set()
        for data in schema_data:
            if isinstance(data, dict) and '@type' in data:
                types.add(data['@type'])
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and '@type' in item:
                        types.add(item['@type'])
        return list(types)
    
    def _generate_schema_recommendations(self, schema_data: List[Dict]) -> List[str]:
        """Generate schema markup recommendations"""
        recommendations = []
        
        if not schema_data:
            recommendations.append("Add structured data markup to improve search visibility")
        
        # Check for common missing schemas
        found_types = self._extract_schema_types(schema_data)
        
        if 'Organization' not in found_types:
            recommendations.append("Add Organization schema for brand information")
        
        if 'WebSite' not in found_types:
            recommendations.append("Add WebSite schema with sitelinks searchbox")
        
        return recommendations

class StrategicSEOScorer:
    """Strategic SEO scoring based on Google ranking factors and business impact"""
    
    def __init__(self):
        # Google confirmed ranking factors with impact weights
        self.issue_severity = {
            # Critical (site-breaking or major ranking impact)
            'http_5xx_error': 100,
            'HTTP Error': 90,  # Match our issue types
            'not_indexable': 100,
            'core_web_vitals_poor': 95,
            'Blocked by Robots Meta': 100,  # Critical - prevents indexing
            'Missing Title Tag': 95,
            'not_mobile_friendly': 90,
            'no_https': 85,
            
            # High (significant SEO impact)
            'Thin Content': 85,  # Major content quality issue
            'Duplicate Title Tags': 80,  # Duplicate content issues
            'Generic Title Tag': 75,  # Poor user experience and CTR
            'Missing Canonical Tag': 75,  # Duplicate content prevention
            'Invalid Canonical URL': 75,
            'Missing Product Schema': 70,  # E-commerce specific
            'Slow Response Time': 80,
            'broken_internal_links': 75,
            'Missing H1 Tag': 65,
            
            # Medium (moderate impact)
            'Duplicate Meta Descriptions': 50,  # Less critical than titles
            'Short Title Tag': 55,
            'Long Title Tag': 50,
            'Missing Structured Data': 50,
            'Insufficient Internal Links': 45,
            'Missing Organization Schema': 45,
            'Missing Meta Description': 45,  # CTR impact, not ranking
            'Large Page Size': 55,
            'Images Without Alt Text': 40,  # Accessibility > SEO
            
            # Low (minor optimization)
            'Short Meta Description': 25,
            'Long Meta Description': 25,
            'Excessive External Links': 30,
            'Multiple H1 Tags': 30,
            'long_title': 25,
            
            # GSC Performance Issues (High business impact)
            'High Impressions Zero Clicks': 95,  # Critical - visible but not clickable
            'High Traffic Page with Technical Issues': 90,  # Critical - technical issues on important pages
            'High Impressions Low CTR': 70,  # High - opportunity for traffic increase
            'High Impressions Poor Position': 60,  # Medium - ranking opportunity
            'High Value Page Opportunity': 50,  # Medium - optimization opportunity
        }
        
        # Page importance weights for e-commerce
        self.page_weights = {
            'homepage': 5.0,
            'product': 4.5,
            'category': 4.0,
            'checkout': 4.8,
            'pricing': 4.5,
            'contact': 3.0,
            'about': 2.5,
            'blog': 2.0,
            'other': 2.0
        }
    
    def classify_page_type(self, url: str) -> str:
        """Classify page type for business importance weighting"""
        url_lower = url.lower()
        
        if any(pattern in url_lower for pattern in ['/', '/home', '/index']) and url_lower.count('/') <= 3:
            return 'homepage'
        elif any(pattern in url_lower for pattern in ['/product', '/p/', 'photo-blankets', 'photo-books', 'photo-mugs']):
            return 'product'
        elif any(pattern in url_lower for pattern in ['/category', '/c/', '/canvas-prints', '/photo-calendars']):
            return 'category' 
        elif any(pattern in url_lower for pattern in ['/checkout', '/cart', '/basket']):
            return 'checkout'
        elif any(pattern in url_lower for pattern in ['/about', '/company']):
            return 'about'
        elif any(pattern in url_lower for pattern in ['/contact', '/support']):
            return 'contact'
        elif any(pattern in url_lower for pattern in ['/blog', '/news']):
            return 'blog'
        else:
            return 'other'
    
    def get_strategic_impact_score(self, issue_type: str, url: str) -> int:
        """Get strategic impact score considering page importance"""
        base_impact = self.issue_severity.get(issue_type, 50)
        page_type = self.classify_page_type(url)
        page_weight = self.page_weights.get(page_type, 2.0)
        
        # Apply business importance multiplier
        strategic_impact = base_impact * (page_weight / 2.5)  # Normalize around 2.5 average
        
        return min(100, int(strategic_impact))

class TechnicalSEOAuditor:
    def __init__(self, config: Dict):
        """Initialize the technical SEO audit system"""
        self.config = config
        self.strategic_scorer = StrategicSEOScorer()
        
        # Initialize components
        if config.get('gsc_credentials_path'):
            self.gsc = GoogleSearchConsoleAPI(config['gsc_credentials_path'])
        else:
            self.gsc = None
            
        self.crawler = WebCrawler(
            max_workers=config.get('max_crawl_workers', 5),
            timeout=config.get('crawl_timeout', 30)
        )
        
        self.schema_validator = SchemaValidator()
        
        # Initialize PageSpeed Insights collector directly
        self.pagespeed_collector = PageSpeedCollector(config['pagespeed_api_key'])
        
        # BigQuery for storing audit results
        if config.get('bigquery'):
            if config['bigquery'].get('credentials_path'):
                credentials = service_account.Credentials.from_service_account_file(
                    config['bigquery']['credentials_path']
                )
                self.storage = bigquery.Client(
                    project=config['bigquery']['project_id'],
                    credentials=credentials
                )
            else:
                self.storage = bigquery.Client(
                    project=config['bigquery']['project_id']
                )
            self.table_ref = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.technical_seo_audit"
        else:
            self.storage = None
            self.table_ref = None
    
    def run_comprehensive_audit(self, site_urls: List[str], urls_to_audit: List[str]) -> Dict:
        """Run comprehensive technical SEO audit"""
        audit_start = datetime.now(timezone.utc)
        logger.info(f"Starting comprehensive technical SEO audit for {len(site_urls)} domains")
        
        audit_results = {
            'site_urls': site_urls,
            'audit_timestamp': audit_start.isoformat(),
            'total_urls_audited': len(urls_to_audit),
            'gsc_data': {},
            'crawl_data': [],
            'pagespeed_data': [],
            'schema_data': [],
            'issues': [],
            'summary': {}
        }
        
        # 1. Google Search Console Data (Multi-Domain)
        if self.gsc:
            logger.info(f"Fetching Google Search Console data for {len(site_urls)} domains...")
            audit_results['gsc_data'] = self.gsc.get_multi_domain_coverage(site_urls, days_back=30, top_pages_per_domain=10)
        
        # 2. Crawl Analysis
        logger.info(f"Crawling {len(urls_to_audit)} URLs...")
        with ThreadPoolExecutor(max_workers=self.crawler.max_workers) as executor:
            future_to_url = {
                executor.submit(self.crawler.crawl_url, url): url 
                for url in urls_to_audit
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    crawl_result = future.result()
                    if crawl_result:
                        audit_results['crawl_data'].append(asdict(crawl_result))
                except Exception as e:
                    logger.error(f"Error crawling {url}: {e}")
        
        # 3. PageSpeed Insights Analysis
        logger.info("Fetching PageSpeed Insights data...")
        for url in urls_to_audit[:5]:  # Limit for API quota
            for strategy in ['mobile', 'desktop']:
                pagespeed_result = self.pagespeed_collector.get_page_speed_data(url, strategy)
                audit_results['pagespeed_data'].append(pagespeed_result)
                time.sleep(1)  # Rate limiting
        
        # 4. Schema Validation
        logger.info("Validating structured data...")
        for url in urls_to_audit[:10]:  # Limit schema validation for performance
            schema_result = self.schema_validator.validate_structured_data(url)
            audit_results['schema_data'].append(schema_result)
        
        # 4. Issue Analysis
        audit_results['issues'] = self._analyze_issues(audit_results)
        
        # 5. Generate Summary
        audit_results['summary'] = self._generate_audit_summary(audit_results)
        
        # 6. Store Results
        self._store_audit_results(audit_results)
        
        audit_duration = datetime.now(timezone.utc) - audit_start
        logger.info(f"Audit completed in {audit_duration.total_seconds():.2f} seconds")
        
        return audit_results
    
    def _analyze_issues(self, audit_results: Dict) -> List[TechnicalSEOIssue]:
        """Analyze audit data to identify technical SEO issues"""
        issues = []
        current_time = datetime.now(timezone.utc)
        
        # Analyze crawl data for issues
        for crawl_data in audit_results['crawl_data']:
            url = crawl_data['url']
            
            # HTTP Status Issues
            if crawl_data['status_code'] >= 400:
                issue_type = 'HTTP Error'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Critical' if crawl_data['status_code'] >= 500 else 'High',
                    category='Crawlability',
                    description=f"HTTP {crawl_data['status_code']} error",
                    recommendation=f"Fix HTTP {crawl_data['status_code']} error to ensure page is accessible",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Performance Issues
            if crawl_data['response_time'] > 3.0:
                issue_type = 'Slow Response Time'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='High' if crawl_data['response_time'] > 5.0 else 'Medium',
                    category='Performance',
                    description=f"Response time: {crawl_data['response_time']:.2f}s",
                    recommendation="Optimize server response time to under 2 seconds",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Content Issues
            if not crawl_data['title']:
                issue_type = 'Missing Title Tag'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='High',
                    category='Content',
                    description="Page is missing title tag",
                    recommendation="Add descriptive title tag (50-60 characters)",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            if not crawl_data['meta_description']:
                issue_type = 'Missing Meta Description'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Medium',
                    category='Content',
                    description="Page is missing meta description",
                    recommendation="Add compelling meta description (150-160 characters)",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # H1 Issues
            if not crawl_data['h1_tags']:
                issue_type = 'Missing H1 Tag'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Medium',
                    category='Content',
                    description="Page is missing H1 tag",
                    recommendation="Add descriptive H1 tag for page hierarchy",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            elif len(crawl_data['h1_tags']) > 1:
                issue_type = 'Multiple H1 Tags'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Low',
                    category='Content',
                    description=f"Page has {len(crawl_data['h1_tags'])} H1 tags",
                    recommendation="Use only one H1 tag per page",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Image Issues
            if crawl_data['images_without_alt'] > 0:
                issue_type = 'Images Without Alt Text'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Medium',
                    category='Content',
                    description=f"{crawl_data['images_without_alt']} images without alt text",
                    recommendation="Add descriptive alt text to all images",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Large Page Size
            if crawl_data['page_size'] > 1024 * 1024:  # 1MB
                issue_type = 'Large Page Size'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Medium',
                    category='Performance',
                    description=f"Page size: {crawl_data['page_size'] / 1024 / 1024:.2f}MB",
                    recommendation="Optimize images and resources to reduce page size",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # CONTENT QUALITY ANALYSIS
            
            # Thin Content Detection
            content_length = len(crawl_data.get('title', '') + crawl_data.get('meta_description', '') + ' '.join(crawl_data.get('h1_tags', [])))
            if content_length < 200:  # Very basic content length check
                issue_type = 'Thin Content'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='High',
                    category='Content',
                    description=f"Insufficient content detected (estimated {content_length} characters)",
                    recommendation="Add substantial, valuable content with at least 300+ words",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Title Tag Quality Issues
            if crawl_data['title']:
                title_len = len(crawl_data['title'])
                
                # Title too short
                if title_len < 30:
                    issue_type = 'Short Title Tag'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='Medium',
                        category='Content',
                        description=f"Title tag too short ({title_len} characters)",
                        recommendation="Expand title to 50-60 characters with descriptive keywords",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
                
                # Title too long  
                elif title_len > 70:
                    issue_type = 'Long Title Tag'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='Medium',
                        category='Content',
                        description=f"Title tag too long ({title_len} characters)",
                        recommendation="Shorten title to 50-60 characters to avoid truncation",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
                
                # Generic/Non-descriptive titles
                generic_patterns = ['untitled', 'new page', 'home page', 'welcome', 'default', 'page']
                if any(pattern in crawl_data['title'].lower() for pattern in generic_patterns):
                    issue_type = 'Generic Title Tag'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='High',
                        category='Content',
                        description=f"Generic/non-descriptive title: '{crawl_data['title']}'",
                        recommendation="Create unique, descriptive title with target keywords",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
            
            # Meta Description Quality Issues
            if crawl_data['meta_description']:
                meta_len = len(crawl_data['meta_description'])
                
                # Meta description too short
                if meta_len < 120:
                    issue_type = 'Short Meta Description'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='Low',
                        category='Content',
                        description=f"Meta description too short ({meta_len} characters)",
                        recommendation="Expand meta description to 150-160 characters",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
                
                # Meta description too long
                elif meta_len > 170:
                    issue_type = 'Long Meta Description'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='Low',
                        category='Content',
                        description=f"Meta description too long ({meta_len} characters)",
                        recommendation="Shorten meta description to 150-160 characters",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
            
            # ADVANCED SEO FACTORS
            
            # Missing or Invalid Canonical URL
            if not crawl_data['canonical_url']:
                issue_type = 'Missing Canonical Tag'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='High',
                    category='Technical SEO',
                    description="Page missing canonical URL tag",
                    recommendation="Add rel='canonical' tag to prevent duplicate content issues",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            elif crawl_data['canonical_url'] != url and not crawl_data['canonical_url'].startswith('http'):
                issue_type = 'Invalid Canonical URL'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='High',
                    category='Technical SEO',
                    description=f"Invalid canonical URL: {crawl_data['canonical_url']}",
                    recommendation="Fix canonical URL to be absolute and valid",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Poor Internal Linking Structure
            if crawl_data['internal_links'] < 3:
                issue_type = 'Insufficient Internal Links'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Medium',
                    category='Technical SEO',
                    description=f"Only {crawl_data['internal_links']} internal links found",
                    recommendation="Add more contextual internal links to improve site navigation and SEO",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Excessive External Links
            if crawl_data['external_links'] > 50:
                issue_type = 'Excessive External Links'
                strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type=issue_type,
                    severity='Low',
                    category='Technical SEO',
                    description=f"{crawl_data['external_links']} external links found",
                    recommendation="Review external links and consider nofollow for non-essential links",
                    date_detected=current_time,
                    status='New',
                    impact_score=strategic_impact
                ))
            
            # Missing or Problematic Robots Meta Tag
            if crawl_data['robots_meta']:
                robots_content = crawl_data['robots_meta'].lower()
                if 'noindex' in robots_content and 'nofollow' in robots_content:
                    issue_type = 'Blocked by Robots Meta'
                    strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                    issues.append(TechnicalSEOIssue(
                        url=url,
                        issue_type=issue_type,
                        severity='Critical',
                        category='Technical SEO',
                        description=f"Page blocked by robots meta: {crawl_data['robots_meta']}",
                        recommendation="Remove noindex/nofollow if page should be indexed",
                        date_detected=current_time,
                        status='New',
                        impact_score=strategic_impact
                    ))
        
        # CROSS-PAGE ANALYSIS (Duplicate Content Detection)
        
        # Collect titles and meta descriptions for duplicate detection
        titles_seen = {}
        meta_descriptions_seen = {}
        
        for crawl_data in audit_results['crawl_data']:
            url = crawl_data['url']
            title = crawl_data.get('title', '').strip()
            meta_desc = crawl_data.get('meta_description', '').strip()
            
            # Track duplicate titles
            if title and len(title) > 10:  # Ignore very short titles
                if title in titles_seen:
                    # Both current and previously seen URL have duplicate titles
                    for duplicate_url in [url, titles_seen[title]]:
                        issue_type = 'Duplicate Title Tags'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, duplicate_url)
                        issues.append(TechnicalSEOIssue(
                            url=duplicate_url,
                            issue_type=issue_type,
                            severity='High',
                            category='Content',
                            description=f"Duplicate title tag: '{title}' (also found on other pages)",
                            recommendation="Create unique title tags for each page",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                else:
                    titles_seen[title] = url
            
            # Track duplicate meta descriptions
            if meta_desc and len(meta_desc) > 20:  # Ignore very short descriptions
                if meta_desc in meta_descriptions_seen:
                    # Both current and previously seen URL have duplicate meta descriptions
                    for duplicate_url in [url, meta_descriptions_seen[meta_desc]]:
                        issue_type = 'Duplicate Meta Descriptions'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, duplicate_url)
                        issues.append(TechnicalSEOIssue(
                            url=duplicate_url,
                            issue_type=issue_type,
                            severity='Medium',
                            category='Content',
                            description=f"Duplicate meta description (also found on other pages)",
                            recommendation="Create unique meta descriptions for each page",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                else:
                    meta_descriptions_seen[meta_desc] = url
        
        # SCHEMA AND STRUCTURED DATA ANALYSIS
        if audit_results.get('schema_data'):
            for schema_data in audit_results['schema_data']:
                url = schema_data.get('url')
                if url and not schema_data.get('error'):
                    json_ld_count = schema_data.get('json_ld_count', 0)
                    schema_types = schema_data.get('schema_types_found', [])
                    
                    # Missing structured data
                    if json_ld_count == 0:
                        issue_type = 'Missing Structured Data'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Medium',
                            category='Technical SEO',
                            description="No JSON-LD structured data found",
                            recommendation="Add relevant schema markup (Organization, Product, etc.)",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    # Missing important schema types for e-commerce
                    page_type = self.strategic_scorer.classify_page_type(url)
                    if page_type == 'product' and 'Product' not in schema_types:
                        issue_type = 'Missing Product Schema'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='High',
                            category='Technical SEO',
                            description="Product page missing Product schema markup",
                            recommendation="Add Product schema with price, availability, and reviews",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    if page_type == 'homepage' and 'Organization' not in schema_types:
                        issue_type = 'Missing Organization Schema'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score(issue_type, url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Medium',
                            category='Technical SEO',
                            description="Homepage missing Organization schema markup",
                            recommendation="Add Organization schema with company information",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
        
        # GSC-SPECIFIC ISSUE ANALYSIS (Top Impression Pages)
        if audit_results.get('gsc_data'):
            for domain, gsc_metrics_list in audit_results['gsc_data'].items():
                for gsc_metric in gsc_metrics_list:
                    url = gsc_metric.url
                    signals = gsc_metric.page_experience_signals
                    impressions = signals.get('impressions', 0)
                    clicks = signals.get('clicks', 0)
                    ctr = signals.get('ctr', 0)
                    position = signals.get('position', 0)
                    domain_name = signals.get('domain', domain)
                    
                    # High impression page with poor CTR
                    if impressions > 1000 and ctr < 0.02:  # Less than 2% CTR
                        issue_type = 'High Impressions Low CTR'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score('High Impressions Low CTR', url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='High',
                            category='GSC Performance',
                            description=f"Top page ({impressions:,} impressions) has low CTR ({ctr:.2%}) in {domain_name}",
                            recommendation="Improve title tag and meta description to increase click-through rate",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    # High impression page with poor ranking position
                    if impressions > 500 and position > 10:  # Not in top 10
                        issue_type = 'High Impressions Poor Position'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score('High Impressions Poor Position', url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Medium',
                            category='GSC Performance',
                            description=f"Page with {impressions:,} impressions ranking at position {position:.1f} in {domain_name}",
                            recommendation="Optimize content and technical SEO to improve ranking position",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    # Top impression page with zero clicks
                    if impressions > 1000 and clicks == 0:
                        issue_type = 'High Impressions Zero Clicks'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score('High Impressions Zero Clicks', url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Critical',
                            category='GSC Performance',
                            description=f"Top visibility page ({impressions:,} impressions) getting zero clicks in {domain_name}",
                            recommendation="Urgent: Review title/meta tags - page visible but not clickable",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    # Very high impression page - opportunity alert
                    if impressions > 10000:
                        issue_type = 'High Value Page Opportunity'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score('High Value Page Opportunity', url)
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Medium',
                            category='GSC Performance',
                            description=f"High-value page ({impressions:,} impressions, {clicks} clicks) in {domain_name}",
                            recommendation="Prioritize optimization - this page has significant traffic potential",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
                    
                    # Cross-reference GSC data with crawl issues
                    crawl_issues_for_url = [issue for issue in issues if issue.url == url and issue.category != 'GSC Performance']
                    if crawl_issues_for_url and impressions > 1000:
                        issue_type = 'High Traffic Page with Technical Issues'
                        strategic_impact = self.strategic_scorer.get_strategic_impact_score('High Traffic Page with Technical Issues', url)
                        technical_issues = ', '.join([issue.issue_type for issue in crawl_issues_for_url[:3]])
                        issues.append(TechnicalSEOIssue(
                            url=url,
                            issue_type=issue_type,
                            severity='Critical',
                            category='GSC Performance',
                            description=f"High-impression page ({impressions:,}) has technical issues: {technical_issues} in {domain_name}",
                            recommendation="URGENT: Fix technical issues on high-traffic page to prevent ranking loss",
                            date_detected=current_time,
                            status='New',
                            impact_score=strategic_impact
                        ))
        
        return issues
    
    def _categorize_issues_by_team(self, issues: List[TechnicalSEOIssue]) -> Dict:
        """Categorize issues by responsible team"""
        team_categories = {
            'tech_team': {
                'name': '🧑‍💻 TECH/DEV TEAM',
                'description': 'Server, infrastructure, and technical implementation issues',
                'issues': [],
                'issue_types': [
                    'HTTP Error', 'Slow Response Time', 'Large Page Size',
                    'Missing Canonical Tag', 'Invalid Canonical URL', 'Blocked by Robots Meta',
                    'Missing Structured Data', 'Missing Product Schema', 'Missing Organization Schema',
                    'Insufficient Internal Links', 'Excessive External Links',
                    'High Impressions Zero Clicks', 'High Traffic Page with Technical Issues',
                    'missing_canonical', 'crawlability', 'https_issues',
                    'server_errors', 'redirect_chains', 'broken_links'
                ]
            },
            'marketing_team': {
                'name': '📈 MARKETING TEAM', 
                'description': 'Content optimization, meta tags, and SEO strategy',
                'issues': [],
                'issue_types': [
                    'Missing Title Tag', 'Short Title Tag', 'Long Title Tag', 'Generic Title Tag',
                    'Duplicate Title Tags', 'Missing Meta Description', 'Short Meta Description',
                    'Long Meta Description', 'Duplicate Meta Descriptions', 'Missing H1 Tag',
                    'Multiple H1 Tags', 'Thin Content',
                    'High Impressions Low CTR', 'High Impressions Poor Position', 'High Value Page Opportunity',
                    'duplicate_content', 'keyword_optimization', 'thin_content', 'missing_schema'
                ]
            },
            'design_team': {
                'name': '🎨 DESIGN/UX TEAM',
                'description': 'User experience, mobile design, and visual optimization',
                'issues': [],
                'issue_types': [
                    'Images Without Alt Text', 'not_mobile_friendly', 'poor_ux',
                    'mobile_usability', 'touch_targets', 'viewport_issues',
                    'image_optimization', 'layout_issues'
                ]
            }
        }
        
        # Categorize each issue
        for issue in issues:
            issue_type = issue.issue_type
            categorized = False
            
            for team, data in team_categories.items():
                if issue_type in data['issue_types'] or \
                   any(keyword in issue_type.lower() for keyword in [t.lower().replace('_', ' ') for t in data['issue_types']]):
                    data['issues'].append(issue)
                    categorized = True
                    break
            
            # Default to tech team for uncategorized issues
            if not categorized:
                team_categories['tech_team']['issues'].append(issue)
        
        # Calculate team metrics
        for team, data in team_categories.items():
            team_issues = data['issues']
            data['total_issues'] = len(team_issues)
            data['critical_issues'] = len([i for i in team_issues if i.severity == 'Critical'])
            data['high_issues'] = len([i for i in team_issues if i.severity == 'High'])
            data['avg_impact'] = sum(i.impact_score for i in team_issues) / len(team_issues) if team_issues else 0
            data['priority_score'] = data['critical_issues'] * 10 + data['high_issues'] * 5 + len(team_issues)
        
        return team_categories
    
    def _generate_audit_summary(self, audit_results: Dict) -> Dict:
        """Generate audit summary statistics"""
        issues = audit_results['issues']
        crawl_data = audit_results['crawl_data']
        
        # Categorize issues by team
        team_breakdown = self._categorize_issues_by_team(issues)
        
        summary = {
            'total_issues': len(issues),
            'critical_issues': len([i for i in issues if i.severity == 'Critical']),
            'high_issues': len([i for i in issues if i.severity == 'High']),
            'medium_issues': len([i for i in issues if i.severity == 'Medium']),
            'low_issues': len([i for i in issues if i.severity == 'Low']),
            'issue_categories': {},
            'avg_response_time': 0,
            'error_pages': 0,
            'seo_score': 0
        }
        
        # Category breakdown
        for issue in issues:
            category = issue.category
            if category not in summary['issue_categories']:
                summary['issue_categories'][category] = 0
            summary['issue_categories'][category] += 1
        
        # Performance metrics
        if crawl_data:
            summary['avg_response_time'] = sum(c['response_time'] for c in crawl_data) / len(crawl_data)
            summary['error_pages'] = len([c for c in crawl_data if c['status_code'] >= 400])
        
        # Calculate Strategic SEO Score
        total_weighted_impact = 0
        total_weight = 0
        
        # Group issues by URL for page-level scoring
        page_issues = {}
        for issue in issues:
            if issue.url not in page_issues:
                page_issues[issue.url] = []
            page_issues[issue.url].append(issue)
        
        # Calculate weighted impact per page
        for url, url_issues in page_issues.items():
            page_type = self.strategic_scorer.classify_page_type(url)
            page_weight = self.strategic_scorer.page_weights.get(page_type, 2.0)
            
            # Sum impact for this page
            page_impact = sum(issue.impact_score for issue in url_issues)
            
            # Weight by business importance
            weighted_impact = page_impact * page_weight
            total_weighted_impact += weighted_impact
            total_weight += page_weight
        
        # Include pages with no issues
        crawl_data = audit_results.get('crawl_data', [])
        for page_data in crawl_data:
            url = page_data['url']
            if url not in page_issues:
                page_type = self.strategic_scorer.classify_page_type(url)
                page_weight = self.strategic_scorer.page_weights.get(page_type, 2.0)
                total_weight += page_weight
        
        if total_weight > 0:
            # Calculate strategic score
            avg_weighted_impact = total_weighted_impact / total_weight
            # Strategic score with business importance weighting
            summary['seo_score'] = max(0, 100 - (avg_weighted_impact / 50))  # Scale down for realistic scores
        else:
            summary['seo_score'] = 100
        
        # Add strategic insights
        summary['strategic_insights'] = self._get_strategic_insights(page_issues, crawl_data)
        
        # Add team breakdown
        summary['team_breakdown'] = team_breakdown
        
        return summary
    
    def _get_strategic_insights(self, page_issues: Dict, crawl_data: List[Dict]) -> Dict:
        """Generate strategic insights about SEO health"""
        insights = {
            'high_priority_pages_with_issues': 0,
            'page_type_breakdown': {},
            'critical_business_impact': []
        }
        
        # Analyze high-priority pages
        for url, issues in page_issues.items():
            page_type = self.strategic_scorer.classify_page_type(url)
            page_weight = self.strategic_scorer.page_weights.get(page_type, 2.0)
            
            # Count high-priority pages with issues
            if page_weight >= 4.0:  # Homepage, product, checkout pages
                insights['high_priority_pages_with_issues'] += 1
                
                # Critical issues on important pages
                critical_issues = [issue for issue in issues if issue.severity == 'Critical']
                if critical_issues:
                    insights['critical_business_impact'].append({
                        'url': url,
                        'page_type': page_type,
                        'critical_issues': len(critical_issues),
                        'business_importance': page_weight
                    })
            
            # Page type breakdown
            if page_type not in insights['page_type_breakdown']:
                insights['page_type_breakdown'][page_type] = {'pages': 0, 'issues': 0}
            insights['page_type_breakdown'][page_type]['pages'] += 1
            insights['page_type_breakdown'][page_type]['issues'] += len(issues)
        
        return insights
    
    def get_urls_to_audit_from_bigquery(self, domains: List[str] = None, limit: int = 100) -> List[str]:
        """Fetch URLs to audit from BigQuery CanonicalURLMapping table for all PrinterPix domains"""
        if not self.storage:
            logger.warning("BigQuery not configured, returning empty URL list")
            return []
            
        # Default to all PrinterPix domains if none specified
        if domains is None:
            domains = [
                'printerpix.com',
                'printerpix.co.uk', 
                'printerpix.fr',
                'printerpix.it',
                'printerpix.nl',
                'printerpix.es',
                'printerpix.de'
            ]
            
        try:
            # Create WHERE condition for multiple domains
            domain_conditions = " OR ".join([f"canonicalURL LIKE '%{domain}%'" for domain in domains])
            
            # Query to get URLs from your specific CanonicalURLMapping table using correct schema
            query = f"""
            SELECT DISTINCT canonicalURL as url
            FROM `printerpix-general.GA_Avanish.CanonicalURLMapping`
            WHERE canonicalURL IS NOT NULL
                AND ({domain_conditions})
            LIMIT {limit}
            """
            
            print(f"DEBUG: Executing BigQuery: {query}")
            query_job = self.storage.query(query)
            results = query_job.result()
            
            urls = [row.url for row in results if row.url]
            logger.info(f"Fetched {len(urls)} URLs from CanonicalURLMapping table for auditing")
            print(f"DEBUG: BigQuery returned {len(urls)} URLs")
            
            return urls
            
        except Exception as e:
            logger.error(f"Error fetching URLs from BigQuery CanonicalURLMapping: {e}")
            print(f"DEBUG: BigQuery error, falling back to default URLs: {str(e)}")
            # Fallback to default URLs if BigQuery fails (main domain only)
            fallback_domain = domains[0] if domains else 'printerpix.com'
            return [
                f'https://www.{fallback_domain}/',
                f'https://www.{fallback_domain}/photo-blankets/custom-mink-photo-blanket/',
                f'https://www.{fallback_domain}/photo-books-q/',
                f'https://www.{fallback_domain}/canvas-prints/v1/',
                f'https://www.{fallback_domain}/photo-calendars/personalized-photo-calendars-v1/',
                f'https://www.{fallback_domain}/photo-gifts/all/photo-frame-prints/',
                f'https://www.{fallback_domain}/photo-mugs/magic-mugs/',
                f'https://www.{fallback_domain}/blog/'
            ]
    
    def _store_audit_results(self, audit_results: Dict) -> None:
        """Store audit results in BigQuery"""
        if not hasattr(self, 'storage'):
            return
        
        try:
            # Convert issues to table format
            issues_data = []
            for issue in audit_results['issues']:
                issues_data.append({
                    'audit_timestamp': audit_results['audit_timestamp'],
                    'site_urls': str(audit_results['site_urls']),
                    'url': issue.url,
                    'issue_type': issue.issue_type,
                    'severity': issue.severity,
                    'category': issue.category,
                    'description': issue.description,
                    'recommendation': issue.recommendation,
                    'impact_score': issue.impact_score,
                    'status': issue.status
                })
            
            if issues_data:
                df = pd.DataFrame(issues_data)
                df.to_gbq(
                    self.table_ref,
                    project_id=self.config['bigquery']['project_id'],
                    if_exists='append'
                )
                logger.info(f"Stored {len(issues_data)} issues in BigQuery")
                
        except Exception as e:
            logger.error(f"Error storing audit results: {e}")

# Configuration and usage
def load_audit_config() -> Dict:
    """Load audit configuration from environment variables"""
    # Get API keys from environment variables
    pagespeed_api_key = os.getenv('PAGESPEED_API_KEY')
    gsc_credentials_path = os.getenv('GSC_CREDENTIALS_PATH')
    bigquery_credentials_path = os.getenv('BIGQUERY_CREDENTIALS_PATH')
    
    # Validate required environment variables
    if not pagespeed_api_key:
        raise ValueError("PAGESPEED_API_KEY environment variable is required")
    if not gsc_credentials_path:
        raise ValueError("GSC_CREDENTIALS_PATH environment variable is required")
    if not bigquery_credentials_path:
        raise ValueError("BIGQUERY_CREDENTIALS_PATH environment variable is required")
    
    return {
        'gsc_credentials_path': gsc_credentials_path,
        'pagespeed_api_key': pagespeed_api_key,
        'bigquery': {
            'project_id': 'printerpix-general',
            'dataset_id': 'GA_CG',
            'credentials_path': bigquery_credentials_path
        },
        'max_crawl_workers': 5,
        'crawl_timeout': 30,
        'audit_schedule': 'weekly'  # daily, weekly, monthly
    }

if __name__ == "__main__":
    # Load configuration
    config = load_audit_config()
    
    # Initialize auditor
    auditor = TechnicalSEOAuditor(config)
    
    # Get URLs to audit from BigQuery for all PrinterPix domains
    urls_to_audit = auditor.get_urls_to_audit_from_bigquery(limit=100)
    
    print(f"DEBUG: Fetched {len(urls_to_audit)} URLs from BigQuery")
    print(f"DEBUG: First 5 URLs: {urls_to_audit[:5]}")
    
    # Group URLs by domain for summary
    domain_counts = {}
    for url in urls_to_audit:
        for domain in ['printerpix.com', 'printerpix.co.uk', 'printerpix.fr', 'printerpix.it', 'printerpix.nl', 'printerpix.es', 'printerpix.de']:
            if domain in url:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
                break
    print(f"DEBUG: URLs per domain: {domain_counts}")
    
    # Run comprehensive audit for all GSC domain properties
    gsc_properties = [
        'sc-domain:printerpix.com',
        'sc-domain:printerpix.co.uk', 
        'sc-domain:printerpix.fr',
        'sc-domain:printerpix.it',
        'sc-domain:printerpix.nl',
        'sc-domain:printerpix.es',
        'sc-domain:printerpix.de'
    ]
    
    print(f"DEBUG: GSC properties to query: {gsc_properties}")
    results = auditor.run_comprehensive_audit(gsc_properties, urls_to_audit)
    
    # Save results to file
    # Enhanced JSON export with team-specific sections
    enhanced_results = results.copy()
    
    # Add team-specific issue exports
    team_breakdown = results['summary'].get('team_breakdown', {})
    enhanced_results['team_exports'] = {}
    
    for team_key, team_data in team_breakdown.items():
        if team_data['total_issues'] > 0:
            enhanced_results['team_exports'][team_key] = {
                'team_name': team_data['name'],
                'description': team_data['description'],
                'summary': {
                    'total_issues': team_data['total_issues'],
                    'critical_issues': team_data['critical_issues'],
                    'high_issues': team_data['high_issues'],
                    'priority_score': team_data['priority_score'],
                    'avg_impact': team_data['avg_impact']
                },
                'detailed_issues': [
                    {
                        'url': issue.url,
                        'issue_type': issue.issue_type,
                        'severity': issue.severity,
                        'category': issue.category,
                        'description': issue.description,
                        'recommendation': issue.recommendation,
                        'impact_score': issue.impact_score,
                        'date_detected': issue.date_detected.isoformat(),
                        'status': issue.status
                    } for issue in team_data['issues']
                ]
            }
    
    with open('technical_seo_audit_results.json', 'w') as f:
        json.dump(enhanced_results, f, indent=2, default=str)
    
    # Print strategic summary
    summary = results['summary']
    insights = summary.get('strategic_insights', {})
    
    print("\n" + "="*60)
    print("    STRATEGIC SEO AUDIT SUMMARY")
    print("="*60)
    
    # Overall score with grade
    score = summary['seo_score']
    if score >= 90:
        grade, status = 'A+', '🟢 EXCELLENT'
    elif score >= 80:
        grade, status = 'A', '🟢 GOOD'
    elif score >= 70:
        grade, status = 'B', '🟡 FAIR'
    elif score >= 60:
        grade, status = 'C', '🟠 POOR'
    else:
        grade, status = 'D', '🔴 CRITICAL'
    
    print(f"Overall SEO Health: {score:.1f}/100 (Grade: {grade})")
    print(f"Status: {status}")
    
    print(f"\n📊 ISSUE BREAKDOWN:")
    print(f"   Total Issues: {summary['total_issues']}")
    print(f"   Critical: {summary['critical_issues']}, High: {summary['high_issues']}, Medium: {summary['medium_issues']}, Low: {summary['low_issues']}")
    
    print(f"\n⚡ PERFORMANCE:")
    print(f"   Average Response Time: {summary.get('avg_response_time', 0):.2f}s")
    print(f"   Pages with Errors: {summary.get('error_pages', 0)}")
    
    # Strategic insights
    if insights:
        print(f"\n🎯 STRATEGIC INSIGHTS:")
        print(f"   High-Priority Pages with Issues: {insights.get('high_priority_pages_with_issues', 0)}")
        
        # Critical business impact
        critical_business = insights.get('critical_business_impact', [])
        if critical_business:
            print(f"   🚨 Critical Business Impact Pages:")
            for page in critical_business[:3]:  # Show top 3
                print(f"     • {page['page_type'].title()}: {page['critical_issues']} critical issues")
        
        # Page type breakdown
        page_breakdown = insights.get('page_type_breakdown', {})
        if page_breakdown:
            print(f"   📄 Issues by Page Type:")
            for page_type, data in sorted(page_breakdown.items(), key=lambda x: x[1]['issues'], reverse=True)[:4]:
                print(f"     • {page_type.title()}: {data['issues']} issues across {data['pages']} pages")
    
    # Team breakdown
    team_breakdown = summary.get('team_breakdown', {})
    if team_breakdown:
        print(f"\n💼 TEAM ASSIGNMENTS:")
        # Sort teams by priority score (critical + high issues)
        sorted_teams = sorted(team_breakdown.items(), key=lambda x: x[1]['priority_score'], reverse=True)
        
        for team_key, team_data in sorted_teams:
            if team_data['total_issues'] > 0:
                print(f"\n   {team_data['name']}")
                print(f"   {team_data['description']}")
                print(f"   📊 Issues: {team_data['total_issues']} total | Critical: {team_data['critical_issues']} | High: {team_data['high_issues']}")
                print(f"   🎯 Priority Score: {team_data['priority_score']:.0f} | Avg Impact: {team_data['avg_impact']:.1f}")
                
                # Show all issues for this team with details
                team_issues = sorted(team_data['issues'], key=lambda x: x.impact_score, reverse=True)
                if team_issues:
                    print(f"   🔴 Issues to Fix ({len(team_issues)} total):")
                    for i, issue in enumerate(team_issues, 1):
                        # Severity indicator
                        if issue.severity == 'Critical':
                            severity_icon = '🚨'
                        elif issue.severity == 'High':
                            severity_icon = '⚠️'
                        elif issue.severity == 'Medium':
                            severity_icon = '🟡'
                        else:
                            severity_icon = '🔵'
                        
                        print(f"     {i}. {severity_icon} {issue.issue_type} ({issue.severity})")
                        print(f"        📍 URL: {issue.url}")
                        print(f"        📝 Issue: {issue.description}")
                        print(f"        💡 Fix: {issue.recommendation}")
                        print(f"        📊 Impact Score: {issue.impact_score}")
                        print()  # Empty line for readability
    
    # Alerts
    if summary['critical_issues'] > 0:
        print(f"\n🚨 IMMEDIATE ACTION REQUIRED!")
        print(f"   {summary['critical_issues']} critical issues found that may impact rankings")
    
    if insights.get('high_priority_pages_with_issues', 0) > 0:
        print(f"\n⚠️  BUSINESS IMPACT ALERT!")
        print(f"   {insights['high_priority_pages_with_issues']} high-value pages have SEO issues")
    
    # Team action summary
    if team_breakdown:
        print(f"\n📋 ACTION SUMMARY BY TEAM:")
        for team_key, team_data in sorted_teams:
            if team_data['total_issues'] > 0:
                if team_data['critical_issues'] > 0:
                    urgency = "🚨 URGENT"
                elif team_data['high_issues'] > 0:
                    urgency = "⚠️ HIGH PRIORITY"
                else:
                    urgency = "🟡 MEDIUM PRIORITY"
                print(f"   {team_data['name']}: {urgency} - {team_data['total_issues']} issues to resolve")
        
        print(f"\n📋 DETAILED ISSUE EXPORT:")
        print(f"   For detailed issue lists per team, check the JSON file sections:")
        for team_key, team_data in sorted_teams:
            if team_data['total_issues'] > 0:
                print(f"   • {team_data['name']}: 'team_breakdown' → '{team_key}' → 'issues'")
    
    print(f"\n💾 Detailed results saved to: technical_seo_audit_results.json")
    print("="*60)