"""
Automated Technical SEO Audit System
Comprehensive technical SEO monitoring using Google APIs and web crawling
"""

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from google.cloud import bigquery
from google.oauth2 import service_account
import requests.utils
import pandas as pd
import logging
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    def __init__(self, api_key: str):
        """Initialize Google Search Console API client with API key
        
        Note: Google Search Console API actually requires OAuth2 authentication,
        not a simple API key. This implementation uses the API key approach for
        simplicity, but in production you would need to implement OAuth2 flow.
        
        For OAuth2 implementation, you would need:
        1. Create OAuth2 credentials in Google Cloud Console
        2. Implement the OAuth2 authorization flow
        3. Use the access token for API requests
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/webmasters/v3"
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def get_sites(self) -> List[str]:
        """Get all verified sites in GSC using API key"""
        try:
            response = requests.get(
                f"{self.base_url}/sites",
                headers=self.headers,
                params={'key': self.api_key}
            )
            response.raise_for_status()
            data = response.json()
            return [site['siteUrl'] for site in data.get('siteEntry', [])]
        except requests.RequestException as e:
            logger.error(f"Error fetching sites: {e}")
            return []
    
    def get_coverage_issues(self, site_url: str, days_back: int = 30) -> List[GSCMetrics]:
        """Get index coverage issues from GSC using API key"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        try:
            request_body = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'type': 'web',
                'dataState': 'final'
            }
            
            response = requests.post(
                f"{self.base_url}/sites/{requests.utils.quote(site_url, safe='')}/searchAnalytics/query",
                headers=self.headers,
                json=request_body,
                params={'key': self.api_key}
            )
            response.raise_for_status()
            data = response.json()
            
            # Process coverage data
            coverage_metrics = []
            for row in data.get('rows', []):
                # Extract coverage information
                coverage_metrics.append(GSCMetrics(
                    url=site_url,
                    date=end_date.strftime('%Y-%m-%d'),
                    coverage_status='Unknown',  # Would need inspection API
                    error_type=None,
                    mobile_usability_issues=[],
                    page_experience_signals={},
                    crawl_stats={}
                ))
            
            return coverage_metrics
            
        except requests.RequestException as e:
            logger.error(f"Error fetching coverage data: {e}")
            return []
    
    def get_mobile_usability_issues(self, site_url: str) -> List[Dict]:
        """Get mobile usability issues using API key"""
        try:
            response = requests.get(
                f"{self.base_url}/sites/{requests.utils.quote(site_url, safe='')}/mobileUsabilityIssues",
                headers=self.headers,
                params={'key': self.api_key}
            )
            response.raise_for_status()
            data = response.json()
            return data.get('issues', [])
        except requests.RequestException as e:
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
            'Slow Response Time': 80,
            'not_mobile_friendly': 90,
            'no_https': 85,
            'Missing Title Tag': 95,
            'duplicate_title': 90,
            
            # High (significant SEO impact)
            'slow_page_speed': 80,
            'broken_internal_links': 75,
            'missing_canonical': 70,
            'thin_content': 75,
            'Missing H1 Tag': 65,
            
            # Medium (moderate impact)
            'Missing Meta Description': 45,  # CTR impact, not ranking
            'Images Without Alt Text': 40,  # Accessibility > SEO
            'missing_schema': 50,
            'Large Page Size': 55,
            
            # Low (minor optimization)
            'Multiple H1 Tags': 30,
            'long_title': 25,
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
        if config.get('gsc_api_key'):
            self.gsc = GoogleSearchConsoleAPI(config['gsc_api_key'])
        else:
            self.gsc = None
            
        self.crawler = WebCrawler(
            max_workers=config.get('max_crawl_workers', 5),
            timeout=config.get('crawl_timeout', 30)
        )
        
        self.schema_validator = SchemaValidator()
        
        # Initialize PageSpeed Insights collector directly
        self.pagespeed_collector = PageSpeedCollector('AIzaSyB5eEwvDY-6yvjB4Pdz_qOgkNyduZlC9fU')
        
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
    
    def run_comprehensive_audit(self, site_url: str, urls_to_audit: List[str]) -> Dict:
        """Run comprehensive technical SEO audit"""
        audit_start = datetime.now(timezone.utc)
        logger.info(f"Starting comprehensive technical SEO audit for {site_url}")
        
        audit_results = {
            'site_url': site_url,
            'audit_timestamp': audit_start.isoformat(),
            'total_urls_audited': len(urls_to_audit),
            'gsc_data': {},
            'crawl_data': [],
            'pagespeed_data': [],
            'schema_data': [],
            'issues': [],
            'summary': {}
        }
        
        # 1. Google Search Console Data
        if self.gsc:
            logger.info("Fetching Google Search Console data...")
            audit_results['gsc_data'] = {
                'coverage_issues': self.gsc.get_coverage_issues(site_url),
                'mobile_usability': self.gsc.get_mobile_usability_issues(site_url)
            }
        
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
                    'missing_canonical', 'crawlability', 'https_issues',
                    'server_errors', 'redirect_chains', 'broken_links'
                ]
            },
            'marketing_team': {
                'name': '📈 MARKETING TEAM', 
                'description': 'Content optimization, meta tags, and SEO strategy',
                'issues': [],
                'issue_types': [
                    'Missing Title Tag', 'Missing Meta Description', 'Missing H1 Tag',
                    'Multiple H1 Tags', 'duplicate_content', 'keyword_optimization',
                    'thin_content', 'missing_schema'
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
                    'site_url': audit_results['site_url'],
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
    """Load audit configuration"""
    return {
        'gsc_api_key': 'AIzaSyD33yeTlxud2DKqV3W2Myjywm7aKOq0np0',  # Your Google Search Console API key
        'pagespeed_api_key': 'AIzaSyB5eEwvDY-6yvjB4Pdz_qOgkNyduZlC9fU',  # Your PageSpeed Insights API key
        'bigquery': {
            'project_id': 'printerpix-general',
            'dataset_id': 'GA_CG',
            'credentials_path': r'C:\Users\charlottegong\Downloads\json_json.json'
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
    
    # Define URLs to audit
    urls_to_audit = [
        'https://www.printerpix.com/',
        'https://www.printerpix.com/photo-blankets/custom-mink-photo-blanket/',
        'https://www.printerpix.com/photo-books-q/',
        'https://www.printerpix.com/canvas-prints/v1/',
        'https://www.printerpix.com/photo-calendars/personalized-photo-calendars-v1/',
        'https://www.printerpix.com/photo-prints/photo-frame-prints/',
        'https://www.printerpix.com/photo-mugs/magic-mugs/'
    ]
    
    # Run comprehensive audit
    results = auditor.run_comprehensive_audit('https://www.printerpix.com/', urls_to_audit)
    
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