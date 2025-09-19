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

class TechnicalSEOAuditor:
    def __init__(self, config: Dict):
        """Initialize the technical SEO audit system"""
        self.config = config
        
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
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='HTTP Error',
                    severity='Critical' if crawl_data['status_code'] >= 500 else 'High',
                    category='Crawlability',
                    description=f"HTTP {crawl_data['status_code']} error",
                    recommendation=f"Fix HTTP {crawl_data['status_code']} error to ensure page is accessible",
                    date_detected=current_time,
                    status='New',
                    impact_score=90 if crawl_data['status_code'] >= 500 else 75
                ))
            
            # Performance Issues
            if crawl_data['response_time'] > 3.0:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Slow Response Time',
                    severity='High' if crawl_data['response_time'] > 5.0 else 'Medium',
                    category='Performance',
                    description=f"Response time: {crawl_data['response_time']:.2f}s",
                    recommendation="Optimize server response time to under 2 seconds",
                    date_detected=current_time,
                    status='New',
                    impact_score=80 if crawl_data['response_time'] > 5.0 else 60
                ))
            
            # Content Issues
            if not crawl_data['title']:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Missing Title Tag',
                    severity='High',
                    category='Content',
                    description="Page is missing title tag",
                    recommendation="Add descriptive title tag (50-60 characters)",
                    date_detected=current_time,
                    status='New',
                    impact_score=85
                ))
            
            if not crawl_data['meta_description']:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Missing Meta Description',
                    severity='Medium',
                    category='Content',
                    description="Page is missing meta description",
                    recommendation="Add compelling meta description (150-160 characters)",
                    date_detected=current_time,
                    status='New',
                    impact_score=60
                ))
            
            # H1 Issues
            if not crawl_data['h1_tags']:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Missing H1 Tag',
                    severity='Medium',
                    category='Content',
                    description="Page is missing H1 tag",
                    recommendation="Add descriptive H1 tag for page hierarchy",
                    date_detected=current_time,
                    status='New',
                    impact_score=50
                ))
            elif len(crawl_data['h1_tags']) > 1:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Multiple H1 Tags',
                    severity='Low',
                    category='Content',
                    description=f"Page has {len(crawl_data['h1_tags'])} H1 tags",
                    recommendation="Use only one H1 tag per page",
                    date_detected=current_time,
                    status='New',
                    impact_score=30
                ))
            
            # Image Issues
            if crawl_data['images_without_alt'] > 0:
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Images Without Alt Text',
                    severity='Medium',
                    category='Content',
                    description=f"{crawl_data['images_without_alt']} images without alt text",
                    recommendation="Add descriptive alt text to all images",
                    date_detected=current_time,
                    status='New',
                    impact_score=40
                ))
            
            # Large Page Size
            if crawl_data['page_size'] > 1024 * 1024:  # 1MB
                issues.append(TechnicalSEOIssue(
                    url=url,
                    issue_type='Large Page Size',
                    severity='Medium',
                    category='Performance',
                    description=f"Page size: {crawl_data['page_size'] / 1024 / 1024:.2f}MB",
                    recommendation="Optimize images and resources to reduce page size",
                    date_detected=current_time,
                    status='New',
                    impact_score=55
                ))
        
        return issues
    
    def _generate_audit_summary(self, audit_results: Dict) -> Dict:
        """Generate audit summary statistics"""
        issues = audit_results['issues']
        crawl_data = audit_results['crawl_data']
        
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
        
        # Calculate SEO score (100 - weighted issue impact)
        total_impact = sum(issue.impact_score for issue in issues)
        max_possible_impact = len(audit_results['crawl_data']) * 100
        
        if max_possible_impact > 0:
            summary['seo_score'] = max(0, 100 - (total_impact / max_possible_impact * 100))
        else:
            summary['seo_score'] = 100
        
        return summary
    
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
    with open('technical_seo_audit_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)h
    
    # Print summary
    summary = results['summary']
    print("\n=== TECHNICAL SEO AUDIT SUMMARY ===")
    print(f"SEO Score: {summary['seo_score']:.1f}/100")
    print(f"Total Issues: {summary['total_issues']}")
    print(f"Critical: {summary['critical_issues']}, High: {summary['high_issues']}, Medium: {summary['medium_issues']}, Low: {summary['low_issues']}")
    print(f"Average Response Time: {summary['avg_response_time']:.2f}s")
    print(f"Pages with Errors: {summary['error_pages']}")
    
    if summary['critical_issues'] > 0:
        print("\n🚨 CRITICAL ISSUES FOUND - Immediate attention required!")
    
    print("\nDetailed results saved to: technical_seo_audit_results.json")