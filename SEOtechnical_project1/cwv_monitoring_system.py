"""
Core Web Vitals Monitoring System
Automated data collection, BigQuery storage, and Looker integration
"""

import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
from dataclasses import dataclass
import schedule

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CWVMetrics:
    url: str
    date_fetched: datetime
    device_type: str
    # Core Web Vitals - Field Data (Real Users)
    field_lcp: Optional[float]
    field_fid: Optional[float]
    field_cls: Optional[float]
    field_fcp: Optional[float]
    field_inp: Optional[float]
    field_ttfb: Optional[float]
    # Lab Data (Lighthouse)
    lab_lcp: Optional[float]
    lab_fid: Optional[float]
    lab_cls: Optional[float]
    lab_fcp: Optional[float]
    lab_ttfb: Optional[float]
    lab_performance_score: Optional[int]
    # Overall Scores
    field_data_score: Optional[str]
    opportunities_count: int
    total_blocking_time: Optional[float]
    speed_index: Optional[float]

class PageSpeedInsightsCollector:
    def __init__(self, api_key: str):
        self.api_key = "AIzaSyB5eEwvDY-6yvjB4Pdz_qOgkNyduZlC9fU"
        self.base_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        
    def fetch_cwv_data(self, url: str, strategy: str = 'mobile') -> Optional[CWVMetrics]:
        """Fetch Core Web Vitals data from PageSpeed Insights API"""
        params = {
            'url': url,
            'key': self.api_key,
            'strategy': strategy,
            'category': ['performance'],
            'locale': 'en'
        }
        
        try:
            logger.info(f"Fetching CWV data for {url} ({strategy})")
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            return self._parse_api_response(data, url, strategy)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing data for {url}: {e}")
            return None
    
    def _parse_api_response(self, data: Dict, url: str, strategy: str) -> CWVMetrics:
        """Parse PageSpeed Insights API response into CWVMetrics"""
        
        # Extract field data (real user metrics from CrUX)
        origin_loading_exp = data.get('originLoadingExperience', {})
        field_metrics = origin_loading_exp.get('metrics', {})
        overall_category = origin_loading_exp.get('overall_category', 'UNKNOWN')
        
        # Extract lab data (Lighthouse metrics)
        lighthouse_result = data.get('lighthouseResult', {})
        audits = lighthouse_result.get('audits', {})
        categories = lighthouse_result.get('categories', {})
        
        # Performance score
        performance_score = None
        if 'performance' in categories:
            performance_score = int(categories['performance'].get('score', 0) * 100)
        
        # Parse field metrics (CrUX data)
        field_lcp = self._extract_field_metric(field_metrics, 'LARGEST_CONTENTFUL_PAINT_MS')
        field_fid = self._extract_field_metric(field_metrics, 'FIRST_INPUT_DELAY_MS')
        field_cls = self._extract_field_metric(field_metrics, 'CUMULATIVE_LAYOUT_SHIFT_SCORE')
        field_fcp = self._extract_field_metric(field_metrics, 'FIRST_CONTENTFUL_PAINT_MS')
        field_inp = self._extract_field_metric(field_metrics, 'INTERACTION_TO_NEXT_PAINT')
        field_ttfb = self._extract_field_metric(field_metrics, 'EXPERIMENTAL_TIME_TO_FIRST_BYTE')
        
        # Parse lab metrics (Lighthouse data)
        lab_lcp = self._extract_lab_metric(audits, 'largest-contentful-paint')
        lab_fid = self._extract_lab_metric(audits, 'max-potential-fid')
        lab_cls = self._extract_lab_metric(audits, 'cumulative-layout-shift')
        lab_fcp = self._extract_lab_metric(audits, 'first-contentful-paint')
        lab_ttfb = self._extract_lab_metric(audits, 'server-response-time')
        
        # Additional metrics
        total_blocking_time = self._extract_lab_metric(audits, 'total-blocking-time')
        speed_index = self._extract_lab_metric(audits, 'speed-index')
        
        # Count opportunities
        opportunities_count = sum(1 for audit in audits.values() 
                                if isinstance(audit, dict) and 
                                audit.get('score') is not None and
                                audit.get('score') < 1 and 
                                'savings' in str(audit.get('details', {})))
        
        return CWVMetrics(
            url=url,
            date_fetched=datetime.now(timezone.utc),
            device_type=strategy,
            field_lcp=field_lcp,
            field_fid=field_fid,
            field_cls=field_cls,
            field_fcp=field_fcp,
            field_inp=field_inp,
            field_ttfb=field_ttfb,
            lab_lcp=lab_lcp,
            lab_fid=lab_fid,
            lab_cls=lab_cls,
            lab_fcp=lab_fcp,
            lab_ttfb=lab_ttfb,
            lab_performance_score=performance_score,
            field_data_score=overall_category,
            opportunities_count=opportunities_count,
            total_blocking_time=total_blocking_time,
            speed_index=speed_index
        )
    
    def _extract_field_metric(self, field_metrics: Dict, metric_key: str) -> Optional[float]:
        """Extract field metric value (75th percentile) and standardize units to match UI"""
        metric = field_metrics.get(metric_key, {})
        percentile = metric.get('percentile')
        
        if percentile is None:
            return None
            
        # Convert based on metric type to match PageSpeed Insights UI
        if metric_key in ['LARGEST_CONTENTFUL_PAINT_MS', 'FIRST_CONTENTFUL_PAINT_MS', 
                         'FIRST_INPUT_DELAY_MS', 'INTERACTION_TO_NEXT_PAINT',
                         'EXPERIMENTAL_TIME_TO_FIRST_BYTE']:
            # Convert milliseconds to seconds with 1 decimal place (like UI)
            return round(float(percentile) / 1000, 1)
        elif metric_key == 'CUMULATIVE_LAYOUT_SHIFT_SCORE':
            # Keep as decimal with 3 decimal places (like UI)
            return round(float(percentile), 3)
        else:
            return float(percentile)
    
    def _extract_lab_metric(self, audits: Dict, audit_key: str) -> Optional[float]:
        """Extract lab metric numeric value and standardize units to match UI"""
        audit = audits.get(audit_key, {})
        numeric_value = audit.get('numericValue')
        
        if numeric_value is None:
            return None
            
        # Convert based on metric type to match PageSpeed Insights UI
        if audit_key in ['largest-contentful-paint', 'first-contentful-paint', 
                        'max-potential-fid', 'server-response-time', 'total-blocking-time']:
            # Convert milliseconds to seconds with 1 decimal place
            return round(float(numeric_value) / 1000, 1)
        elif audit_key == 'cumulative-layout-shift':
            # Keep as decimal with 3 decimal places
            return round(float(numeric_value), 3)
        elif audit_key == 'speed-index':
            # Convert to seconds with 1 decimal place
            return round(float(numeric_value) / 1000, 1)
        else:
            return float(numeric_value)

class BigQueryStorage:
    def __init__(self, project_id: str, dataset_id: str, table_id: str, 
                 credentials_path: Optional[str] = None):
        """
        Initialize BigQuery client
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset name
            table_id: BigQuery table name
            credentials_path: Path to service account JSON file
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            # Use default credentials (e.g., from Google Cloud environment)
            self.client = bigquery.Client(project=project_id)
        
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    def create_table_if_not_exists(self):
        """Create BigQuery table with proper schema if it doesn't exist"""
        schema = [
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date_fetched", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("device_type", "STRING", mode="REQUIRED"),
            # Field data (real users) - All timing metrics in seconds to match UI
            bigquery.SchemaField("field_lcp", "FLOAT", mode="NULLABLE", description="Largest Contentful Paint (seconds)"),
            bigquery.SchemaField("field_fid", "FLOAT", mode="NULLABLE", description="First Input Delay (seconds)"),
            bigquery.SchemaField("field_cls", "FLOAT", mode="NULLABLE", description="Cumulative Layout Shift (unitless)"),
            bigquery.SchemaField("field_fcp", "FLOAT", mode="NULLABLE", description="First Contentful Paint (seconds)"),
            bigquery.SchemaField("field_inp", "FLOAT", mode="NULLABLE", description="Interaction to Next Paint (seconds)"),
            bigquery.SchemaField("field_ttfb", "FLOAT", mode="NULLABLE", description="Time to First Byte (seconds)"),
            # Lab data (Lighthouse) - All timing metrics in seconds to match UI
            bigquery.SchemaField("lab_lcp", "FLOAT", mode="NULLABLE", description="LCP from Lighthouse (seconds)"),
            bigquery.SchemaField("lab_fid", "FLOAT", mode="NULLABLE", description="Max potential FID (seconds)"),
            bigquery.SchemaField("lab_cls", "FLOAT", mode="NULLABLE", description="CLS from Lighthouse (unitless)"),
            bigquery.SchemaField("lab_fcp", "FLOAT", mode="NULLABLE", description="FCP from Lighthouse (seconds)"),
            bigquery.SchemaField("lab_ttfb", "FLOAT", mode="NULLABLE", description="TTFB from Lighthouse (seconds)"),
            bigquery.SchemaField("lab_performance_score", "INTEGER", mode="NULLABLE"),
            # Overall scores and metrics
            bigquery.SchemaField("field_data_score", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("opportunities_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("total_blocking_time", "FLOAT", mode="NULLABLE", description="Total blocking time (seconds)"),
            bigquery.SchemaField("speed_index", "FLOAT", mode="NULLABLE", description="Speed index (seconds)"),
        ]
        
        table = bigquery.Table(self.table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_fetched"
        )
        
        try:
            self.client.create_table(table)
            logger.info(f"Created table {self.table_ref}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Table {self.table_ref} already exists")
            else:
                logger.error(f"Error creating table: {e}")
                raise
    
    def insert_metrics(self, metrics_list: List[CWVMetrics]) -> bool:
        """Insert CWV metrics into BigQuery table"""
        if not metrics_list:
            logger.warning("No metrics to insert")
            return True
        
        # Convert to list of dictionaries
        rows_to_insert = []
        for metrics in metrics_list:
            row = {
                "url": metrics.url,
                "date_fetched": metrics.date_fetched.isoformat(),
                "device_type": metrics.device_type,
                "field_lcp": metrics.field_lcp,
                "field_fid": metrics.field_fid,
                "field_cls": metrics.field_cls,
                "field_fcp": metrics.field_fcp,
                "field_inp": metrics.field_inp,
                "field_ttfb": metrics.field_ttfb,
                "lab_lcp": metrics.lab_lcp,
                "lab_fid": metrics.lab_fid,
                "lab_cls": metrics.lab_cls,
                "lab_fcp": metrics.lab_fcp,
                "lab_ttfb": metrics.lab_ttfb,
                "lab_performance_score": metrics.lab_performance_score,
                "field_data_score": metrics.field_data_score,
                "opportunities_count": metrics.opportunities_count,
                "total_blocking_time": metrics.total_blocking_time,
                "speed_index": metrics.speed_index,
            }
            rows_to_insert.append(row)
        
        try:
            table = self.client.get_table(self.table_ref)
            errors = self.client.insert_rows_json(table, rows_to_insert)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False
            else:
                logger.info(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery")
                return True
                
        except Exception as e:
            logger.error(f"Error inserting into BigQuery: {e}")
            return False

class CWVMonitoringSystem:
    def __init__(self, config: Dict):
        """
        Initialize the complete CWV monitoring system
        
        Args:
            config: Configuration dictionary with API keys, BigQuery settings, etc.
        """
        self.config = config
        self.collector = PageSpeedInsightsCollector(config['pagespeed_api_key'])
        self.storage = BigQueryStorage(
            project_id=config['bigquery']['project_id'],
            dataset_id=config['bigquery']['dataset_id'],
            table_id=config['bigquery']['table_id'],
            credentials_path=config['bigquery'].get('credentials_path')
        )
        
        # Canonical URLs to monitor
        self.canonical_urls = config.get('canonical_urls', [])
        
        # Device types to test
        self.device_types = config.get('device_types', ['mobile', 'desktop'])
        
        # Rate limiting
        self.request_delay = config.get('request_delay_seconds', 2)
    
    def setup(self):
        """Initialize BigQuery table and other setup tasks"""
        logger.info("Setting up CWV monitoring system...")
        self.storage.create_table_if_not_exists()
        logger.info("Setup completed")
    
    def collect_and_store_cwv_data(self) -> bool:
        """Main method to collect CWV data for all URLs and store in BigQuery"""
        logger.info(f"Starting CWV data collection for {len(self.canonical_urls)} URLs")
        
        all_metrics = []
        
        for url in self.canonical_urls:
            for device_type in self.device_types:
                try:
                    # Fetch CWV data
                    metrics = self.collector.fetch_cwv_data(url, device_type)
                    
                    if metrics:
                        all_metrics.append(metrics)
                        logger.info(f"Collected data for {url} ({device_type})")
                    else:
                        logger.warning(f"Failed to collect data for {url} ({device_type})")
                    
                    # Rate limiting
                    time.sleep(self.request_delay)
                    
                except Exception as e:
                    logger.error(f"Error processing {url} ({device_type}): {e}")
                    continue
        
        # Store all metrics in BigQuery
        if all_metrics:
            success = self.storage.insert_metrics(all_metrics)
            if success:
                logger.info(f"Successfully stored {len(all_metrics)} CWV measurements")
                return True
            else:
                logger.error("Failed to store CWV data in BigQuery")
                return False
        else:
            logger.warning("No CWV data collected")
            return False
    
    def run_scheduled_collection(self):
        """Set up scheduled data collection"""
        logger.info("Setting up scheduled CWV data collection...")
        
        # Schedule weekly collection (every Monday at 9 AM)
        schedule.every().monday.at("09:00").do(self.collect_and_store_cwv_data)
        
        # Also run daily for critical pages (subset)
        critical_urls = self.config.get('critical_urls', self.canonical_urls[:5])
        
        def collect_critical():
            original_urls = self.canonical_urls
            self.canonical_urls = critical_urls
            result = self.collect_and_store_cwv_data()
            self.canonical_urls = original_urls
            return result
        
        schedule.every().day.at("06:00").do(collect_critical)
        
        logger.info("Scheduled jobs configured. Running scheduler...")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def generate_looker_sql(self) -> str:
        """Generate SQL queries for Looker dashboard"""
        sql_template = f"""
        -- Core Web Vitals Trend Analysis
        -- Use this in Looker to create CWV dashboards
        
        WITH cwv_trends AS (
          SELECT 
            url,
            DATE(date_fetched) as date,
            device_type,
            field_lcp,
            field_fid,
            field_cls,
            field_inp,
            lab_performance_score,
            field_data_score,
            -- CWV Pass/Fail Logic (thresholds in seconds to match UI)
            CASE 
              WHEN field_lcp IS NOT NULL AND field_lcp <= 2.5 
                   AND (field_fid IS NULL OR field_fid <= 0.1)
                   AND (field_inp IS NULL OR field_inp <= 0.2)
                   AND field_cls <= 0.1 
              THEN 'PASS'
              ELSE 'FAIL'
            END as cwv_status,
            -- Performance categories
            CASE 
              WHEN lab_performance_score >= 90 THEN 'Good'
              WHEN lab_performance_score >= 50 THEN 'Needs Improvement' 
              ELSE 'Poor'
            END as performance_category
          FROM `{self.storage.table_ref}`
          WHERE date_fetched >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        )
        
        SELECT 
          date,
          url,
          device_type,
          field_lcp as lcp_seconds,
          field_fid as fid_seconds,
          field_cls,
          field_inp as inp_seconds,
          lab_performance_score,
          cwv_status,
          performance_category,
          -- Calculate week-over-week changes
          LAG(field_lcp, 7) OVER (PARTITION BY url, device_type ORDER BY date) as lcp_prev_week,
          LAG(lab_performance_score, 7) OVER (PARTITION BY url, device_type ORDER BY date) as score_prev_week
        FROM cwv_trends
        ORDER BY date DESC, url, device_type;
        
        -- Summary metrics for dashboard cards
        SELECT 
          COUNT(DISTINCT url) as total_pages_monitored,
          COUNTIF(cwv_status = 'PASS') / COUNT(*) * 100 as cwv_pass_rate,
          AVG(lab_performance_score) as avg_performance_score,
          COUNTIF(performance_category = 'Good') / COUNT(*) * 100 as good_performance_rate
        FROM cwv_trends
        WHERE date = CURRENT_DATE() - 1;
        """
        
        return sql_template

# Configuration and usage example
def load_config() -> Dict:
    """Load configuration from JSON file or environment variables"""
    return {
        'pagespeed_api_key': 'AIzaSyB5eEwvDY-6yvjB4Pdz_qOgkNyduZlC9fU',
        'bigquery': {
            'project_id': 'printerpix-general',
            'dataset_id': 'GA_CG',
            'table_id': 'core_web_vitals',
            'credentials_path': r'C:\Users\charlottegong\Downloads\json_json.json'

        },
        'canonical_urls': [
            'https://www.printerpix.com/photo-blankets/custom-mink-photo-blanket/',
            'https://www.printerpix.com/photo-books-q/',
            'https://www.printerpix.com/canvas-prints/v1/',
            'https://www.printerpix.com/photo-calendars/personalized-photo-calendars-v1/',
            'https://www.printerpix.com/photo-prints/photo-frame-prints/',
            'https://www.printerpix.com/photo-mugs/magic-mugs/'
        ],
        'critical_urls': [  # Subset for daily monitoring
            'https://printerpix.com/'
        ],
        'device_types': ['mobile', 'desktop'],
        'request_delay_seconds': 2
    }

if __name__ == "__main__":
    # Load configuration
    config = load_config()
    
    # Initialize monitoring system
    monitoring_system = CWVMonitoringSystem(config)
    
    # Setup (create BigQuery table)
    monitoring_system.setup()
    
    # Option 1: Run one-time collection
    success = monitoring_system.collect_and_store_cwv_data()
    if success:
        print("CWV data collection completed successfully")
    
    # Option 2: Run scheduled collection (uncomment to use)
    # monitoring_system.run_scheduled_collection()
    
    # Generate Looker SQL for dashboard creation
    looker_sql = monitoring_system.generate_looker_sql()
    with open('cwv_looker_dashboard.sql', 'w') as f:
        f.write(looker_sql)
    
    print("Looker SQL generated: cwv_looker_dashboard.sql")