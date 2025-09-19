# Technical SEO Audit System v3

A comprehensive automated technical SEO monitoring system for PrinterPix that combines Google Search Console data, web crawling, and performance analysis to identify and prioritize SEO issues across multiple domains.

## 🎯 Overview

This system performs automated technical SEO audits by:
- Fetching data from Google Search Console for multiple domains
- Crawling websites to analyze technical SEO elements
- Gathering PageSpeed Insights for performance metrics
- Validating structured data markup
- Identifying and prioritizing issues by business impact
- Generating actionable reports organized by team responsibility

## ✨ Features

### Multi-Domain Support
- **PrinterPix Domains**: US, UK, FR, DE, IT, ES, NL
- **GSC Integration**: Fetches top pages by impressions from each domain
- **Concurrent Processing**: Handles multiple domains efficiently

### Comprehensive Analysis
- **HTTP Status Monitoring**: Identifies errors, redirects, and broken pages
- **Performance Metrics**: Response times, page sizes, Core Web Vitals
- **Content Quality**: Title tags, meta descriptions, H1 tags, alt text
- **Technical SEO**: Canonical URLs, robots meta, internal linking
- **Structured Data**: Schema markup validation and recommendations
- **Mobile Usability**: Mobile-specific issue detection

### Strategic Prioritization
- **Business Impact Scoring**: Weighs issues by page importance (homepage, product pages, etc.)
- **Page Type Classification**: Different scoring for different page types
- **GSC Performance Integration**: Correlates technical issues with actual search performance
- **Team-Based Organization**: Groups issues by responsible team (Tech, Marketing, Design)

## 🛠️ Setup & Configuration

### Prerequisites
- Python 3.7+
- Google Search Console access
- Google Cloud Project with APIs enabled
- BigQuery dataset for storing results

### Required APIs
1. **Google Search Console API**
2. **PageSpeed Insights API**
3. **BigQuery API**

### Installation

1. **Install Dependencies**:
```bash
pip install google-cloud-bigquery google-auth google-auth-oauthlib google-auth-httplib2
pip install requests pandas beautifulsoup4 lxml
```

2. **Set Up Credentials**:
   - Download service account JSON from Google Cloud Console
   - Place at: `C:\Users\charlottegong\Downloads\json_json.json`
   - Ensure the service account has access to your GSC properties

3. **Configure BigQuery**:
   - Project ID: `printerpix-general`
   - Dataset: `GA_CG`
   - The system auto-creates the `technical_seo_audit` table

### Configuration

Update the configuration in `load_audit_config()`:

```python
def load_audit_config():
    return {
        'gsc_credentials_path': r'C:\Users\charlottegong\Downloads\json_json.json',
        'pagespeed_api_key': 'YOUR_PAGESPEED_API_KEY',
        'bigquery': {
            'project_id': 'printerpix-general',
            'dataset_id': 'GA_CG',
            'credentials_path': r'C:\Users\charlottegong\Downloads\json_json.json'
        },
        'max_crawl_workers': 5,
        'crawl_timeout': 30,
        'audit_schedule': 'weekly'
    }
```

## 🚀 Usage

### Basic Usage

```python
# Initialize the auditor
config = load_audit_config()
auditor = TechnicalSEOAuditor(config)

# Get URLs from BigQuery
urls_to_audit = auditor.get_urls_to_audit_from_bigquery(limit=100)

# Define GSC properties
gsc_properties = [
    'sc-domain:printerpix.com',
    'sc-domain:printerpix.co.uk',
    'sc-domain:printerpix.fr',
    # ... other domains
]

# Run comprehensive audit
results = auditor.run_comprehensive_audit(gsc_properties, urls_to_audit)
```

### Command Line Usage

```bash
python technical_seo_audit_system_v3.py
```

## 📊 Output & Reports

### Console Output
The system provides a detailed console report with:
- **Overall SEO Health Score** (0-100 with letter grade)
- **Issue Breakdown** by severity (Critical, High, Medium, Low)
- **Team Assignments** with priority scores
- **Strategic Insights** for high-value pages
- **Action Items** organized by responsible team

### JSON Export
Detailed results are saved to `technical_seo_audit_results.json` with:
- All identified issues with descriptions and recommendations
- Team-specific issue exports
- Performance metrics and GSC data
- Strategic insights and business impact analysis

### BigQuery Storage
All audit results are automatically stored in BigQuery for:
- Historical tracking of issues
- Trend analysis over time
- Integration with other analytics tools
- Custom reporting and dashboards

## 🎯 Issue Classification

### Severity Levels
- **Critical**: Site-breaking issues (HTTP 5xx, noindex pages, etc.)
- **High**: Major ranking impact (missing titles, thin content, etc.)
- **Medium**: Moderate optimization opportunities
- **Low**: Minor improvements

### Team Organization
Issues are automatically assigned to responsible teams:

#### 🧑‍💻 Tech/Dev Team
- Server errors and infrastructure issues
- Technical implementation (canonical tags, robots meta)
- Structured data and schema markup
- Internal linking and crawlability

#### 📈 Marketing Team
- Content optimization (titles, meta descriptions)
- SEO strategy and keyword optimization
- GSC performance issues (low CTR, poor positions)
- Content quality and user experience

#### 🎨 Design/UX Team
- Mobile usability issues
- Image optimization and alt text
- Visual layout and accessibility
- User interface improvements

## 📈 Strategic Features

### Business Impact Scoring
The system weighs issues by page importance:
- **Homepage**: 5.0x multiplier
- **Product Pages**: 4.5x multiplier
- **Checkout/Pricing**: 4.8x multiplier
- **Category Pages**: 4.0x multiplier
- **Other Pages**: 2.0x multiplier

### GSC Performance Integration
Correlates technical issues with search performance:
- **High Impressions, Zero Clicks**: Critical business impact
- **High Traffic Pages with Issues**: Urgent fixes needed
- **Low CTR on High Impression Pages**: Title/meta optimization opportunities

### URL Source Management
Intelligently sources URLs from:
1. **BigQuery CanonicalURLMapping**: Primary source for comprehensive coverage
2. **Fallback URLs**: Default high-priority pages if BigQuery unavailable
3. **Multi-domain Support**: Handles all PrinterPix regional domains

## 🔧 Customization

### Adding New Issue Types
Extend the `_analyze_issues()` method:

```python
# Example: Add new issue type
if some_condition:
    issues.append(TechnicalSEOIssue(
        url=url,
        issue_type='Your New Issue Type',
        severity='High',
        category='Technical SEO',
        description='Description of the issue',
        recommendation='How to fix it',
        date_detected=current_time,
        status='New',
        impact_score=strategic_impact
    ))
```

### Modifying Page Type Classification
Update the `classify_page_type()` method in `StrategicSEOScorer`:

```python
def classify_page_type(self, url: str) -> str:
    url_lower = url.lower()

    # Add your custom page type logic
    if 'your-custom-pattern' in url_lower:
        return 'custom_page_type'

    # Existing logic...
```

### Custom Scoring Weights
Modify issue severity weights in `StrategicSEOScorer.__init__()`:

```python
self.issue_severity = {
    'Your Custom Issue': 85,  # Score 0-100
    # Existing issues...
}
```

## 📅 Automation

### Scheduled Runs
Set up automated audits using:
- **Cron jobs** (Linux/Mac)
- **Task Scheduler** (Windows)
- **GitHub Actions** (CI/CD)
- **Google Cloud Scheduler** (Cloud-based)

Example cron job for weekly audits:
```bash
0 9 * * 1 cd /path/to/project && python technical_seo_audit_system_v3.py
```

## 🔍 Troubleshooting

### Common Issues

**GSC API Access Denied**:
- Verify service account has access to GSC properties
- Check that GSC properties are correctly formatted (`sc-domain:example.com`)

**BigQuery Connection Failed**:
- Ensure service account has BigQuery permissions
- Verify project ID and dataset ID are correct

**Crawling Timeouts**:
- Reduce `max_crawl_workers` in config
- Increase `crawl_timeout` for slow sites

**Missing PageSpeed Data**:
- Verify PageSpeed Insights API key is valid
- Check API quota limits

### Debug Mode
Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📝 Contributing

When contributing new features:
1. Follow the existing code structure
2. Add appropriate error handling
3. Update the strategic scoring if needed
4. Test with multiple domains
5. Document new configuration options

## 📄 License

This is a proprietary system for PrinterPix internal use.

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section
2. Review BigQuery logs for data issues
3. Verify GSC access permissions
4. Contact the development team for technical support

---

**Last Updated**: December 2024
**Version**: 3.0
**Compatible with**: PrinterPix multi-domain setup