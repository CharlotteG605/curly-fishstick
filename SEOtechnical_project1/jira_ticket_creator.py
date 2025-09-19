"""
Jira Ticket Creator for SEO Audit Results
Automatically creates Jira tickets for each team based on technical SEO audit findings
"""

import json
import requests
from datetime import datetime
from typing import Dict, List
import base64
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JiraTicketCreator:
    def __init__(self, jira_config: Dict):
        """
        Initialize Jira API client
        
        Args:
            jira_config: Dictionary containing Jira configuration
                {
                    'base_url': 'https://yourcompany.atlassian.net',
                    'username': 'your-email@company.com',
                    'api_token': 'YOUR_JIRA_API_TOKEN_HERE',  # Leave placeholder for user
                    'project_key': 'SEO',  # Your Jira project key
                }
        """
        self.base_url = jira_config['base_url'].rstrip('/')
        self.username = jira_config['username']
        self.api_token = jira_config['api_token']
        self.project_key = jira_config['project_key']
        
        # Setup authentication
        self.auth_string = base64.b64encode(f"{self.username}:{self.api_token}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {self.auth_string}',
            'Content-Type': 'application/json'
        }
        
        # Team assignment mapping
        self.team_labels = {
            'tech_team': ['tech-team', 'seo-tech', 'backend'],
            'marketing_team': ['marketing-team', 'seo-content', 'content'],
            'design_team': ['design-team', 'ux-team', 'frontend']
        }
        
        # Priority mapping based on severity
        self.severity_to_priority = {
            'Critical': 'Highest',
            'High': 'High', 
            'Medium': 'Medium',
            'Low': 'Low'
        }
    
    def test_connection(self) -> bool:
        """Test Jira API connection"""
        try:
            url = f"{self.base_url}/rest/api/3/myself"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                user_info = response.json()
                logger.info(f"✅ Connected to Jira as {user_info.get('displayName', self.username)}")
                return True
            else:
                logger.error(f"❌ Jira connection failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Jira connection error: {e}")
            return False
    
    def load_audit_results(self, file_path: str = 'technical_seo_audit_results.json') -> Dict:
        """Load SEO audit results from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading audit results: {e}")
            return {}
    
    def create_epic_ticket(self, audit_results: Dict) -> str:
        """Create an Epic ticket for the overall SEO audit"""
        try:
            audit_date = audit_results.get('audit_timestamp', datetime.now().isoformat())[:10]
            total_issues = audit_results.get('summary', {}).get('total_issues', 0)
            seo_score = audit_results.get('summary', {}).get('seo_score', 0)
            
            epic_data = {
                "fields": {
                    "project": {"key": self.project_key},
                    "summary": f"SEO Audit Results - {audit_date} ({total_issues} Issues Found)",
                    "description": self._format_epic_description(audit_results),
                    "issuetype": {"name": "Epic"},
                    "priority": {"name": self._get_overall_priority(audit_results)},
                    "labels": ["seo-audit", f"score-{int(seo_score)}", audit_date.replace('-', '')],
                    "customfield_10011": f"SEO-AUDIT-{audit_date}",  # Epic Name field (may need adjustment)
                }
            }
            
            url = f"{self.base_url}/rest/api/3/issue"
            response = requests.post(url, headers=self.headers, json=epic_data, timeout=30)
            
            if response.status_code == 201:
                epic_key = response.json()['key']
                logger.info(f"✅ Created Epic: {epic_key}")
                return epic_key
            else:
                logger.error(f"❌ Epic creation failed: {response.status_code} - {response.text}")
                return ""
                
        except Exception as e:
            logger.error(f"Error creating Epic: {e}")
            return ""
    
    def create_team_tickets(self, audit_results: Dict, epic_key: str = "") -> Dict[str, str]:
        """Create individual tickets for each team"""
        team_breakdown = audit_results.get('summary', {}).get('team_breakdown', {})
        created_tickets = {}
        
        for team_key, team_data in team_breakdown.items():
            if team_data.get('total_issues', 0) > 0:
                ticket_key = self._create_single_team_ticket(team_key, team_data, audit_results, epic_key)
                if ticket_key:
                    created_tickets[team_key] = ticket_key
        
        return created_tickets
    
    def _create_single_team_ticket(self, team_key: str, team_data: Dict, audit_results: Dict, epic_key: str) -> str:
        """Create a single team ticket with all their issues"""
        try:
            audit_date = audit_results.get('audit_timestamp', datetime.now().isoformat())[:10]
            team_name = team_data.get('name', team_key.upper())
            total_issues = team_data.get('total_issues', 0)
            critical_issues = team_data.get('critical_issues', 0)
            high_issues = team_data.get('high_issues', 0)
            
            # Determine priority based on critical/high issues
            if critical_issues > 0:
                priority = 'Highest'
            elif high_issues > 0:
                priority = 'High'
            else:
                priority = 'Medium'
            
            ticket_data = {
                "fields": {
                    "project": {"key": self.project_key},
                    "summary": f"{team_name}: {total_issues} SEO Issues - {audit_date}",
                    "description": self._format_team_description(team_key, team_data, audit_results),
                    "issuetype": {"name": "Task"},
                    "priority": {"name": priority},
                    "labels": self.team_labels.get(team_key, []) + ["seo-audit", audit_date.replace('-', '')],
                }
            }
            
            # Link to Epic if provided
            if epic_key:
                ticket_data["fields"]["parent"] = {"key": epic_key}
            
            url = f"{self.base_url}/rest/api/3/issue"
            response = requests.post(url, headers=self.headers, json=ticket_data, timeout=30)
            
            if response.status_code == 201:
                ticket_key = response.json()['key']
                logger.info(f"✅ Created ticket for {team_name}: {ticket_key}")
                return ticket_key
            else:
                logger.error(f"❌ Ticket creation failed for {team_name}: {response.status_code} - {response.text}")
                return ""
                
        except Exception as e:
            logger.error(f"Error creating ticket for {team_key}: {e}")
            return ""
    
    def _format_epic_description(self, audit_results: Dict) -> str:
        """Format Epic description with overall audit summary"""
        summary = audit_results.get('summary', {})
        audit_date = audit_results.get('audit_timestamp', '')[:10]
        
        description = f"""# 🔍 SEO Audit Results - {audit_date}

## 📊 Overall Health Score
**SEO Score: {summary.get('seo_score', 0):.1f}/100**

## 🚨 Issues Breakdown
* **Critical Issues:** {summary.get('critical_issues', 0)}
* **High Issues:** {summary.get('high_issues', 0)} 
* **Medium Issues:** {summary.get('medium_issues', 0)}
* **Low Issues:** {summary.get('low_issues', 0)}
* **Total Issues:** {summary.get('total_issues', 0)}

## ⚡ Performance Metrics
* **Average Response Time:** {summary.get('avg_response_time', 0):.2f}s
* **Pages with Errors:** {summary.get('error_pages', 0)}
* **Total URLs Audited:** {audit_results.get('total_urls_audited', 0)}

## 🎯 Strategic Insights
{self._format_strategic_insights(summary.get('strategic_insights', {}))}

## 💼 Team Assignments
{self._format_team_summary(summary.get('team_breakdown', {}))}

---
*This audit covers multiple domains and focuses on business-critical SEO factors.*
*Individual team tickets contain detailed issue breakdowns and recommendations.*
"""
        return description
    
    def _format_team_description(self, team_key: str, team_data: Dict, audit_results: Dict) -> str:
        """Format team-specific ticket description"""
        team_name = team_data.get('name', team_key.upper())
        description = team_data.get('description', '')
        
        ticket_description = f"""# {team_name} - SEO Issues

## 📝 Team Responsibility
{description}

## 📊 Issue Summary
* **Total Issues:** {team_data.get('total_issues', 0)}
* **Critical:** {team_data.get('critical_issues', 0)}
* **High:** {team_data.get('high_issues', 0)}
* **Average Impact Score:** {team_data.get('avg_impact', 0):.1f}
* **Priority Score:** {team_data.get('priority_score', 0)}

## 🔥 Issues to Fix

{self._format_team_issues(team_data.get('issues', []))}

## 📋 Action Items
{self._generate_action_items(team_key, team_data)}

---
**Next Steps:**
1. Review and prioritize issues by impact score
2. Assign individual issues to team members
3. Set target completion dates
4. Update progress in this ticket
"""
        return ticket_description
    
    def _format_team_issues(self, issues: List[Dict]) -> str:
        """Format list of issues for team tickets"""
        if not issues:
            return "*No issues found for this team.*"
        
        # Sort by impact score (highest first)
        sorted_issues = sorted(issues, key=lambda x: x.get('impact_score', 0), reverse=True)
        
        formatted_issues = ""
        for i, issue in enumerate(sorted_issues, 1):
            severity_emoji = {
                'Critical': '🚨',
                'High': '⚠️',
                'Medium': '🟡',
                'Low': '🔵'
            }.get(issue.get('severity', 'Medium'), '🔵')
            
            formatted_issues += f"""
### {i}. {severity_emoji} {issue.get('issue_type', 'Unknown Issue')} ({issue.get('severity', 'Medium')})

**URL:** `{issue.get('url', 'Unknown URL')}`
**Impact Score:** {issue.get('impact_score', 0)}/100
**Category:** {issue.get('category', 'General')}

**Issue:** {issue.get('description', 'No description')}

**Recommendation:** {issue.get('recommendation', 'No recommendation provided')}

---
"""
        
        return formatted_issues
    
    def _generate_action_items(self, team_key: str, team_data: Dict) -> str:
        """Generate specific action items for each team"""
        action_items = {
            'tech_team': [
                "🔧 Review server response times and fix HTTP errors",
                "🔗 Implement proper canonical URL tags",
                "📱 Add missing structured data (schema markup)",
                "🤖 Review robots.txt and meta robots settings",
                "⚡ Optimize page load times and resource loading"
            ],
            'marketing_team': [
                "✍️ Rewrite generic or duplicate title tags",
                "📝 Create unique, compelling meta descriptions",
                "📄 Audit thin content and expand where needed",
                "🎯 Optimize high-impression pages with poor CTR",
                "📊 Analyze GSC performance data for optimization opportunities"
            ],
            'design_team': [
                "🖼️ Add alt text to all images",
                "📱 Review mobile usability and responsive design",
                "🎨 Improve user experience on high-traffic pages",
                "🔍 Optimize visual hierarchy and content layout",
                "⚡ Compress images and optimize visual assets"
            ]
        }
        
        team_actions = action_items.get(team_key, ["Review and fix assigned SEO issues"])
        return "\n".join([f"- {action}" for action in team_actions])
    
    def _format_strategic_insights(self, insights: Dict) -> str:
        """Format strategic insights section"""
        if not insights:
            return "*No strategic insights available.*"
        
        insight_text = ""
        if insights.get('high_priority_pages_with_issues', 0) > 0:
            insight_text += f"* 🎯 **{insights['high_priority_pages_with_issues']} high-priority pages** have SEO issues\n"
        
        critical_business = insights.get('critical_business_impact', [])
        if critical_business:
            insight_text += f"* 🚨 **{len(critical_business)} pages** have critical business impact issues\n"
        
        return insight_text or "*All pages performing within acceptable parameters.*"
    
    def _format_team_summary(self, team_breakdown: Dict) -> str:
        """Format team breakdown summary"""
        if not team_breakdown:
            return "*No team breakdown available.*"
        
        summary = ""
        for team_key, team_data in team_breakdown.items():
            if team_data.get('total_issues', 0) > 0:
                urgency = "🚨 URGENT" if team_data.get('critical_issues', 0) > 0 else "⚠️ HIGH PRIORITY" if team_data.get('high_issues', 0) > 0 else "🟡 MEDIUM PRIORITY"
                summary += f"* **{team_data.get('name', team_key)}**: {urgency} - {team_data.get('total_issues', 0)} issues\n"
        
        return summary
    
    def _get_overall_priority(self, audit_results: Dict) -> str:
        """Determine overall audit priority based on critical issues"""
        summary = audit_results.get('summary', {})
        critical_issues = summary.get('critical_issues', 0)
        high_issues = summary.get('high_issues', 0)
        
        if critical_issues > 5:
            return 'Highest'
        elif critical_issues > 0 or high_issues > 10:
            return 'High'
        elif high_issues > 0:
            return 'Medium'
        else:
            return 'Low'
    
    def create_all_tickets(self, audit_file_path: str = 'technical_seo_audit_results.json') -> Dict:
        """Main method to create all Jira tickets from audit results"""
        logger.info("🎫 Starting Jira ticket creation process...")
        
        # Test connection first
        if not self.test_connection():
            logger.error("❌ Cannot proceed without valid Jira connection")
            return {}
        
        # Load audit results
        audit_results = self.load_audit_results(audit_file_path)
        if not audit_results:
            logger.error("❌ No audit results found")
            return {}
        
        logger.info(f"📊 Loaded audit with {audit_results.get('summary', {}).get('total_issues', 0)} total issues")
        
        # Create Epic ticket
        epic_key = self.create_epic_ticket(audit_results)
        
        # Create team tickets
        team_tickets = self.create_team_tickets(audit_results, epic_key)
        
        # Summary
        results = {
            'epic_key': epic_key,
            'team_tickets': team_tickets,
            'total_tickets_created': len(team_tickets) + (1 if epic_key else 0)
        }
        
        logger.info(f"🎉 Successfully created {results['total_tickets_created']} Jira tickets")
        if epic_key:
            logger.info(f"📋 Epic: {epic_key}")
        for team, ticket in team_tickets.items():
            logger.info(f"🎫 {team}: {ticket}")
        
        return results


# Configuration and usage
def load_jira_config() -> Dict:
    """Load Jira configuration - UPDATE WITH YOUR DETAILS"""
    return {
        'base_url': 'https://yourcompany.atlassian.net',  # Your Jira instance URL
        'username': 'your-email@company.com',  # Your Jira email
        'api_token': 'YOUR_JIRA_API_TOKEN_HERE',  # ⚠️  ADD YOUR API TOKEN HERE
        'project_key': 'SEO',  # Your Jira project key for SEO tasks
    }


if __name__ == "__main__":
    print("🎫 SEO Audit Jira Ticket Creator")
    print("=" * 50)
    
    # Load configuration
    jira_config = load_jira_config()
    
    # Check if API token is configured
    if jira_config['api_token'] == 'YOUR_JIRA_API_TOKEN_HERE':
        print("⚠️  Please configure your Jira API token in the load_jira_config() function")
        print("📖 Instructions:")
        print("   1. Go to https://id.atlassian.com/manage-profile/security/api-tokens")
        print("   2. Create a new API token")
        print("   3. Replace 'YOUR_JIRA_API_TOKEN_HERE' with your actual token")
        print("   4. Update the base_url, username, and project_key as well")
        exit(1)
    
    # Initialize ticket creator
    creator = JiraTicketCreator(jira_config)
    
    # Create tickets
    results = creator.create_all_tickets('technical_seo_audit_results.json')
    
    print("\n" + "=" * 50)
    print("✅ Jira ticket creation completed!")
    print(f"📊 Total tickets created: {results.get('total_tickets_created', 0)}")
    
    if results.get('epic_key'):
        print(f"🎯 Epic created: {jira_config['base_url']}/browse/{results['epic_key']}")
    
    for team, ticket_key in results.get('team_tickets', {}).items():
        print(f"🎫 {team}: {jira_config['base_url']}/browse/{ticket_key}")