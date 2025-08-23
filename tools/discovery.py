#!/usr/bin/env python3
"""
System Discovery & Inventory Script

This script generates a comprehensive, up-to-date inventory of the trading system
for precise fixes and improvements. It produces a detailed Markdown report containing:
- Repository layout and runtime configuration
- CI/CD workflows analysis
- Environment configuration
- DigitalOcean App spec and deployment status
- Spaces bucket structure and freshness
- Market data provider wiring
- Strategy and screener inventory
- Risk assessment and recommended fixes

The script safely redacts all sensitive information while providing actionable insights.
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import yaml

try:
    import boto3
    from jinja2 import Template
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install: pip install boto3 jinja2")
    sys.exit(1)


class SystemDiscovery:
    """Main class for system discovery and inventory generation."""
    
    def __init__(self):
        self.repo_root = Path(__file__).parent.parent
        self.discovery_artifacts_dir = self.repo_root / "discovery_artifacts"
        self.discovery_artifacts_dir.mkdir(exist_ok=True)
        
        # Git information
        self.commit_sha = self._get_git_commit()
        self.branch_name = self._get_git_branch()
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Discovery results storage
        self.results = {}
        
    def _get_git_commit(self) -> str:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.stdout.strip()[:8] if result.returncode == 0 else "unknown"
        except Exception:
            return "unknown"
    
    def _get_git_branch(self) -> str:
        """Get current git branch name."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.stdout.strip() if result.returncode == 0 else "unknown"
        except Exception:
            return "unknown"
    
    def _redact_sensitive(self, value: str, show_chars: int = 4) -> str:
        """Safely redact sensitive information while keeping useful info."""
        if not value or len(value) <= show_chars * 2:
            return "REDACTED"
        return f"{value[:show_chars]}...{value[-show_chars:]} (redacted)"
    
    def discover_repo_architecture(self) -> Dict[str, Any]:
        """Analyze repository architecture and runtime configuration."""
        print("🔍 Discovering repository architecture...")
        
        results = {
            "python_version": self._discover_python_version(),
            "entry_points": self._discover_entry_points(),
            "key_directories": self._analyze_directory_structure(),
            "scheduling": self._analyze_scheduling()
        }
        
        return results
    
    def _discover_python_version(self) -> Dict[str, str]:
        """Determine Python version from various sources."""
        sources = {}
        
        # Check .python-version
        python_version_file = self.repo_root / ".python-version"
        if python_version_file.exists():
            sources["python_version_file"] = python_version_file.read_text().strip()
        
        # Check pyproject.toml
        pyproject_file = self.repo_root / "pyproject.toml"
        if pyproject_file.exists():
            try:
                try:
                    import tomllib
                    with open(pyproject_file, 'rb') as f:
                        pyproject_data = tomllib.load(f)
                        if 'project' in pyproject_data and 'requires-python' in pyproject_data['project']:
                            sources["pyproject_toml"] = pyproject_data['project']['requires-python']
                except ImportError:
                    # Fall back to simple regex parsing for Python < 3.11
                    content = pyproject_file.read_text()
                    match = re.search(r'requires-python\s*=\s*["\']([^"\']+)["\']', content)
                    if match:
                        sources["pyproject_toml"] = match.group(1)
            except Exception:
                # Final fallback
                content = pyproject_file.read_text()
                match = re.search(r'requires-python\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    sources["pyproject_toml"] = match.group(1)
        
        # Check runtime.txt
        runtime_file = self.repo_root / "runtime.txt"
        if runtime_file.exists():
            sources["runtime_txt"] = runtime_file.read_text().strip()
        
        # Check workflows for Python versions
        workflows_dir = self.repo_root / ".github" / "workflows"
        if workflows_dir.exists():
            for workflow_file in workflows_dir.glob("*.yml"):
                try:
                    with open(workflow_file) as f:
                        workflow_data = yaml.safe_load(f)
                        if 'jobs' in workflow_data:
                            for job_name, job_data in workflow_data['jobs'].items():
                                if 'strategy' in job_data and 'matrix' in job_data['strategy']:
                                    matrix = job_data['strategy']['matrix']
                                    if 'python-version' in matrix:
                                        versions = matrix['python-version']
                                        sources[f"workflow_{workflow_file.name}"] = str(versions)
                except Exception:
                    continue
        
        # Determine final resolution
        final_version = "UNKNOWN"
        if "runtime_txt" in sources:
            final_version = sources["runtime_txt"] + " (DigitalOcean runtime.txt)"
        elif "python_version_file" in sources:
            final_version = sources["python_version_file"] + " (.python-version file)"
        elif "pyproject_toml" in sources:
            final_version = sources["pyproject_toml"] + " (pyproject.toml)"
        
        sources["final_resolution"] = final_version
        return sources
    
    def _discover_entry_points(self) -> Dict[str, str]:
        """Find application entry points and start commands."""
        entry_points = {}
        
        # Check for Procfile
        procfile = self.repo_root / "Procfile"
        if procfile.exists():
            entry_points["Procfile"] = procfile.read_text().strip()
        
        # Check for Dockerfile
        dockerfile = self.repo_root / "Dockerfile"
        if dockerfile.exists():
            content = dockerfile.read_text()
            cmd_match = re.search(r'CMD\s+\[(.*?)\]', content, re.DOTALL)
            if cmd_match:
                entry_points["Dockerfile_CMD"] = cmd_match.group(1)
            entrypoint_match = re.search(r'ENTRYPOINT\s+\[(.*?)\]', content, re.DOTALL)
            if entrypoint_match:
                entry_points["Dockerfile_ENTRYPOINT"] = entrypoint_match.group(1)
        
        # Check orchestrator main entry point
        orchestrator_main = self.repo_root / "orchestrator" / "run_all.py"
        if orchestrator_main.exists():
            entry_points["orchestrator"] = "python orchestrator/run_all.py"
        
        return entry_points
    
    def _analyze_directory_structure(self) -> Dict[str, str]:
        """Analyze key directories and their purposes."""
        directories = {}
        
        key_dirs = [
            "notebooks", "ticker_selectors", "jobs", "screeners", 
            "dashboard", "orchestrator", "utils", "data", "journal",
            "tools", "docs", "tests"
        ]
        
        for dir_name in key_dirs:
            dir_path = self.repo_root / dir_name
            if dir_path.exists():
                files = list(dir_path.rglob("*.py"))
                directories[dir_name] = f"{len(files)} Python files"
                
                # Add specific descriptions for known directories
                if dir_name == "orchestrator":
                    directories[dir_name] += " - Master scheduling and runtime controller"
                elif dir_name == "screeners":
                    py_files = [f.stem for f in dir_path.glob("*.py") if f.stem != "__init__"]
                    directories[dir_name] += f" - Strategies: {', '.join(py_files)}"
                elif dir_name == "jobs":
                    directories[dir_name] += " - Data fetching and processing"
                elif dir_name == "dashboard":
                    directories[dir_name] += " - Signal consolidation and trade planning"
                elif dir_name == "utils":
                    directories[dir_name] += " - Shared utilities and configuration"
        
        return directories
    
    def _analyze_scheduling(self) -> Dict[str, Any]:
        """Analyze orchestrator scheduling configuration."""
        scheduling = {"jobs": [], "frequencies": {}}
        
        orchestrator_file = self.repo_root / "orchestrator" / "run_all.py"
        if orchestrator_file.exists():
            content = orchestrator_file.read_text()
            
            # Look for schedule usage
            schedule_matches = re.findall(r'schedule\.every\(.*?\)\.([^.]+)\.do\(([^)]+)\)', content)
            for frequency, job in schedule_matches:
                scheduling["jobs"].append(f"{job} - {frequency}")
                scheduling["frequencies"][frequency] = scheduling["frequencies"].get(frequency, 0) + 1
        
        schedules_file = self.repo_root / "orchestrator" / "schedules.py"
        if schedules_file.exists():
            content = schedules_file.read_text()
            # Extract schedule information from schedules.py
            function_matches = re.findall(r'def (run_.*?)\(', content)
            scheduling["orchestrator_functions"] = function_matches
        
        return scheduling
    
    def discover_ci_cd_workflows(self) -> Dict[str, Any]:
        """Analyze GitHub Actions workflows."""
        print("🔍 Discovering CI/CD workflows...")
        
        workflows_dir = self.repo_root / ".github" / "workflows"
        workflows = []
        
        if not workflows_dir.exists():
            return {"workflows": [], "last_runs": "No .github/workflows directory found"}
        
        for workflow_file in workflows_dir.glob("*.yml"):
            try:
                with open(workflow_file) as f:
                    workflow_data = yaml.safe_load(f)
                
                workflow_info = {
                    "name": workflow_data.get("name", workflow_file.stem),
                    "file": workflow_file.name,
                    "triggers": self._extract_workflow_triggers(workflow_data),
                    "purpose": self._determine_workflow_purpose(workflow_data, workflow_file.name),
                    "needs_secrets": self._extract_required_secrets(workflow_data)
                }
                workflows.append(workflow_info)
                
            except Exception as e:
                workflows.append({
                    "name": workflow_file.name,
                    "file": workflow_file.name,
                    "error": f"Failed to parse: {str(e)}"
                })
        
        return {
            "workflows": workflows,
            "last_runs": "GitHub API access not available in this context"
        }
    
    def _extract_workflow_triggers(self, workflow_data: Dict) -> List[str]:
        """Extract workflow triggers from workflow YAML."""
        triggers = []
        if 'on' in workflow_data:
            on_data = workflow_data['on']
            if isinstance(on_data, dict):
                triggers.extend(on_data.keys())
            elif isinstance(on_data, list):
                triggers.extend(on_data)
            else:
                triggers.append(str(on_data))
        return triggers
    
    def _determine_workflow_purpose(self, workflow_data: Dict, filename: str) -> str:
        """Determine the purpose of a workflow."""
        name = workflow_data.get("name", "").lower()
        filename = filename.lower()
        
        if "ci" in name or "test" in name:
            return "CI - Testing and validation"
        elif "deploy" in name or "deploy" in filename:
            return "Deploy - Application deployment"
        elif "discovery" in name or "inventory" in name:
            return "Ops - System discovery and inventory"
        elif any(word in name for word in ["repair", "fix", "health", "check"]):
            return "Ops - Maintenance and repair"
        else:
            return "Ops - Utility workflow"
    
    def _extract_required_secrets(self, workflow_data: Dict) -> List[str]:
        """Extract required secrets from workflow."""
        secrets = set()
        workflow_str = yaml.dump(workflow_data)
        secret_matches = re.findall(r'secrets\.([A-Z_][A-Z0-9_]*)', workflow_str)
        secrets.update(secret_matches)
        return sorted(list(secrets))
    
    def discover_environment_variables(self) -> Dict[str, Any]:
        """Analyze environment variables and secrets usage."""
        print("🔍 Discovering environment variables...")
        
        env_vars = {}
        
        # Scan Python files for environment variable usage
        for py_file in self.repo_root.rglob("*.py"):
            try:
                content = py_file.read_text()
                
                # Find os.getenv() calls
                getenv_matches = re.findall(r'os\.getenv\(["\']([^"\']+)["\'](?:,\s*["\']([^"\']*)["\'])?\)', content)
                for var_name, default_value in getenv_matches:
                    if var_name not in env_vars:
                        env_vars[var_name] = {
                            "used_in": [],
                            "default": default_value or None,
                            "required": default_value == "" or default_value is None
                        }
                    env_vars[var_name]["used_in"].append(str(py_file.relative_to(self.repo_root)))
                
                # Find os.environ[] access
                environ_matches = re.findall(r'os\.environ\[["\']([^"\']+)["\']\]', content)
                for var_name in environ_matches:
                    if var_name not in env_vars:
                        env_vars[var_name] = {"used_in": [], "default": None, "required": True}
                    env_vars[var_name]["used_in"].append(str(py_file.relative_to(self.repo_root)))
                
            except Exception:
                continue
        
        # Check GitHub secrets/variables availability (placeholder - would need API access)
        github_secrets = "API access not available"
        
        return {
            "environment_variables": env_vars,
            "github_secrets_status": github_secrets
        }
    
    def discover_digitalocean_app(self) -> Dict[str, Any]:
        """Fetch DigitalOcean App information."""
        print("🔍 Discovering DigitalOcean App configuration...")
        
        do_app_id = os.getenv("DO_APP_ID")
        do_api_token = os.getenv("DO_API_TOKEN")
        
        if not do_app_id or not do_api_token:
            return {
                "error": "Missing DO_APP_ID or DO_API_TOKEN",
                "app_id": "MISSING PERMISSION",
                "components": [],
                "deployments": []
            }
        
        try:
            # Use doctl to fetch app spec
            result = subprocess.run(
                ["doctl", "apps", "spec", "get", do_app_id],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                app_spec = json.loads(result.stdout)
                
                # Save raw app spec
                with open(self.discovery_artifacts_dir / "do_app_spec.json", "w") as f:
                    json.dump(app_spec, f, indent=2)
                
                # Extract key information
                components = []
                if "services" in app_spec:
                    for service in app_spec["services"]:
                        components.append({
                            "name": service.get("name"),
                            "type": "service",
                            "runtime": service.get("environment_slug", "UNKNOWN"),
                            "port": service.get("http_port", "UNKNOWN"),
                            "source_branch": service.get("source", {}).get("branch", "UNKNOWN")
                        })
                
                if "workers" in app_spec:
                    for worker in app_spec["workers"]:
                        components.append({
                            "name": worker.get("name"),
                            "type": "worker",
                            "runtime": worker.get("environment_slug", "UNKNOWN"),
                            "source_branch": worker.get("source", {}).get("branch", "UNKNOWN")
                        })
                
                return {
                    "app_id": self._redact_sensitive(do_app_id),
                    "components": components,
                    "app_spec_saved": "do_app_spec.json",
                    "deployments": "Use doctl apps list-deployments for history"
                }
            else:
                return {
                    "error": f"doctl command failed: {result.stderr}",
                    "app_id": self._redact_sensitive(do_app_id)
                }
                
        except Exception as e:
            return {
                "error": f"Failed to fetch DigitalOcean app info: {str(e)}",
                "app_id": self._redact_sensitive(do_app_id) if do_app_id else "MISSING"
            }
    
    def discover_spaces_structure(self) -> Dict[str, Any]:
        """Analyze DigitalOcean Spaces structure and freshness."""
        print("🔍 Discovering Spaces structure...")
        
        access_key = os.getenv("SPACES_ACCESS_KEY_ID")
        secret_key = os.getenv("SPACES_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("SPACES_BUCKET_NAME")
        region = os.getenv("SPACES_REGION", "nyc3")
        endpoint = os.getenv("SPACES_ENDPOINT")
        base_prefix = os.getenv("SPACES_BASE_PREFIX", "")
        
        if not all([access_key, secret_key, bucket_name]):
            return {
                "error": "Missing Spaces credentials",
                "bucket": "MISSING PERMISSION",
                "structure": {}
            }
        
        try:
            # Create S3 client for Spaces
            s3_client = boto3.client(
                's3',
                region_name=region,
                endpoint_url=f"https://{region}.digitaloceanspaces.com" if not endpoint else endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            
            # List objects with prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=bucket_name,
                Prefix=base_prefix,
                MaxKeys=1000  # Limit for discovery
            )
            
            structure = {}
            sample_tickers = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN"]
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        size = obj['Size']
                        modified = obj['LastModified']
                        
                        # Analyze key structure
                        parts = key.split('/')
                        if len(parts) >= 2:
                            root_prefix = parts[0]
                            if root_prefix not in structure:
                                structure[root_prefix] = {"count": 0, "sample_files": [], "latest_modified": None}
                            
                            structure[root_prefix]["count"] += 1
                            
                            # Track latest modification
                            if (structure[root_prefix]["latest_modified"] is None or 
                                modified > structure[root_prefix]["latest_modified"]):
                                structure[root_prefix]["latest_modified"] = modified
                            
                            # Sample files for key paths
                            if len(structure[root_prefix]["sample_files"]) < 5:
                                structure[root_prefix]["sample_files"].append({
                                    "key": key,
                                    "size": size,
                                    "modified": modified.isoformat()
                                })
            
            return {
                "bucket": bucket_name,
                "endpoint": endpoint or f"https://{region}.digitaloceanspaces.com",
                "region": region,
                "base_prefix": base_prefix,
                "structure": structure,
                "analysis_limited": "First 1000 objects only"
            }
            
        except Exception as e:
            return {
                "error": f"Failed to analyze Spaces: {str(e)}",
                "bucket": bucket_name if bucket_name else "MISSING"
            }
    
    def discover_market_data_provider(self) -> Dict[str, Any]:
        """Analyze market data provider configuration."""
        print("🔍 Discovering market data provider...")
        
        providers = {}
        
        # Check utils/providers/ directory
        providers_dir = self.repo_root / "utils" / "providers"
        if providers_dir.exists():
            for provider_file in providers_dir.glob("*.py"):
                if provider_file.name == "__init__.py":
                    continue
                    
                try:
                    content = provider_file.read_text()
                    
                    # Extract endpoint information
                    endpoint_matches = re.findall(r'base_url.*?=.*?["\']([^"\']+)["\']', content)
                    token_matches = re.findall(r'token.*?=.*?os\.getenv\(["\']([^"\']+)["\']', content)
                    
                    providers[provider_file.stem] = {
                        "endpoints": endpoint_matches,
                        "token_env_vars": token_matches,
                        "file": str(provider_file.relative_to(self.repo_root))
                    }
                except Exception:
                    continue
        
        # Check config.py for provider settings
        config_file = self.repo_root / "utils" / "config.py"
        provider_config = {}
        if config_file.exists():
            content = config_file.read_text()
            # Look for provider-related configuration
            provider_matches = re.findall(r'(MARKETDATA_[A-Z_]+|PROVIDER_[A-Z_]+|ALPHA_VANTAGE_[A-Z_]+)', content)
            provider_config["config_vars"] = list(set(provider_matches))
        
        return {
            "providers": providers,
            "configuration": provider_config
        }
    
    def discover_strategies(self) -> Dict[str, Any]:
        """Map trading strategies and screeners."""
        print("🔍 Discovering strategies and screeners...")
        
        strategies = {}
        
        # Analyze screeners directory
        screeners_dir = self.repo_root / "screeners"
        if screeners_dir.exists():
            for screener_file in screeners_dir.glob("*.py"):
                if screener_file.name == "__init__.py":
                    continue
                
                try:
                    content = screener_file.read_text()
                    
                    # Look for output file patterns
                    output_matches = re.findall(r'["\']([^"\']*signals[^"\']*\.csv)["\']', content)
                    
                    strategies[screener_file.stem] = {
                        "file": str(screener_file.relative_to(self.repo_root)),
                        "outputs": output_matches,
                        "description": self._get_strategy_description(screener_file.stem)
                    }
                except Exception:
                    strategies[screener_file.stem] = {
                        "file": str(screener_file.relative_to(self.repo_root)),
                        "error": "Failed to analyze"
                    }
        
        # Check dashboard for consolidation logic
        dashboard_info = {}
        dashboard_file = self.repo_root / "dashboard" / "master_dashboard.py"
        if dashboard_file.exists():
            try:
                content = dashboard_file.read_text()
                if "r_multiple" in content.lower() or "R_multiple" in content:
                    dashboard_info["risk_management"] = "R-multiple calculation found"
                if "entry" in content.lower() and ("stop" in content.lower() or "target" in content.lower()):
                    dashboard_info["trade_planning"] = "Entry/Stop/Target logic found"
            except Exception:
                dashboard_info["error"] = "Failed to analyze dashboard"
        
        return {
            "screeners": strategies,
            "dashboard": dashboard_info
        }
    
    def _get_strategy_description(self, strategy_name: str) -> str:
        """Get description for known strategies."""
        descriptions = {
            "gapgo": "Gap & Go - Pre-market gap detection and breakout confirmation",
            "orb": "Opening Range Breakout - First 30min range breakout strategy",
            "avwap_reclaim": "AVWAP Reclaim - Price reclaim of AVWAP anchor levels",
            "breakout": "Breakout - Support/resistance level breakouts",
            "ema_pullback": "EMA Pullback - Moving average pullback entries",
            "exhaustion_reversal": "Exhaustion Reversal - Overextended reversal patterns"
        }
        return descriptions.get(strategy_name, "Custom strategy")
    
    def assess_risks_and_fixes(self) -> List[Dict[str, str]]:
        """Identify risks and provide recommended fixes."""
        print("🔍 Assessing risks and recommended fixes...")
        
        risks = []
        
        # Check Python version consistency
        python_versions = self.results.get("repo_architecture", {}).get("python_version", {})
        if len([v for v in python_versions.values() if v and "unknown" not in v.lower()]) > 2:
            risks.append({
                "issue": "Inconsistent Python versions",
                "evidence": f"Multiple version sources: {list(python_versions.keys())}",
                "impact": "Deployment and development environment mismatches",
                "recommended_fix": "Add runtime.txt with single Python version for DigitalOcean",
                "owner": "Code"
            })
        
        # Check for missing environment variables
        env_vars = self.results.get("environment_variables", {}).get("environment_variables", {})
        required_vars = [var for var, info in env_vars.items() if info.get("required", False)]
        if required_vars:
            risks.append({
                "issue": "Required environment variables without defaults",
                "evidence": f"{len(required_vars)} variables: {', '.join(required_vars[:5])}{'...' if len(required_vars) > 5 else ''}",
                "impact": "Runtime failures in production",
                "recommended_fix": "Add validation in config.py or provide defaults",
                "owner": "Code"
            })
        
        # Check DigitalOcean configuration
        do_config = self.results.get("digitalocean_app", {})
        if "error" in do_config:
            risks.append({
                "issue": "DigitalOcean API access limited",
                "evidence": do_config["error"],
                "impact": "Cannot validate deployment configuration",
                "recommended_fix": "Verify DO_API_TOKEN and DO_APP_ID in GitHub Secrets",
                "owner": "GitHub Actions"
            })
        
        # Check Spaces configuration
        spaces_config = self.results.get("spaces_structure", {})
        if "error" in spaces_config:
            risks.append({
                "issue": "Spaces connectivity issues",
                "evidence": spaces_config["error"],
                "impact": "Data storage and retrieval failures",
                "recommended_fix": "Verify Spaces credentials in GitHub Secrets",
                "owner": "GitHub Actions"
            })
        
        return risks
    
    def generate_report(self) -> str:
        """Generate the comprehensive discovery report."""
        print("📝 Generating discovery report...")
        
        report_template = """# System Discovery & Inventory — {{ repo_name }}@{{ commit_sha }} on {{ timestamp }}

**Quick Start**: Run [Validate Environment](../../actions/workflows/validate-env.yml) first to confirm vars and connectivity; then run System Discovery & Inventory (manual) for the full report.

## 1) Repo Architecture & Runtime

### Python Version Resolution
{% for source, version in repo_architecture.python_version.items() %}
- **{{ source }}**: {{ version }}
{% endfor %}

**Final Resolution**: {{ repo_architecture.python_version.final_resolution }}

### Start/Entry Commands
{% for name, command in repo_architecture.entry_points.items() %}
- **{{ name }}**: {{ command }}
{% endfor %}

### Key Directories
{% for dir_name, description in repo_architecture.key_directories.items() %}
- **{{ dir_name }}/**: {{ description }}
{% endfor %}

### Scheduling Information
{% if repo_architecture.scheduling.jobs %}
**Scheduled Jobs**:
{% for job in repo_architecture.scheduling.jobs %}
- {{ job }}
{% endfor %}
{% endif %}

{% if repo_architecture.scheduling.orchestrator_functions %}
**Orchestrator Functions**:
{% for func in repo_architecture.scheduling.orchestrator_functions %}
- {{ func }}()
{% endfor %}
{% endif %}

## 2) CI/CD Workflows (GitHub Actions)

| Workflow | Triggers | Purpose | Needs Secrets | File |
|----------|----------|---------|---------------|------|
{% for workflow in ci_cd_workflows.workflows %}
| {{ workflow.name }} | {{ workflow.triggers | join(", ") }} | {{ workflow.purpose }} | {{ workflow.needs_secrets | join(", ") or "None" }} | {{ workflow.file }} |
{% endfor %}

**Last Runs**: {{ ci_cd_workflows.last_runs }}

## 3) Environment & Secrets (names only, redacted values)

| Name | Used In (files) | Required? | Default | 
|------|----------------|-----------|---------|
{% for var_name, var_info in environment_variables.environment_variables.items() %}
| `{{ var_name }}` | {{ var_info.used_in | length }} files | {{ "✅ Yes" if var_info.required else "❌ No" }} | {{ var_info.default or "None" }} |
{% endfor %}

**GitHub Secrets Status**: {{ environment_variables.github_secrets_status }}

## 4) DigitalOcean App Inventory

{% if digitalocean_app.error %}
**Error**: {{ digitalocean_app.error }}
{% else %}
- **App ID**: {{ digitalocean_app.app_id }}
- **Components**:
{% for component in digitalocean_app.components %}
  - **{{ component.name }}**: {{ component.type }}, runtime={{ component.runtime }}, port={{ component.port }}, branch={{ component.source_branch }}
{% endfor %}
- **App Spec**: Saved as {{ digitalocean_app.app_spec_saved }}
- **Deployments**: {{ digitalocean_app.deployments }}
{% endif %}

## 5) Spaces (S3) Structure & Freshness

{% if spaces_structure.error %}
**Error**: {{ spaces_structure.error }}
{% else %}
- **Bucket**: {{ spaces_structure.bucket }} @ {{ spaces_structure.endpoint }}/{{ spaces_structure.region }}
- **Base Prefix**: {{ spaces_structure.base_prefix }}
- **Structure Analysis**:
{% for prefix, info in spaces_structure.structure.items() %}
  - **{{ prefix }}/**: {{ info.count }} objects, latest: {{ info.latest_modified.strftime('%Y-%m-%d %H:%M UTC') if info.latest_modified else 'Unknown' }}
{% endfor %}
- **Note**: {{ spaces_structure.analysis_limited }}
{% endif %}

## 6) Market Data Provider Wiring

### Configured Providers
{% for provider_name, provider_info in market_data_provider.providers.items() %}
- **{{ provider_name }}**:
  - File: {{ provider_info.file }}
  - Endpoints: {{ provider_info.endpoints | join(", ") or "None found" }}
  - Token Variables: {{ provider_info.token_env_vars | join(", ") or "None found" }}
{% endfor %}

### Configuration Variables
{{ market_data_provider.configuration.config_vars | join(", ") or "None found" }}

## 7) Strategies & Trade Plan Outputs

### Screeners Present
{% for screener_name, screener_info in strategies.screeners.items() %}
- **{{ screener_name }}**: {{ screener_info.description if screener_info.description else "Unknown strategy" }}
  - File: {{ screener_info.file }}
  - Outputs: {{ screener_info.outputs | join(", ") or "No CSV outputs detected" }}
{% endfor %}

### Dashboard Consolidation
{% for feature, status in strategies.dashboard.items() %}
- **{{ feature }}**: {{ status }}
{% endfor %}

## 8) Risks & Recommended Fixes

| Issue | Evidence | Impact | Recommended Fix | Owner |
|-------|----------|--------|----------------|-------|
{% for risk in risks_and_fixes %}
| {{ risk.issue }} | {{ risk.evidence }} | {{ risk.impact }} | {{ risk.recommended_fix }} | {{ risk.owner }} |
{% endfor %}

## Appendix

### Generated Files
- **DO App Spec**: {{ digitalocean_app.app_spec_saved if digitalocean_app.app_spec_saved else "Not available" }}
- **Discovery Artifacts**: All files saved in `discovery_artifacts/` directory

### Analysis Metadata
- **Repository**: {{ repo_name }}
- **Branch**: {{ branch_name }}
- **Commit**: {{ commit_sha }}
- **Analysis Time**: {{ timestamp }}
- **Discovery Script**: tools/discovery.py

---
*This report was generated automatically by the System Discovery & Inventory workflow.*
*For manual execution: `python tools/discovery.py`*
"""
        
        template = Template(report_template)
        
        return template.render(
            repo_name="Tradingstation",
            commit_sha=self.commit_sha,
            branch_name=self.branch_name,
            timestamp=self.timestamp,
            repo_architecture=self.results["repo_architecture"],
            ci_cd_workflows=self.results["ci_cd_workflows"],
            environment_variables=self.results["environment_variables"],
            digitalocean_app=self.results["digitalocean_app"],
            spaces_structure=self.results["spaces_structure"],
            market_data_provider=self.results["market_data_provider"],
            strategies=self.results["strategies"],
            risks_and_fixes=self.results["risks_and_fixes"]
        )
    
    def run_discovery(self) -> None:
        """Run the complete system discovery process."""
        print(f"🚀 Starting System Discovery & Inventory for {self.repo_root.name}")
        print(f"📍 Branch: {self.branch_name}, Commit: {self.commit_sha}")
        print(f"⏰ Timestamp: {self.timestamp}")
        print()
        
        # Run all discovery phases
        self.results["repo_architecture"] = self.discover_repo_architecture()
        self.results["ci_cd_workflows"] = self.discover_ci_cd_workflows()
        self.results["environment_variables"] = self.discover_environment_variables()
        self.results["digitalocean_app"] = self.discover_digitalocean_app()
        self.results["spaces_structure"] = self.discover_spaces_structure()
        self.results["market_data_provider"] = self.discover_market_data_provider()
        self.results["strategies"] = self.discover_strategies()
        self.results["risks_and_fixes"] = self.assess_risks_and_fixes()
        
        # Generate and save report
        report_content = self.generate_report()
        
        # Save main report
        report_file = self.repo_root / "DISCOVERY_REPORT.md"
        with open(report_file, "w") as f:
            f.write(report_content)
        
        # Save copy in artifacts directory
        artifacts_report = self.discovery_artifacts_dir / "DISCOVERY_REPORT.md"
        with open(artifacts_report, "w") as f:
            f.write(report_content)
        
        # Save raw results as JSON
        results_file = self.discovery_artifacts_dir / "discovery_results.json"
        with open(results_file, "w") as f:
            # Make results JSON-serializable
            serializable_results = json.loads(json.dumps(self.results, default=str))
            json.dump(serializable_results, f, indent=2)
        
        print()
        print("✅ Discovery completed successfully!")
        print(f"📄 Main report: DISCOVERY_REPORT.md")
        print(f"📦 Artifacts directory: discovery_artifacts/")
        print(f"🔍 Raw data: discovery_artifacts/discovery_results.json")
        
        if "do_app_spec.json" in [f.name for f in self.discovery_artifacts_dir.iterdir()]:
            print(f"🔧 DigitalOcean App Spec: discovery_artifacts/do_app_spec.json")


def main():
    """Main entry point for the discovery script."""
    try:
        discovery = SystemDiscovery()
        discovery.run_discovery()
        return 0
    except Exception as e:
        print(f"❌ Discovery failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())