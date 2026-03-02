#!/usr/bin/env python3
"""
Buddyliko Standards Library Scraper
====================================
Scrapes standards metadata, schema URLs, sample files from:
- GitHub repos (phax/*, OpenPEPPOL/*, hapifhir/*, etc.)
- HL7.org
- OASIS docs
- UN/CEFACT
- X12.org (limited - requires subscription for full access)

Run this script periodically to update the CSV seed files.

Usage:
    python standards_scraper.py --output ./csv/  [--github-token YOUR_TOKEN]
    python standards_scraper.py --check-releases  # check for new versions
    python standards_scraper.py --download-samples ./samples/

Requirements:
    pip install requests httpx lxml bs4
"""

import os
import sys
import json
import time
import logging
import argparse
import csv
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_API   = 'https://api.github.com'
RATE_LIMIT_PAUSE = 1.5  # seconds between API calls (unauthenticated: 60/hr, authenticated: 5000/hr)


# ─── GITHUB REPOS TO SCRAPE ────────────────────────────────────────────────

HELGER_REPOS = [
    # (repo, description, standard_slug)
    ('phax/ph-ubl',              'UBL 2.x Java library',           'ubl-2-1'),
    ('phax/ph-cii',              'CII D16B Java library',           'cii-d16b'),
    ('phax/ph-schematron',       'Schematron validation engine',    'ph-schematron'),
    ('phax/phive',               'Business document validation',    'phive'),
    ('phax/phive-rules',         'Validation rules (Peppol, etc.)', 'phive-rules'),
    ('phax/phase4',              'AS4/ebMS messaging',              'phase4'),
    ('phax/peppol-commons',      'Peppol infrastructure libs',      'peppol-commons'),
    ('phax/ph-ebinterface',      'Austrian ebInterface',            'ph-ebinterface'),
    ('phax/en16931-cii-validation', 'EN 16931 CII validation',     'en-16931'),
]

PEPPOL_REPOS = [
    ('OpenPEPPOL/peppol-bis-invoice-3', 'Peppol BIS 3.0 Invoice rules', 'peppol-bis-3'),
    ('OpenPEPPOL/peppol-bis-billing-3', 'Peppol BIS Billing 3',        'peppol-bis-3'),
]

FHIR_REPOS = [
    ('hapifhir/hapi-fhir', 'HAPI FHIR Reference Implementation', 'hl7-fhir-r4'),
]

ZUGFERD_REPOS = [
    ('ZUGFeRD/mustangproject', 'ZUGFeRD/Factur-X Java library', 'zugferd'),
]

ALL_REPOS = HELGER_REPOS + PEPPOL_REPOS + FHIR_REPOS + ZUGFERD_REPOS


# ─── STATIC URL REGISTRY ────────────────────────────────────────────────────
# Direct sample/schema URLs (for when API access is limited)

STATIC_RESOURCES = {
    'ubl-2-1': {
        'schema_url':     'https://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/',
        'sample_urls': [
            'https://docs.oasis-open.org/ubl/os-UBL-2.1/xml/UBL-Invoice-2.1-Example.xml',
            'https://docs.oasis-open.org/ubl/os-UBL-2.1/xml/UBL-CreditNote-2.1-Example.xml',
            'https://docs.oasis-open.org/ubl/os-UBL-2.1/xml/UBL-Order-2.1-Example.xml',
        ],
        'spec_url': 'https://docs.oasis-open.org/ubl/UBL-2.1.html',
    },
    'ubl-2-3': {
        'schema_url': 'https://docs.oasis-open.org/ubl/os-UBL-2.3/xsd/',
        'spec_url':   'https://docs.oasis-open.org/ubl/UBL-2.3.html',
        'sample_urls': ['https://docs.oasis-open.org/ubl/os-UBL-2.3/xml/'],
    },
    'peppol-bis-3': {
        'spec_url':       'https://docs.peppol.eu/poacc/billing/3.0/',
        'schematron_url': 'https://github.com/OpenPEPPOL/peppol-bis-invoice-3/tree/master/rules',
        'sample_urls': [
            'https://docs.peppol.eu/poacc/billing/3.0/examples/T10-ubl.xml',
            'https://docs.peppol.eu/poacc/billing/3.0/examples/T14-ubl.xml',
        ],
    },
    'en-16931': {
        'schematron_url': 'https://github.com/ConnectingEurope/eInvoicing-EN16931',
        'spec_url': 'https://standards.cen.eu/',
    },
    'fatturapa-1-2': {
        'schema_url':  'https://www.fatturapa.gov.it/export/documenti/fatturapa/v1.2.2/Schema_del_file_xml_FatturaPA_v1.2.2.xsd',
        'spec_url':    'https://www.fatturapa.gov.it/it/norme-e-regole/documentazione-fatturapa/',
        'sample_urls': ['https://www.fatturapa.gov.it/export/documenti/fatturapa/v1.2.2/IT01234567890_FPR12.xml'],
    },
    'xrechnung': {
        'spec_url':       'https://xeinkauf.de/xrechnung/',
        'schematron_url': 'https://github.com/itplr-kosit/xrechnung-schematron',
        'sample_urls':    ['https://raw.githubusercontent.com/itplr-kosit/xrechnung-testsuite/master/src/test/resources/'],
    },
    'zugferd': {
        'spec_url': 'https://www.ferd-net.de/standards/zugferd-2.3/',
        'sample_urls': ['https://github.com/ZUGFeRD/mustangproject/tree/master/validator/src/test/resources'],
    },
    'cii-d16b': {
        'schema_url': 'https://unece.org/sites/default/files/2023-03/D22B_SCRDM__Subset__CII_-_Cross_Industry_Invoice.zip',
        'spec_url':   'https://unece.org/trade/uncefact/xml-schemas',
    },
    'hl7-fhir-r4': {
        'spec_url':    'https://hl7.org/fhir/R4/',
        'schema_url':  'https://hl7.org/fhir/R4/fhir.schema.json',
        'sample_urls': ['https://hl7.org/fhir/R4/examples.zip'],
    },
    'hl7-fhir-r5': {
        'spec_url':   'https://hl7.org/fhir/R5/',
        'schema_url': 'https://hl7.org/fhir/R5/fhir.schema.json',
        'sample_urls': ['https://hl7.org/fhir/R5/examples.zip'],
    },
    'iso-20022': {
        'spec_url':    'https://www.iso20022.org/',
        'schema_url':  'https://www.iso20022.org/catalogue-messages',
    },
    'sepa-sct': {
        'spec_url': 'https://www.europeanpaymentscouncil.eu/what-we-do/sepa-credit-transfer',
    },
}


# ─── GITHUB API HELPERS ──────────────────────────────────────────────────────

def github_get(endpoint: str) -> Optional[Dict]:
    """Make a GitHub API call with rate limiting."""
    url = f"{GITHUB_API}{endpoint}"
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/vnd.github.v3+json')
    if GITHUB_TOKEN:
        req.add_header('Authorization', f'token {GITHUB_TOKEN}')
    try:
        time.sleep(RATE_LIMIT_PAUSE)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 403:
            log.warning(f"GitHub rate limit hit. Use --github-token. URL: {url}")
        elif e.code == 404:
            log.debug(f"Not found: {url}")
        else:
            log.error(f"HTTP {e.code} for {url}")
    except Exception as e:
        log.error(f"Error fetching {url}: {e}")
    return None


def get_repo_info(repo: str) -> Optional[Dict]:
    """Get repository metadata from GitHub."""
    data = github_get(f'/repos/{repo}')
    if not data:
        return None
    return {
        'name':        data.get('name'),
        'description': data.get('description', ''),
        'stars':       data.get('stargazers_count', 0),
        'updated_at':  data.get('updated_at', ''),
        'topics':      data.get('topics', []),
        'homepage':    data.get('homepage', ''),
        'html_url':    data.get('html_url', ''),
        'license':     (data.get('license') or {}).get('spdx_id', ''),
    }


def get_latest_release(repo: str) -> Optional[str]:
    """Get latest release tag for a repo."""
    data = github_get(f'/repos/{repo}/releases/latest')
    if data:
        return data.get('tag_name', '')
    # fallback to tags
    tags = github_get(f'/repos/{repo}/tags')
    if tags and len(tags) > 0:
        return tags[0].get('name', '')
    return None


def get_repo_file_url(repo: str, path: str, branch: str = 'master') -> str:
    """Get raw GitHub URL for a file."""
    return f'https://raw.githubusercontent.com/{repo}/{branch}/{path}'


def find_sample_files(repo: str, extensions: List[str] = None, max_files: int = 3) -> List[str]:
    """Find sample XML/EDI/JSON files in a GitHub repo."""
    if extensions is None:
        extensions = ['.xml', '.json', '.edi', '.txt']
    
    # Try common sample directories
    sample_dirs = ['samples', 'examples', 'test', 'tests', 'src/test/resources', 'testfiles']
    found = []
    
    for d in sample_dirs:
        contents = github_get(f'/repos/{repo}/contents/{d}')
        if not contents or not isinstance(contents, list):
            continue
        for item in contents:
            if item.get('type') == 'file':
                name = item.get('name', '')
                if any(name.endswith(ext) for ext in extensions):
                    found.append(item.get('download_url') or item.get('html_url', ''))
                    if len(found) >= max_files:
                        break
        if found:
            break
    
    return found


# ─── SCRAPER FUNCTIONS ───────────────────────────────────────────────────────

def scrape_repo_data(repo: str, standard_slug: str) -> Dict:
    """Scrape all relevant data for a standard from a GitHub repo."""
    log.info(f"Scraping {repo} for {standard_slug}...")
    
    result = {
        'slug':         standard_slug,
        'github_url':   f'https://github.com/{repo}',
        'github_stars': 0,
        'latest_version': '',
        'last_updated': '',
        'sample_urls':  [],
    }
    
    # Repo info
    info = get_repo_info(repo)
    if info:
        result['github_stars'] = info['stars']
        result['last_updated'] = info['updated_at'][:10] if info['updated_at'] else ''
        log.info(f"  ★ {info['stars']} stars, updated {result['last_updated']}")
    
    # Latest release
    version = get_latest_release(repo)
    if version:
        result['latest_version'] = version
        log.info(f"  Latest release: {version}")
    
    # Sample files
    samples = find_sample_files(repo)
    result['sample_urls'] = samples
    log.info(f"  Found {len(samples)} sample files")
    
    return result


def build_release_report(repos: List[Tuple]) -> List[Dict]:
    """Check all repos for latest versions and return report."""
    report = []
    for repo, desc, slug in repos:
        version = get_latest_release(repo)
        report.append({
            'slug':    slug,
            'repo':    repo,
            'version': version or 'unknown',
            'desc':    desc,
        })
    return report


def download_sample(url: str, dest_dir: Path, slug: str, idx: int = 0) -> Optional[Path]:
    """Download a sample file to dest_dir."""
    ext_map = {'xml': '.xml', 'json': '.json', 'edi': '.edi', 'txt': '.txt', 'zip': '.zip'}
    ext = '.' + url.split('.')[-1].lower() if '.' in url.split('/')[-1] else '.xml'
    
    filename = f"{slug}_sample_{idx+1}{ext}"
    dest = dest_dir / filename
    
    try:
        log.info(f"  Downloading {url[:80]}...")
        urllib.request.urlretrieve(url, dest)
        log.info(f"  → Saved to {dest}")
        return dest
    except Exception as e:
        log.error(f"  Download failed: {e}")
        return None


# ─── CSV MERGER ─────────────────────────────────────────────────────────────

def merge_scraped_into_csv(scraped_data: Dict[str, Dict], csv_path: Path) -> None:
    """Merge scraped data (versions, sample URLs) into existing CSV."""
    if not csv_path.exists():
        log.warning(f"CSV not found: {csv_path}")
        return
    
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            slug = row.get('slug', '')
            if slug in scraped_data:
                scraped = scraped_data[slug]
                # Update fields from scraped data
                if scraped.get('latest_version') and not row.get('version'):
                    row['version'] = scraped['latest_version']
                if scraped.get('github_url') and not row.get('github_url'):
                    row['github_url'] = scraped['github_url']
                if scraped.get('sample_urls') and not row.get('sample_url'):
                    row['sample_url'] = scraped['sample_urls'][0] if scraped['sample_urls'] else ''
            rows.append(row)
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    log.info(f"Updated {csv_path}")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Buddyliko Standards Library Scraper')
    parser.add_argument('--output',          default='./csv',     help='Output directory for CSV files')
    parser.add_argument('--samples',         default='./samples', help='Output directory for sample files')
    parser.add_argument('--github-token',    default='',          help='GitHub personal access token')
    parser.add_argument('--check-releases',  action='store_true', help='Check latest versions only')
    parser.add_argument('--download-samples',action='store_true', help='Download sample files')
    parser.add_argument('--slug',            default='',          help='Scrape only specific standard slug')
    args = parser.parse_args()
    
    global GITHUB_TOKEN
    if args.github_token:
        GITHUB_TOKEN = args.github_token
    
    output_dir  = Path(args.output)
    samples_dir = Path(args.samples)
    output_dir.mkdir(exist_ok=True)
    samples_dir.mkdir(exist_ok=True)
    
    repos_to_scrape = ALL_REPOS
    if args.slug:
        repos_to_scrape = [(r, d, s) for r, d, s in ALL_REPOS if s == args.slug]
    
    # ── Check releases mode ──────────────────────────────────────────────────
    if args.check_releases:
        log.info("=== Checking latest releases ===")
        report = build_release_report(repos_to_scrape)
        print("\n{:<30} {:<20} {:<15} {}".format('Slug', 'Version', 'Repo', 'Description'))
        print("-" * 100)
        for r in report:
            print("{:<30} {:<20} {:<15} {}".format(r['slug'], r['version'], r['repo'].split('/')[-1], r['desc'][:50]))
        
        # Save report
        report_path = output_dir / f"versions_report_{datetime.now().strftime('%Y%m%d')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        log.info(f"\nReport saved to {report_path}")
        return
    
    # ── Full scrape ──────────────────────────────────────────────────────────
    log.info("=== Starting full standards scrape ===")
    
    if not GITHUB_TOKEN:
        log.warning("No GitHub token provided. Rate limited to 60 req/hr. Use --github-token for 5000/hr.")
    
    all_scraped: Dict[str, Dict] = {}
    
    for repo, desc, slug in repos_to_scrape:
        try:
            data = scrape_repo_data(repo, slug)
            all_scraped[slug] = data
        except Exception as e:
            log.error(f"Failed to scrape {repo}: {e}")
    
    # Merge scraped versions/URLs into CSV files
    for csv_file in output_dir.glob('*.csv'):
        merge_scraped_into_csv(all_scraped, csv_file)
    
    # Save full scraped data as JSON
    scraped_json = output_dir / f"scraped_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(scraped_json, 'w') as f:
        json.dump(all_scraped, f, indent=2, default=str)
    log.info(f"\nScraped data saved to {scraped_json}")
    
    # ── Download samples ─────────────────────────────────────────────────────
    if args.download_samples:
        log.info("\n=== Downloading sample files ===")
        
        # From scraped data
        for slug, data in all_scraped.items():
            for i, url in enumerate(data.get('sample_urls', [])[:2]):
                if url.startswith('http'):
                    download_sample(url, samples_dir, slug, i)
        
        # From static registry
        for slug, resources in STATIC_RESOURCES.items():
            for i, url in enumerate(resources.get('sample_urls', [])[:1]):
                dest = samples_dir / f"{slug}_sample_official_{i+1}.xml"
                if not dest.exists() and url.startswith('http'):
                    download_sample(url, samples_dir, f"{slug}_official", i)
    
    log.info("\n=== Scrape complete ===")
    log.info(f"Standards scraped: {len(all_scraped)}")
    log.info(f"Output: {output_dir}")


if __name__ == '__main__':
    main()
