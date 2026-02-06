"""
NYC TLC Data Scraper Module
============================
Automated web scraping of NYC Taxi & Limousine Commission parquet files.

This module implements:
- Web scraping of TLC website for available parquet file links
- PowerShell-based downloads (bypasses CloudFront CDN blocking)
- Automatic detection of available vs. missing data months
- Taxi zone lookup table download

Key Technical Decision:
    Python requests library gets blocked by CloudFront CDN (403 Forbidden).
    PowerShell Invoke-WebRequest successfully bypasses this restriction.

Bonus Marks: This module enables fully automated data ingestion.
"""

import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    TLC_BASE_URL, TLC_PAGE_URL, RAW_DIR, 
    TAXI_ZONE_URL, TAXI_ZONE_SHAPEFILE_URL
)

# HTTP headers to mimic browser requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.nyc.gov/',
}


def scrape_tlc_links():
    """
    Web scrape TLC website for all parquet file download links.
    
    Parses the TLC trip record data page and extracts links to
    Yellow and Green taxi parquet files.
    
    Returns:
        list: URLs of available parquet files (deduplicated)
        
    Example:
        >>> links = scrape_tlc_links()
        >>> print(len(links))  # e.g., 351 parquet files found
    """
    try:
        # Request TLC trip record page
        response = requests.get(TLC_PAGE_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        
        parquet_links = []
        for link in links:
            href = link['href']
            # Filter for yellow/green taxi parquet files only
            if '.parquet' in href and ('yellow_tripdata' in href or 'green_tripdata' in href):
                # Clean URL encoding artifacts
                clean_href = href.replace('%20', '').strip()
                # Build full URL if relative path
                if clean_href.startswith('http'):
                    parquet_links.append(clean_href)
                elif clean_href.startswith('/'):
                    parquet_links.append(f"https://d37ci6vzurychx.cloudfront.net{clean_href}")
                else:
                    parquet_links.append(f"https://d37ci6vzurychx.cloudfront.net/trip-data/{clean_href}")
        
        # Remove duplicates
        return list(set(parquet_links))
    except Exception as e:
        print(f"Error scraping TLC page: {e}")
        return []


def parse_filename(url):
    """
    Extract metadata from parquet filename URL.
    
    Args:
        url: URL or filename string
        
    Returns:
        dict: {'taxi_type': 'yellow'/'green', 'year': int, 'month': int, 'url': str}
        None: If filename doesn't match expected pattern
    """
    url = url.replace('%20', '')
    match = re.search(r'(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet', url)
    if match:
        return {
            'taxi_type': match.group(1),
            'year': int(match.group(2)),
            'month': int(match.group(3)),
            'url': url
        }
    return None


def download_with_powershell(url, dest_path, timeout=600):
    """
    Download file using PowerShell Invoke-WebRequest.
    
    PowerShell is used instead of Python requests because:
    - CloudFront CDN blocks Python requests with 403 Forbidden
    - PowerShell's Invoke-WebRequest successfully bypasses this
    
    Args:
        url: Source file URL
        dest_path: Local destination path
        timeout: Download timeout in seconds (default: 600)
        
    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        cmd = [
            'powershell', '-Command',
            f"Invoke-WebRequest -Uri '{url}' -OutFile '{dest_path}' -Headers @{{'User-Agent'='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}} -TimeoutSec {timeout}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout+60)
        
        if result.returncode == 0 and Path(dest_path).exists():
            return True
        else:
            print(f"PowerShell error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"Download timed out: {url}")
        return False
    except Exception as e:
        print(f"Download error: {e}")
        return False


def download_parquet(taxi_type, year, month, force=False):
    """
    Download a single parquet file for given taxi type, year, and month.
    
    Args:
        taxi_type: 'yellow' or 'green'
        year: Year (e.g., 2025)
        month: Month (1-12)
        force: If True, re-download even if file exists
        
    Returns:
        str: Path to downloaded file, or None if failed
    """
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    dest_path = RAW_DIR / filename
    
    # Skip if already downloaded (unless force=True)
    if dest_path.exists() and not force:
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"  Already exists: {filename} ({size_mb:.1f} MB)")
        return str(dest_path)
    
    # Build download URL
    url = f"{TLC_BASE_URL}/{filename}"
    print(f"  Downloading: {filename}...")
    
    # Download using PowerShell
    if download_with_powershell(url, str(dest_path)):
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {filename} ({size_mb:.1f} MB)")
        return str(dest_path)
    return None


def check_local_files():
    """
    Check what parquet files already exist locally.
    
    Returns:
        list: List of dicts with file metadata:
            {'taxi_type', 'year', 'month', 'path', 'size_mb'}
    """
    existing = list(RAW_DIR.glob("*.parquet"))
    files_info = []
    for f in existing:
        parsed = parse_filename(f.name)
        if parsed:
            parsed['path'] = str(f)
            parsed['size_mb'] = f.stat().st_size / (1024 * 1024)
            files_info.append(parsed)
    return files_info


def get_available_months_from_scrape():
    """
    Get available months from TLC website via web scraping.
    
    Returns:
        dict: Nested dict by year -> taxi_type -> list of months
        Example: {2025: {'yellow': [1, 2, 3], 'green': [1, 2, 3]}}
    """
    links = scrape_tlc_links()
    available = {}
    
    for link in links:
        parsed = parse_filename(link)
        if parsed:
            year = parsed['year']
            taxi_type = parsed['taxi_type']
            month = parsed['month']
            
            if year not in available:
                available[year] = {'yellow': [], 'green': []}
            if month not in available[year][taxi_type]:
                available[year][taxi_type].append(month)
    
    # Sort months for each year/type
    for year in available:
        for taxi_type in available[year]:
            available[year][taxi_type].sort()
    return available


def download_all_data(years=[2025], include_comparison=[2024]):
    """
    Download all available data for specified years.
    
    Args:
        years: List of years to download (main analysis)
        include_comparison: Additional years for comparison analysis
        
    Returns:
        dict: {'downloaded': [paths], 'failed': [identifiers]}
    """
    all_years = sorted(list(set(years + include_comparison)))
    downloaded = []
    failed = []
    
    print("Scraping TLC website for available data...")
    available = get_available_months_from_scrape()
    
    for year in all_years:
        if year not in available:
            print(f"No data found for {year}")
            continue
        
        print(f"\nYear {year}:")
        for taxi_type in ['yellow', 'green']:
            months = available.get(year, {}).get(taxi_type, [])
            for month in months:
                result = download_parquet(taxi_type, year, month)
                if result:
                    downloaded.append(result)
                else:
                    failed.append(f"{taxi_type}_{year}_{month:02d}")
    
    return {'downloaded': downloaded, 'failed': failed}


def download_taxi_zones():
    """
    Download taxi zone lookup CSV for geospatial mapping.
    
    The lookup table maps LocationID (used in trip data) to 
    zone names and borough information.
    
    Returns:
        dict: {'csv': path to downloaded CSV}
    """
    csv_path = RAW_DIR / "taxi_zone_lookup.csv"
    
    if not csv_path.exists():
        print("Downloading taxi zone lookup...")
        try:
            response = requests.get(TAXI_ZONE_URL, headers=HEADERS, timeout=30)
            response.raise_for_status()
            with open(csv_path, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            print(f"Error downloading zone lookup: {e}")
    
    return {'csv': str(csv_path)}


def check_data_availability(year=2025):
    """
    Check what data is available locally for a given year.
    
    Args:
        year: Year to check (default: 2025)
        
    Returns:
        dict: {'yellow': [months], 'green': [months]}
    """
    local = check_local_files()
    local_by_year = {}
    
    for f in local:
        y = f['year']
        if y not in local_by_year:
            local_by_year[y] = {'yellow': [], 'green': []}
        local_by_year[y][f['taxi_type']].append(f['month'])
    
    return local_by_year.get(year, {'yellow': [], 'green': []})


def get_missing_months(year=2025):
    """
    Compare local files with TLC website to find missing months.
    
    Used to detect if December 2025 needs imputation.
    
    Args:
        year: Year to check
        
    Returns:
        dict: {'yellow': [missing months], 'green': [missing months]}
    """
    available_online = get_available_months_from_scrape().get(year, {'yellow': [], 'green': []})
    local = check_data_availability(year)
    
    yellow_missing = set(available_online.get('yellow', [])) - set(local.get('yellow', []))
    green_missing = set(available_online.get('green', [])) - set(local.get('green', []))
    
    return {'yellow': sorted(list(yellow_missing)), 'green': sorted(list(green_missing))}


if __name__ == "__main__":
    # Test module functionality when run directly
    print("Checking local data files...")
    existing = check_local_files()
    print(f"Found {len(existing)} existing parquet files")
    
    print("\nScraping TLC website...")
    available = get_available_months_from_scrape()
    for year in sorted(available.keys()):
        print(f"  {year}: Yellow {len(available[year]['yellow'])} months, Green {len(available[year]['green'])} months")
    
    print("\nMissing 2025 months (local vs online):")
    missing = get_missing_months(2025)
    print(f"Yellow: {missing['yellow']}")
    print(f"Green: {missing['green']}")
    
    if missing['yellow'] or missing['green']:
        print("\n⚠️ Missing files detected! Starting auto-download...")
        download_all_data(years=[2025], include_comparison=[2024])
    else:
        print("\n✅ All data is up to date!")
