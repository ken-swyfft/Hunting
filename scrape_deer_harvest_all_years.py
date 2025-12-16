"""
Scrape Washington State deer harvest statistics from WDFW website for all years (2013-2024).
Outputs one CSV row per GMU per method with full harvest detail.
"""

import requests
from bs4 import BeautifulSoup
import csv
import re
import time


def scrape_year(year: int) -> list[dict]:
    """Scrape deer harvest data for a single year."""
    url = f"https://wdfw.wa.gov/hunting/management/game-harvest/{year}/deer-general"

    print(f"  Fetching {url}...")
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    results = []

    # Build mappings by tracking elements in document order
    # District headings are h2, GMU identifiers vary by year
    district_map = {}  # table id -> district number
    gmu_map = {}       # table id -> (gmu_number, gmu_name)
    current_district = None
    current_gmu = None
    current_gmu_name = None

    # Iterate through all relevant elements in document order
    for element in soup.find_all(['h2', 'h3', 'h4', 'table']):
        if element.name == 'h2':
            text = element.get_text(strip=True)
            district_match = re.search(r'District\s*(\d+)', text, re.IGNORECASE)
            if district_match:
                current_district = district_match.group(1)
                # Reset GMU when entering new district
                current_gmu = None
                current_gmu_name = None

        elif element.name in ('h3', 'h4'):
            # Older years use h3 or h4 for GMU headings
            text = element.get_text(strip=True)
            gmu_match = re.match(r'(\d{3})\s*[-–]\s*(.+)', text)
            if gmu_match:
                current_gmu = gmu_match.group(1)
                current_gmu_name = gmu_match.group(2).strip()

        elif element.name == 'table':
            district_map[id(element)] = current_district

            # Check for caption (newer years 2018+)
            caption = element.find('caption')
            if caption:
                caption_text = caption.get_text(strip=True)
                gmu_match = re.match(r'(\d{3})\s*[-–]\s*(.+)', caption_text)
                if gmu_match:
                    gmu_map[id(element)] = (gmu_match.group(1), gmu_match.group(2).strip())
            elif current_gmu:
                # Use h3-based GMU from older format
                gmu_map[id(element)] = (current_gmu, current_gmu_name or '')

    # Process all tables that have GMU mappings
    tables = soup.find_all('table')

    for table in tables:
        table_id = id(table)
        if table_id not in gmu_map:
            continue

        gmu_number, gmu_name = gmu_map[table_id]
        district = district_map.get(table_id, '')

        rows = parse_gmu_table(table, year, district, gmu_number, gmu_name)
        results.extend(rows)

    return results


def parse_gmu_table(table, year: int, district: str, gmu_number: str, gmu_name: str) -> list[dict]:
    """Parse a GMU's harvest data table, returning one row per method.

    Table columns (0-indexed):
    0: Method
    1: Antlerless Harvest
    2: Antlered Harvest
    3: Total Harvest
    4: 1 Point
    5: 2 Point
    6: 3 Point
    7: 4 Point
    8: 5+ Point
    9: Number Hunters
    10: Hunter Success
    11: Hunter Days
    12: Days/Kill
    """
    rows = table.find_all('tr')
    results = []

    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 13:
            continue

        cell_texts = [c.get_text(strip=True) for c in cells]
        method = cell_texts[0]

        # Skip header row
        if method.lower() == 'method':
            continue

        # Skip totals row (we want individual methods only)
        if method.lower() == 'totals':
            continue

        results.append({
            'year': year,
            'district': district or '',
            'gmu_number': gmu_number,
            'gmu_name': gmu_name,
            'method': normalize_method(method),
            'antlerless_harvest': clean_value(cell_texts[1]),
            'antlered_harvest': clean_value(cell_texts[2]),
            'total_harvest': clean_value(cell_texts[3]),
            'points_1': clean_value(cell_texts[4]),
            'points_2': clean_value(cell_texts[5]),
            'points_3': clean_value(cell_texts[6]),
            'points_4': clean_value(cell_texts[7]),
            'points_5_plus': clean_value(cell_texts[8]),
            'num_hunters': clean_value(cell_texts[9]),
            'hunter_success_rate': clean_value(cell_texts[10]),
            'hunter_days': clean_value(cell_texts[11]),
            'days_per_kill': clean_value(cell_texts[12]),
        })

    return results


def clean_value(val: str) -> str:
    """Clean a cell value."""
    val = val.strip()
    if val.lower() == 'n/a':
        return ''
    return val


def normalize_method(method: str) -> str:
    """Normalize method names for consistency."""
    # Standardize "Modern Firearms" -> "Modern Firearm"
    if method.lower() == 'modern firearms':
        return 'Modern Firearm'
    return method


def write_csv(data: list[dict], output_path: str):
    """Write results to CSV file."""
    fieldnames = [
        'year',
        'district',
        'gmu_number',
        'gmu_name',
        'method',
        'antlerless_harvest',
        'antlered_harvest',
        'total_harvest',
        'points_1',
        'points_2',
        'points_3',
        'points_4',
        'points_5_plus',
        'num_hunters',
        'hunter_success_rate',
        'hunter_days',
        'days_per_kill',
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Wrote {len(data)} records to {output_path}")


def main():
    output_file = "deer_harvest_2013_2024.csv"
    start_year = 2013
    end_year = 2024

    all_data = []

    print(f"Scraping deer harvest data from {start_year} to {end_year}...")

    for year in range(start_year, end_year + 1):
        try:
            year_data = scrape_year(year)
            all_data.extend(year_data)
            print(f"  {year}: {len(year_data)} records")
            # Be polite to the server
            time.sleep(1)
        except requests.exceptions.HTTPError as e:
            print(f"  {year}: Failed to fetch ({e})")
        except Exception as e:
            print(f"  {year}: Error - {e}")

    if not all_data:
        print("No data found.")
        return

    write_csv(all_data, output_file)
    print("Done!")


if __name__ == "__main__":
    main()
