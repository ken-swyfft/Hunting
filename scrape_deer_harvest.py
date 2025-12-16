"""
Scrape 2024 Washington State deer harvest statistics from WDFW website.
Outputs one CSV row per GMU with archery and total harvest statistics.
"""

import requests
from bs4 import BeautifulSoup
import csv
import re


def scrape_deer_harvest(url: str) -> list[dict]:
    """Scrape deer harvest data from WDFW page."""
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    results = []

    # Build a mapping of which district each table belongs to
    # by tracking district headings (h2) in document order
    district_map = {}  # table -> district number
    current_district = None

    for element in soup.find_all(['h2', 'table']):
        if element.name == 'h2':
            text = element.get_text(strip=True)
            district_match = re.search(r'District\s*(\d+)', text, re.IGNORECASE)
            if district_match:
                current_district = district_match.group(1)
        elif element.name == 'table':
            district_map[id(element)] = current_district

    # Find all tables with captions containing GMU info
    tables = soup.find_all('table')

    for table in tables:
        caption = table.find('caption')
        if not caption:
            continue

        caption_text = caption.get_text(strip=True)

        # Match GMU pattern: "101 - SHERMAN"
        gmu_match = re.match(r'(\d{3})\s*[-–]\s*(.+)', caption_text)
        if not gmu_match:
            continue

        gmu_number = gmu_match.group(1)
        gmu_name = gmu_match.group(2).strip()
        district = district_map.get(id(table), '')

        gmu_data = parse_gmu_table(table, district, gmu_number, gmu_name)
        if gmu_data:
            results.append(gmu_data)

    return results


def parse_gmu_table(table, district: str, gmu_number: str, gmu_name: str) -> dict | None:
    """Parse a GMU's harvest data table.

    Table columns (0-indexed):
    0: Method
    1: Antlerless Harvest
    2: Antlered Harvest
    3: Total Harvest
    4-8: Point breakdowns (1-5+ Point)
    9: Number Hunters
    10: Hunter Success
    11: Hunter Days
    12: Days/Kill
    """
    rows = table.find_all('tr')

    archery_data = None
    total_harvest = 0
    total_hunters = 0
    total_hunter_days = 0

    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 13:
            continue

        cell_texts = [c.get_text(strip=True) for c in cells]
        method = cell_texts[0].lower()

        # Skip header row
        if 'method' in method:
            continue

        if 'archery' in method:
            archery_data = {
                'harvest': clean_value(cell_texts[3]),
                'hunters': clean_value(cell_texts[9]),
                'success_rate': clean_value(cell_texts[10]),
                'days_per_kill': clean_value(cell_texts[12]),
            }

        if 'total' in method:
            # Totals row has the aggregate harvest
            total_harvest = parse_int(cell_texts[3])
        else:
            # Accumulate hunters and hunter days from individual methods
            hunters = parse_int(cell_texts[9])
            hunter_days = parse_int(cell_texts[11])
            total_hunters += hunters
            total_hunter_days += hunter_days

    # Calculate overall stats
    if total_harvest > 0 and total_hunters > 0:
        overall_success_rate = f"{(total_harvest / total_hunters * 100):.0f}%"
        overall_days_per_kill = f"{(total_hunter_days / total_harvest):.0f}" if total_harvest > 0 else ''
    else:
        overall_success_rate = ''
        overall_days_per_kill = ''

    return {
        'district': district or '',
        'gmu_number': gmu_number,
        'gmu_name': gmu_name,
        'archery_hunters': archery_data.get('hunters', '') if archery_data else '',
        'archery_harvest': archery_data.get('harvest', '') if archery_data else '',
        'archery_success_rate': archery_data.get('success_rate', '') if archery_data else '',
        'archery_hunter_days_per_kill': archery_data.get('days_per_kill', '') if archery_data else '',
        'total_hunters': str(total_hunters) if total_hunters > 0 else '',
        'total_harvest': str(total_harvest) if total_harvest > 0 else '',
        'overall_success_rate': overall_success_rate,
        'overall_hunter_days_per_kill': overall_days_per_kill,
    }


def clean_value(val: str) -> str:
    """Clean a cell value."""
    val = val.strip()
    if val.lower() == 'n/a':
        return ''
    return val


def parse_int(val: str) -> int:
    """Parse a string to int, handling commas and n/a."""
    val = val.strip().replace(',', '')
    if val.lower() == 'n/a' or not val:
        return 0
    try:
        return int(val)
    except ValueError:
        return 0


def write_csv(data: list[dict], output_path: str):
    """Write results to CSV file."""
    fieldnames = [
        'district',
        'gmu_number',
        'gmu_name',
        'archery_hunters',
        'archery_harvest',
        'archery_success_rate',
        'archery_hunter_days_per_kill',
        'total_hunters',
        'total_harvest',
        'overall_success_rate',
        'overall_hunter_days_per_kill',
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Wrote {len(data)} GMU records to {output_path}")


def main():
    url = "https://wdfw.wa.gov/hunting/management/game-harvest/2024/deer-general"
    output_file = "deer_harvest_2024.csv"

    print(f"Fetching data from {url}...")
    data = scrape_deer_harvest(url)

    if not data:
        print("No data found. The page structure may have changed.")
        return

    write_csv(data, output_file)
    print("Done!")


if __name__ == "__main__":
    main()
