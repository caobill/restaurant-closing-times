from google.colab import drive
drive.mount('/content/drive')

# Import pandas
import pandas as pd

import folium

import requests
import re
import json
from bs4 import BeautifulSoup
import datetime
import time
import requests
from requests.exceptions import ConnectionError, HTTPError
import math

# Specify the file path (adjust the path if your CSV is in a subfolder)
file_path = '/content/drive/My Drive/restaurants_wip.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(file_path)

# Display the first few rows of the DataFrame
df.head()


def parse_time(time_str):
    """
    Parse a time string into a datetime.time object.
    Tries multiple common time formats.
    """
    time_str = time_str.strip()
    time_formats = ["%I:%M %p", "%I %p", "%H:%M"]
    for fmt in time_formats:
        try:
            dt = datetime.datetime.strptime(time_str, fmt)
            return dt.time()
        except ValueError:
            continue
    print(f"Could not parse time: {time_str}")
    return None

def get_snapshot_timestamp(url, target_timestamp, max_retries=3, backoff_seconds=60):
    """
    Uses the Wayback Machine's CDX API to retrieve the latest snapshot timestamp
    on or before target_timestamp. If the API refuses the connection (rate limit),
    waits backoff_seconds and retries up to max_retries times.
    """
    cdx_url = "https://web.archive.org/cdx/search/cdx"
    params = {
        'url': url,
        'output': 'json',
        'to': target_timestamp,
        'filter': 'statuscode:200'
    }
    headers = { 'User-Agent': 'MyApp/1.0 (+https://caobill.com/)' }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(cdx_url, params=params, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            break  # success!
        except ConnectionError:
            if attempt < max_retries:
                print(f"[Attempt {attempt}/{max_retries}] Connection refused—sleeping {backoff_seconds}s before retry.")
                time.sleep(backoff_seconds)
            else:
                print(f"[Attempt {attempt}/{max_retries}] Connection refused—no retries left.")
                return None
        except HTTPError as e:
            # HTTP 4xx/5xx: don’t retry on bad params, but you might on 5xx
            print(f"HTTP error: {e.response.status_code}")
            return None
        except ValueError:
            # JSON decode error
            print("Malformed JSON in response.")
            return None

    # if we exit loop without 'data', it means we never succeeded
    if 'data' not in locals():
        return None

    if len(data) < 2:
        print("No snapshots found.")
        return None

    # Skip header row, then pick the closest snapshot ≤ target_timestamp.
    snapshots = data[1:]
    valid = [s for s in snapshots if s[1] <= target_timestamp]
    if not valid:
        print("No snapshots before target.")
        return None

    return max(valid, key=lambda row: row[1])[1]

def get_archived_page(url, timestamp, max_retries=3, initial_delay=5, backoff_seconds=10):
    """
    Downloads the archived page from the Wayback Machine at the given timestamp.
    If the connection is refused (rate-limit), retries up to max_retries times,
    sleeping initial_delay before the first attempt and backoff_seconds between retries.
    """
    archived_url = f"https://web.archive.org/web/{timestamp}/{url}"

    # initial wait to avoid hammering the service
    time.sleep(initial_delay)

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(archived_url, timeout=20)
            resp.raise_for_status()  # raises HTTPError for 4xx/5xx
            return resp.text

        except ConnectionError as e:
            # usually “Errno 111: Connection refused” from rate-limiting
            print(f"[Attempt {attempt}/{max_retries}] Connection refused: {e}")
            if attempt < max_retries:
                print(f"→ Retrying in {backoff_seconds}s…")
                time.sleep(backoff_seconds)
            else:
                print("→ Max retries exceeded. Giving up.")
                return None

        except HTTPError as e:
            # non-200 responses: likely bad timestamp or URL – don’t retry
            print(f"HTTP error {e.response.status_code} fetching archived page.")
            return None

        except RequestException as e:
            # catch-all for other request issues
            print(f"Request failed on attempt {attempt}: {e}")
            return None

    # fallback
    return None



def extract_operating_hours(html):
    """
    Parses the HTML of a Yelp business page to extract the operating hours.
    Assumes that the page contains a table where each row (<tr>) has a weekday (e.g. "Mon")
    in a <th> and a time range (e.g. "9:00 AM – 5:00 PM" or "5:00 PM - 4:00 AM (Next day)")
    in a <td>.

    It splits the time range on an en dash (–) or hyphen (-), removes any "(Next day)" text,
    parses the closing time into a datetime.time object, and maps abbreviated weekdays to full names.

    Returns:
        A dictionary mapping the full day name (e.g., "Monday") to its closing time (as a time object).
    """
    from bs4 import BeautifulSoup
    import re

    soup = BeautifulSoup(html, 'html.parser')
    hours_map = {}

    # Look for table rows in the page.
    rows = soup.find_all("tr")
    day_abbrevs = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if th and td:
            day_text = th.get_text(strip=True)
            if day_text in day_abbrevs:
                hours_range = td.get_text(strip=True)
                # Split on an en dash (–) or hyphen (-) with optional spaces.
                parts = re.split(r'\s*(?:–|-)\s*', hours_range)
                if len(parts) >= 2:
                    closing_time_str = parts[-1].strip()
                else:
                    closing_time_str = hours_range.strip()
                # Remove the "(Next day)" portion if present.
                closing_time_str = closing_time_str.replace("(Next day)", "").strip()
                closing_time_obj = parse_time(closing_time_str)
                hours_map[day_text] = closing_time_obj

    # Fallback: if no table was found, try searching text directly.
    if not hours_map:
        possible_elements = soup.find_all(text=re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)'))
        for element in possible_elements:
            parent = element.parent
            text = parent.get_text(separator=" ", strip=True)
            match = re.search(
                r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+[\d:]+\s*(?:AM|PM).*?(?:–|-)\s*([\d:]+\s*(?:AM|PM))',
                text)
            if match:
                day = match.group(1)
                closing_time_str = match.group(2)
                closing_time_str = closing_time_str.replace("(Next day)", "").strip()
                closing_time_obj = parse_time(closing_time_str)
                hours_map[day] = closing_time_obj

    # Map abbreviated day names to full day names.
    day_full = {
        "Mon": "Monday",
        "Tue": "Tuesday",
        "Wed": "Wednesday",
        "Thu": "Thursday",
        "Fri": "Friday",
        "Sat": "Saturday",
        "Sun": "Sunday"
    }
    final_hours = {}
    for abbrev, closing in hours_map.items():
        full_day = day_full.get(abbrev, abbrev)

        if closing is None:
          final_hours[full_day] = None
        else:
          time_int = closing.hour * 100 + closing.minute
          if time_int <= 700:
              time_int += 2400
          final_hours[full_day] = time_int
    return final_hours

    import requests


def get_businesses(lat, lon):
  yelp_api_key = os.getenv('yelp_key')
  url = 'https://api.yelp.com/v3/businesses/search'
  headers = {
      "accept": "application/json",
      "authorization": yelp_api_key
  }
  params = {
      "latitude": lat,
      "longitude": lon,
      "term": "restaurant",
      "radius": 5000,
      "categories": "",  # empty category
      "sort_by": "distance",
      "limit": 50
  }

  response = requests.get(url, headers=headers, params=params)
  data = response.json()
  return data


coords = [(40.68460720720714, -74.00622643678166),
    (40.68460720720714, -74.00047931034488),
    (40.68460720720714, -73.99473218390810),
    (40.68460720720714, -73.98898505747132),
    (40.68460720720714, -73.98323793103454),
    (40.68460720720714, -73.97749080459776),
    (40.68460720720714, -73.97174367816098),
    (40.68911171171164, -74.00622643678166),
    (40.68911171171164, -74.00047931034488),
    (40.68911171171164, -73.99473218390810),
    (40.68911171171164, -73.98898505747132),
    (40.68911171171164, -73.98323793103454),
    (40.68911171171164, -73.97749080459776),
    (40.68911171171164, -73.97174367816098),
    (40.69361621621614, -74.00622643678166),
    (40.69361621621614, -74.00047931034488),
    (40.69361621621614, -73.99473218390810),
    (40.69361621621614, -73.98898505747132),
    (40.69361621621614, -73.98323793103454),
    (40.69361621621614, -73.97749080459776),
    (40.69361621621614, -73.97174367816098),
]

for coord in coords:
  data = get_businesses(coord[0], coord[1])
  print(data)

  already_grabbed = df["Restaurant Alias"].tolist()
  for biz in data.get("businesses", []):
      if biz.get("alias") in already_grabbed:
        print("already in df")
        continue
      rowToAdd = []
      print("\nname: " + biz.get("name"))
      print("alias: " + biz.get("alias"))
      rowToAdd.append(biz.get("alias"))
      print("coordinates: " + str(biz.get("coordinates").get("latitude")) + ", " + str(biz.get("coordinates").get("longitude")))
      rowToAdd.append(str(biz.get("coordinates").get("latitude")))
      rowToAdd.append(str(biz.get("coordinates").get("longitude")))
      print("business hours: {}".format(str(biz.get("businesss_hours"))))

      closing_time = {0: "", 1: "", 2: "", 3: "", 4: "", 5: "", 6: ""}
      if biz.get("business_hours") is not None:
          for hour in biz.get("business_hours"):
              open = hour.get("open")
              for day in open:
                  if closing_time[int(day.get("day"))] != "":
                    later_time = max(int(day.get("end")), int(closing_time[day.get("day")]))
                    closing_time[int(day.get("day"))] = later_time
                  else:
                    closing_time[int(day.get("day"))] = int(day.get("end"))

      closingTimes = [closing_time[x] for x in range(7)]
      for i in range(7):
        if closingTimes[i] == "":
          closingTimes[i] = "-1"
        elif int(closingTimes[i]) < 700:
          closingTimes[i] = str(int(closingTimes[i]) + 2400)

      if closingTimes == {0: "-1", 1: "-1", 2: "-1", 3: "-1", 4: "-1", 5: "-1", 6: "-1"}:
        print("not open anymore, skipping")
        continue

      print(closingTimes)
      if sum([int(x) for x in closingTimes]) == -7:
        print("not open anymore, skipping")
        continue

      rowToAdd += closingTimes


      url = "https://www.yelp.com/biz/{}".format(biz.get("alias"))
      target_timestamp = "20200201000000"


      snapshot_timestamp = get_snapshot_timestamp(url, target_timestamp)

      # only 15 requests for minute so sleep 4 secs
      time.sleep(4)
      print("sleeping for 4 seconds")
      if not snapshot_timestamp:
        print("No valid snapshot found before the target date.")
        continue
      else:
        print(f"Using snapshot from timestamp: {snapshot_timestamp}")

      print("Fetching archived page...")
      archived_html = get_archived_page(url, snapshot_timestamp,6)
      if not archived_html:
        rowToAdd += [None, None, None, None, None, None, None]
        print("Failed to fetch the archived page.")
      else:
        print("Extracting archived operating hours...")
        archived_hours = extract_operating_hours(archived_html)
        rowToAdd += [archived_hours.get("Monday"), archived_hours.get("Tuesday"), archived_hours.get("Wednesday"), archived_hours.get("Thursday"), archived_hours.get("Friday"), archived_hours.get("Saturday"), archived_hours.get("Sunday")]

      print(rowToAdd)
      df.loc[len(df)] = rowToAdd
      print(rowToAdd)
      time.sleep(4)

# read what’s already there
file_path = '/content/drive/My Drive/restaurants_wip.csv'

old = pd.read_csv(file_path)

# e.g. concatenate
combined = pd.concat([old, df], ignore_index=True)

# optionally drop duplicates
combined.drop_duplicates(inplace=True)

# overwrite the CSV
combined.to_csv(file_path, index=False)



def create_map(coords_colors, center=None, zoom_start=12, output_file='map.html'):
    """
    Renders an OpenStreetMap map with colored points and hover text.

    Args:
        coords_colors (list of tuples): Each tuple is (latitude, longitude, color, text).
        center (tuple): Optional (lat, lon) to center the map. If None, uses average coords.
        zoom_start (int): Initial zoom level.
        output_file (str): Path to save the generated HTML map.
    """
    if center is None:
        avg_lat = sum(lat for lat, lon, color, text in coords_colors) / len(coords_colors)
        avg_lon = sum(lon for lat, lon, color, text in coords_colors) / len(coords_colors)
        center = (avg_lat, avg_lon)

    # Create map with OpenStreetMap tiles
    m = folium.Map(location=center, zoom_start=zoom_start, tiles='OpenStreetMap')

    # Add each point with tooltip
    for lat, lon, color, text in coords_colors:
        folium.CircleMarker(
            location=(lat, lon),
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            tooltip=text  # Show this text on hover
        ).add_to(m)

    # Save to HTML
    m.save(output_file)
    print(f"Map saved to {output_file}")

def intt(s):
  if s is None or math.isnan(s):
    return -1
  return int(s)

def get_color(hours):
  before = [intt(x) for x in hours[4:11]]
  after = [intt(x) for x in hours[11:18]]

  print("hours before: {}".format(before))
  print("hours after: {}".format(after))

  value = sum([before[i] - after[i] for i in range(7) if before[i] != -1 and after[i] != -1 and after[i] is not None and before[i] is not None])
  print("diff: {}".format(value))

  # Define the gradient stops
  stops = [
        (-float('inf'), (139, 0, 0)),    # Dark Red for < -4000
        (-4000, (139, 0, 0)),            # Dark Red
        (-2000, (255, 0, 0)),            # Red
        (-500, (255, 255, 0)),           # Yellow
        (1000, (0, 255, 0)),             # Green
        (2000, (173, 216, 230)),         # Light Blue
        (4000, (0, 0, 139)),             # Dark Blue
        (float('inf'), (0, 0, 139))      # Dark Blue for > 4000
    ]

  # Find the range
  for i in range(len(stops) - 1):
      v0, c0 = stops[i]
      v1, c1 = stops[i + 1]
      if v0 <= value <= v1:
          if v0 == -float('inf') or v1 == float('inf'):
              # If value is outside defined range, return the edge color
              r, g, b = c0
          else:
              # Interpolate between c0 and c1
              ratio = (value - v0) / (v1 - v0)
              r = int(c0[0] + (c1[0] - c0[0]) * ratio)
              g = int(c0[1] + (c1[1] - c0[1]) * ratio)
              b = int(c0[2] + (c1[2] - c0[2]) * ratio)
          return f'#{r:02X}{g:02X}{b:02X}'

  # If somehow not found (edge case), return the last color
  return ""

if __name__ == '__main__':
  # Example usage with hover text

    sample_points = [
    ]

    for entry in df.itertuples():
      color = get_color(entry).lower()
      point = (float(entry[2]), float(entry[3]), color, entry[1])
      print(entry)
      sample_points.append(point)

    print(sample_points)
    create_map(sample_points, zoom_start=3, output_file='sample_map_with_tooltip.html')


