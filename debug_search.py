# clear_notion_database_debug.py
# 🗑️ DEBUG: Xem cấu trúc dữ liệu từ Search API

import requests
import os
from dotenv import load_dotenv
import json

load_dotenv(".env")
load_dotenv(".env.config")

NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID_DAILY = os.getenv('NOTION_DATABASE_ID_DAILY')

def get_notion_headers():
    return {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2025-09-03'
    }

print("\n" + "=" * 70)
print("🔍 DEBUG: Xem cấu trúc parent của pages từ Search API")
print("=" * 70)
print(f"\nDatabase ID cần tìm: {NOTION_DATABASE_ID_DAILY}\n")

url = "https://api.notion.com/v1/search"

payload = {
    "filter": {
        "value": "page",
        "property": "object"
    },
    "page_size": 10
}

response = requests.post(
    url,
    json=payload,
    headers=get_notion_headers(),
    timeout=15
)

if response.status_code == 200:
    data = response.json()
    results = data.get('results', [])
    
    print(f"Tìm được {len(results)} pages\n")
    
    for i, item in enumerate(results[:5], 1):
        print(f"--- Page {i} ---")
        print(f"ID: {item.get('id')}")
        print(f"Title: {item.get('title', [{}])[0].get('text', {}).get('content', 'N/A')}")
        print(f"Parent structure:")
        print(json.dumps(item.get('parent', {}), indent=2))
        print()
else:
    print(f"Lỗi: {response.status_code}")
    print(response.text)
