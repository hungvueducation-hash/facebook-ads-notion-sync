# sync_facebook_ads_daily_breakdown_NO_STEP1.py
# ✅ SIMPLIFIED: Bỏ Bước 1 - Chỉ tạo mới (không kiểm tra existing)

import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

# ========== CONFIGURATION ==========

FACEBOOK_ACCESS_TOKEN = os.getenv('FACEBOOK_ACCESS_TOKEN')
FACEBOOK_AD_ACCOUNT_IDS_STR = os.getenv('FACEBOOK_AD_ACCOUNT_IDS', '')
START_DATE = os.getenv('START_DATE', '2025-10-01')
END_DATE = os.getenv('END_DATE', '2025-10-29')
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID_DAILY = os.getenv('NOTION_DATABASE_ID_DAILY', '')

# Parse Ad Account IDs
FACEBOOK_AD_ACCOUNT_IDS = [id.strip() for id in FACEBOOK_AD_ACCOUNT_IDS_STR.split(',') if id.strip()]

# Parse Dynamic Fields
FACEBOOK_FIELDS_STR = os.getenv('FACEBOOK_FIELDS', 'spend,impressions,clicks,ctr,cpc')
FACEBOOK_FIELDS = [f.strip() for f in FACEBOOK_FIELDS_STR.split(',') if f.strip()]

# Parse Notion Field Mappings
NOTION_FIELD_MAPPINGS_STR = os.getenv('NOTION_FIELD_MAPPINGS', '')

def parse_field_mappings():
    """Parse Notion field mappings từ .env"""
    mappings = {}
    
    if NOTION_FIELD_MAPPINGS_STR:
        pairs = NOTION_FIELD_MAPPINGS_STR.split(',')
        for pair in pairs:
            if '|' in pair:
                fb_field, notion_field = pair.strip().split('|')
                mappings[fb_field.strip()] = notion_field.strip()
    
    if not mappings:
        mappings = {
            'spend': 'Spend',
            'impressions': 'Impressions',
            'clicks': 'Clicks',
            'ctr': 'CTR (%)',
            'cpc': 'CPC',
            'cpm': 'CPM'
        }
    
    return mappings

NOTION_FIELD_MAPPINGS = parse_field_mappings()

print("\n" + "=" * 70)
print("🚀 FACEBOOK ADS DAILY BREAKDOWN → NOTION (NO STEP 1)")
print("=" * 70)
print(f"\n📊 Configuration:")
print(f"   Ad Accounts: {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
print(f"   Date Range: {START_DATE} to {END_DATE}")
print(f"   Facebook Fields: {', '.join(FACEBOOK_FIELDS)}")
print(f"   Notion Fields: {', '.join(NOTION_FIELD_MAPPINGS.values())}")
print(f"   Notion DB: {NOTION_DATABASE_ID_DAILY[:20]}...")


def get_notion_headers():
    return {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2025-09-03'
    }


# ========== GET FACEBOOK DAILY DATA ==========

def get_facebook_daily_data_multi():
    """Lấy Facebook data breakdown by day từ multiple Ad Accounts"""
    
    print("\n📋 Bước 1: Lấy Facebook Daily Breakdown...")
    print("-" * 70)

    all_daily_data = []

    fields_to_fetch = ','.join(FACEBOOK_FIELDS)
    if 'account_id' not in fields_to_fetch:
        fields_to_fetch += ',account_id'

    for account_id in FACEBOOK_AD_ACCOUNT_IDS:
        print(f"\n📍 Account: {account_id}")

        url = f"https://graph.facebook.com/v19.0/act_{account_id}/insights"
        params = {
            'access_token': FACEBOOK_ACCESS_TOKEN,
            'fields': fields_to_fetch,
            'level': 'account',
            'time_increment': 1,
            'time_range[since]': START_DATE,
            'time_range[until]': END_DATE,
        }
        try:
            response = requests.get(url, params=params, timeout=15)
            print(f"   Status: {response.status_code}")
            response.raise_for_status()

            data = response.json()
            records = data.get('data', [])
            print(f"   ✅ Lấy {len(records)} daily records")
            for record in records:
                record['account_id'] = account_id
                print(f"   - {record.get('date_start')}: ${record.get('spend', 0)}")
                all_daily_data.append(record)

        except Exception as e:
            print(f"   ❌ Error: {str(e)[:80]}")

    print(f"\n✅ Tổng lấy được: {len(all_daily_data)} daily records từ {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
    return all_daily_data


# ========== BUILD NOTION PROPERTIES ==========

def build_notion_properties_daily(record):
    """Build Notion properties cho daily data"""
    
    properties = {}
    
    # Account ID phải là title
    account_id = record.get('account_id', 'Unknown')
    properties['Account ID'] = {
        "title": [{"text": {"content": str(account_id)}}]
    }
    
    # Always add Date
    date_str = record.get('date_start')
    if date_str:
        properties['Date'] = {
            "date": {"start": date_str}
        }
    
    # Add dynamic fields
    for fb_field, notion_field in NOTION_FIELD_MAPPINGS.items():
        value = record.get(fb_field)
        
        if value is None:
            continue
        
        # Numeric fields
        if fb_field in ['spend', 'impressions', 'clicks', 'ctr', 'cpc', 'cpm']:
            try:
                properties[notion_field] = {
                    "number": float(value)
                }
            except:
                properties[notion_field] = {
                    "rich_text": [{"text": {"content": str(value)}}]
                }
        else:
            properties[notion_field] = {
                "rich_text": [{"text": {"content": str(value)}}]
            }
    
    return properties


# ========== CREATE PAGE ==========

def create_page_daily(record):
    """Tạo page mới cho daily data"""
    
    url = "https://api.notion.com/v1/pages"
    
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID_DAILY},
        "properties": build_notion_properties_daily(record)
    }
    
    try:
        response = requests.post(url, json=payload, headers=get_notion_headers(), timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"  ⚠️ Lỗi: {str(e)[:60]}")
        return False


# ========== MAIN ==========

def main():
    
    # Validation
    if not FACEBOOK_AD_ACCOUNT_IDS:
        print("\n❌ Không có Ad Account IDs trong .env!")
        return
    
    if not NOTION_DATABASE_ID_DAILY:
        print("\n❌ Không có NOTION_DATABASE_ID_DAILY trong .env!")
        return
    
    if not all([FACEBOOK_ACCESS_TOKEN, NOTION_API_KEY]):
        print("\n❌ Credentials không đầy đủ!")
        return
    
    # ✅ BỎ BƯỚC 1: get_existing_daily_records() - Không cần kiểm tra existing
    
    # Bước 2: Get Facebook daily data
    facebook_daily_data = get_facebook_daily_data_multi()
    if not facebook_daily_data:
        print("\n⚠️ Không lấy được daily data từ Facebook")
        return
    
    # ✅ Bước 2 (Simplified): Tạo daily records - Chỉ tạo mới, không cập nhật
    print("\n🔄 Bước 2: Tạo daily records...")
    print("-" * 70)
    
    created = 0
    
    for i, record in enumerate(facebook_daily_data, 1):
        account_id = str(record.get('account_id', ''))
        date_str = record.get('date_start', '')
        
        if create_page_daily(record):
            created += 1
            print(f"  ✅ {i}/{len(facebook_daily_data)} Tạo: {account_id} - {date_str}")
        
        time.sleep(0.1)
    
    # Result
    print("\n" + "=" * 70)
    print("✅ SYNC DAILY BREAKDOWN HOÀN TẤT!")
    print("=" * 70)
    print(f"📊 Lấy: {len(facebook_daily_data)} daily records từ {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
    print(f"📅 Date Range: {START_DATE} → {END_DATE}")
    print(f"✨ Tạo mới: {created}")
    print(f"📊 Tổng: {created}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Dừng")
    except Exception as e:
        print(f"\n❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()
