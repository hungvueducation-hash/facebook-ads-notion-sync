# sync_facebook_to_notion_dynamic_fields.py
# ✅ DYNAMIC FIELDS CONFIGURATION FROM .env

import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import time
import json

load_dotenv()

# ========== CONFIGURATION ==========

FACEBOOK_ACCESS_TOKEN = os.getenv('FACEBOOK_ACCESS_TOKEN')
FACEBOOK_AD_ACCOUNT_IDS_STR = os.getenv('FACEBOOK_AD_ACCOUNT_IDS', '')
START_DATE = os.getenv('START_DATE', '2025-10-01')
END_DATE = os.getenv('END_DATE', '2025-10-29')
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')

# ========== DYNAMIC FIELDS CONFIGURATION ==========

# Parse Ad Account IDs
FACEBOOK_AD_ACCOUNT_IDS = [id.strip() for id in FACEBOOK_AD_ACCOUNT_IDS_STR.split(',') if id.strip()]

# Parse Dynamic Fields từ .env
# Format: FACEBOOK_FIELDS=field1,field2,field3
FACEBOOK_FIELDS_STR = os.getenv('FACEBOOK_FIELDS', 'campaign_name,campaign_id,spend,impressions,clicks,ctr,cpc')
FACEBOOK_FIELDS = [f.strip() for f in FACEBOOK_FIELDS_STR.split(',') if f.strip()]

# Parse Notion Field Mappings từ .env
# Format: NOTION_FIELDS=Campaign Name|campaign_name,Spend|spend,CTR (%)|ctr
# Hoặc: NOTION_FIELD_MAPPINGS={"campaign_name":"Campaign Name","spend":"Spend","ctr":"CTR (%)"}
NOTION_FIELD_MAPPINGS_STR = os.getenv('NOTION_FIELD_MAPPINGS', '')

# Parse Notion Field Mappings
def parse_field_mappings():
    """Parse Notion field mappings từ .env"""
    mappings = {}
    
    if NOTION_FIELD_MAPPINGS_STR:
        # Format: facebook_field|notion_field,facebook_field2|notion_field2
        pairs = NOTION_FIELD_MAPPINGS_STR.split(',')
        for pair in pairs:
            if '|' in pair:
                fb_field, notion_field = pair.strip().split('|')
                mappings[fb_field.strip()] = notion_field.strip()
    
    # Fallback mapping nếu không có trong .env
    if not mappings:
        mappings = {
            'campaign_name': 'Campaign Name',
            'campaign_id': 'Campaign ID',
            'spend': 'Spend',
            'impressions': 'Impressions',
            'clicks': 'Clicks',
            'ctr': 'CTR (%)',
            'cpc': 'CPC',
            'cpm': 'CPM',
            'account_id': 'Account ID'
        }
    
    return mappings

NOTION_FIELD_MAPPINGS = parse_field_mappings()

print("\n" + "=" * 70)
print("🚀 FACEBOOK ADS → NOTION SYNC (DYNAMIC FIELDS CONFIG)")
print("=" * 70)
print(f"\n📊 Configuration:")
print(f"   Ad Accounts: {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
print(f"   Date Range: {START_DATE} to {END_DATE}")
print(f"   Facebook Fields: {', '.join(FACEBOOK_FIELDS)}")
print(f"   Notion Fields: {', '.join(NOTION_FIELD_MAPPINGS.values())}")
print(f"   Notion DB: {NOTION_DATABASE_ID[:20]}...")


def get_notion_headers():
    return {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2025-09-03'
    }


# ========== GET EXISTING CAMPAIGNS ==========

def get_existing_campaigns():
    """Lấy tất cả campaigns hiện có trong Notion"""
    
    print("\n📋 Bước 1: Lấy campaigns hiện có...")
    print("-" * 70)
    
    try:
        url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
        
        response = requests.post(
            url,
            json={"page_size": 100},
            headers=get_notion_headers(),
            timeout=15
        )
        
        if response.status_code == 200:
            pages = response.json().get('results', [])
            
            existing = {}
            for page in pages:
                try:
                    page_id = page['id']
                    props = page.get('properties', {})
                    
                    # Get Campaign ID từ Notion Field Mapping
                    campaign_id_notion_field = NOTION_FIELD_MAPPINGS.get('campaign_id', 'Campaign ID')
                    campaign_id_prop = props.get(campaign_id_notion_field, {})
                    
                    if campaign_id_prop.get('type') == 'rich_text':
                        rich_texts = campaign_id_prop.get('rich_text', [])
                        if rich_texts:
                            cid = rich_texts[0].get('text', {}).get('content', '')
                            if cid:
                                existing[cid] = page_id
                except:
                    pass
            
            print(f"✅ Tìm {len(existing)} campaigns hiện có")
            return existing
        else:
            print(f"⚠️ Query lỗi {response.status_code} - tiếp tục...")
            return {}
            
    except Exception as e:
        print(f"⚠️ Lỗi: {str(e)[:80]}")
        return {}


# ========== GET FACEBOOK DATA (DYNAMIC FIELDS) ==========

def get_facebook_data_multi():
    """Lấy Facebook data với dynamic fields"""
    
    print("\n📊 Bước 2: Lấy Facebook Ads...")
    print("-" * 70)
    
    all_campaigns = []
    
    # Build fields string with account_id
    fields_to_fetch = ','.join(FACEBOOK_FIELDS)
    if 'account_id' not in fields_to_fetch:
        fields_to_fetch += ',account_id'
    
    for account_id in FACEBOOK_AD_ACCOUNT_IDS:
        print(f"\n📍 Account: {account_id}")
        
        url = f"https://graph.facebook.com/v19.0/act_{account_id}/insights"
        
        params = {
            'access_token': FACEBOOK_ACCESS_TOKEN,
            'fields': fields_to_fetch,
            'time_range[since]': START_DATE,
            'time_range[until]': END_DATE,
            'level': 'campaign'
        }
        
        try:
            print(f"   📍 Request: {url}")
            print(f"   📅 Date: {START_DATE} to {END_DATE}")
            print(f"   📊 Fields: {fields_to_fetch}")
            
            response = requests.get(url, params=params, timeout=15)
            
            print(f"   Status: {response.status_code}")
            
            response.raise_for_status()
            
            data = response.json()
            campaigns = data.get('data', [])
            
            print(f"   ✅ Lấy {len(campaigns)} campaigns")
            
            for campaign in campaigns:
                campaign['account_id'] = account_id
                all_campaigns.append(campaign)
                campaign_name = campaign.get('campaign_name', 'Unknown')[:50]
                print(f"   - {campaign_name}")
            
        except requests.exceptions.HTTPError as e:
            print(f"   ❌ HTTP Error: {e.response.status_code}")
            print(f"   Response: {e.response.text[:200]}")
        except Exception as e:
            print(f"   ❌ Lỗi: {str(e)[:80]}")
    
    print(f"\n✅ Tổng lấy được: {len(all_campaigns)} campaigns từ {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
    return all_campaigns


# ========== BUILD NOTION PROPERTIES ==========

def build_notion_properties(campaign):
    """Build Notion properties dynamically từ campaign data"""
    
    properties = {}
    
    for fb_field, notion_field in NOTION_FIELD_MAPPINGS.items():
        value = campaign.get(fb_field)
        
        # Skip nếu field không có value
        if value is None:
            continue
        
        # Campaign Name (Title)
        if fb_field == 'campaign_name':
            properties[notion_field] = {
                "title": [{"text": {"content": str(value)}}]
            }
        
        # Numeric fields (spend, impressions, clicks, ctr, cpc, cpm)
        elif fb_field in ['spend', 'impressions', 'clicks', 'ctr', 'cpc', 'cpm']:
            try:
                properties[notion_field] = {
                    "number": float(value)
                }
            except:
                properties[notion_field] = {
                    "rich_text": [{"text": {"content": str(value)}}]
                }
        
        # Text fields (campaign_id, account_id, etc)
        else:
            properties[notion_field] = {
                "rich_text": [{"text": {"content": str(value)}}]
            }
    

    return properties


# ========== CREATE PAGE ==========

def create_page(campaign):
    """Tạo page mới với dynamic fields"""
    
    url = "https://api.notion.com/v1/pages"
    
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": build_notion_properties(campaign)
    }
    
    try:
        response = requests.post(url, json=payload, headers=get_notion_headers(), timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"  ⚠️ Lỗi: {str(e)[:60]}")
        return False


# ========== UPDATE PAGE ==========

def update_page(page_id, campaign):
    """Cập nhật page với dynamic fields"""
    
    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    payload = {
        "properties": build_notion_properties(campaign)
    }
    
    try:
        response = requests.patch(url, json=payload, headers=get_notion_headers(), timeout=10)
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
    
    if not all([FACEBOOK_ACCESS_TOKEN, NOTION_API_KEY, NOTION_DATABASE_ID]):
        print("\n❌ Credentials không đầy đủ!")
        return
    
    if not FACEBOOK_FIELDS:
        print("\n❌ Không có Facebook Fields trong .env!")
        return
    
    # Step 1: Get existing
    existing_campaigns = get_existing_campaigns()
    
    # Step 2: Get Facebook data
    facebook_campaigns = get_facebook_data_multi()
    if not facebook_campaigns:
        print("\n⚠️ Không lấy được campaign từ Facebook")
        return
    
    # Step 3: Sync
    print("\n🔄 Bước 3: Cập nhật/Tạo campaigns...")
    print("-" * 70)
    
    created = 0
    updated = 0
    
    for i, campaign in enumerate(facebook_campaigns, 1):
        campaign_id = str(campaign.get('campaign_id', ''))
        name = campaign.get('campaign_name', 'Unknown')[:50]
        account = campaign.get('account_id', 'Unknown')
        
        if campaign_id in existing_campaigns:
            if update_page(existing_campaigns[campaign_id], campaign):
                updated += 1
                print(f"  ✅ {i}/{len(facebook_campaigns)} Cập nhật: {name} (Account: {account})")
        else:
            if create_page(campaign):
                created += 1
                print(f"  ✅ {i}/{len(facebook_campaigns)} Tạo: {name} (Account: {account})")
        
        time.sleep(0.1)
    
    # Result
    print("\n" + "=" * 70)
    print("✅ SYNC HOÀN TẤT!")
    print("=" * 70)
    print(f"📊 Lấy: {len(facebook_campaigns)} campaigns từ {len(FACEBOOK_AD_ACCOUNT_IDS)} accounts")
    print(f"📅 Date Range: {START_DATE} → {END_DATE}")
    print(f"📋 Fields: {', '.join(FACEBOOK_FIELDS)}")
    print(f"✨ Tạo mới: {created}")
    print(f"🔄 Cập nhật: {updated}")
    print(f"📊 Tổng: {created + updated}")
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
