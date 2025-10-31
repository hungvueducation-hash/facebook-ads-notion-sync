# clear_notion_database_ultra_fast.py
# 🗑️ XÓA TOÀN BỘ BẢN GHI - SIÊU NHANH (MAX PARALLEL - 20 THREADS)

import requests
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ========== LOAD CONFIGURATION FILES ==========

load_dotenv(".env")
load_dotenv(".env.config")

# ========== CONFIGURATION ==========

NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID_DAILY = os.getenv('NOTION_DATABASE_ID_DAILY')

# Lock để tránh race condition khi update counter
delete_lock = Lock()
deleted_count = 0
failed_count = 0

print("\n" + "=" * 70)
print("🗑️  CLEAR NOTION DATABASE - SIÊU NHANH (MAX PARALLEL)")
print("=" * 70)
print(f"\n📊 Configuration:")
print(f"   Database ID: {NOTION_DATABASE_ID_DAILY}")
print(f"   Notion API Key: {NOTION_API_KEY[:20]}...")

# ✅ Validation
if not NOTION_DATABASE_ID_DAILY:
    print("\n❌ LỖI: NOTION_DATABASE_ID_DAILY không có giá trị!")
    exit(1)

if not NOTION_API_KEY:
    print("\n❌ LỖI: NOTION_API_KEY không có giá trị!")
    exit(1)

def get_notion_headers():
    """Tạo headers cho Notion API"""
    return {
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json',
        'Notion-Version': '2025-09-03'
    }

def normalize_id(id_str):
    """Normalize ID: xóa dấu - và convert thành lowercase"""
    return id_str.replace('-', '').lower() if id_str else ""

def get_all_pages():
    """Lấy tất cả pages từ database bằng Search API"""
    
    print("\n📋 Bước 1: Lấy danh sách tất cả bản ghi...")
    print("-" * 70)
    
    all_pages = []
    has_more = True
    next_cursor = None
    target_db_id = normalize_id(NOTION_DATABASE_ID_DAILY)
    
    try:
        url = "https://api.notion.com/v1/search"
        
        while has_more:
            payload = {
                "filter": {
                    "value": "page",
                    "property": "object"
                },
                "page_size": 100
            }
            
            if next_cursor:
                payload["start_cursor"] = next_cursor
            
            response = requests.post(
                url,
                json=payload,
                headers=get_notion_headers(),
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                for item in results:
                    if item.get('object') == 'page':
                        parent = item.get('parent', {})
                        parent_db_id = parent.get('database_id', '')
                        
                        if parent_db_id and normalize_id(parent_db_id) == target_db_id:
                            all_pages.append(item)
                
                has_more = data.get('has_more', False)
                next_cursor = data.get('next_cursor')
                
                print(f"   ✅ Searched {len(results)} items (Found {len(all_pages)} in this DB)")
            else:
                print(f"   ❌ Lỗi search: {response.status_code}")
                break
        
        print(f"\n✅ Tổng cộng lấy được: {len(all_pages)} bản ghi từ database")
        return all_pages
        
    except Exception as e:
        print(f"   ❌ Lỗi: {str(e)}")
        return []

def delete_page(page_id):
    """Xóa 1 page từ Notion"""
    
    global deleted_count, failed_count
    
    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    try:
        response = requests.patch(
            url,
            json={"archived": True},
            headers=get_notion_headers(),
            timeout=10
        )
        
        if response.status_code == 200:
            with delete_lock:
                deleted_count += 1
            return True
        else:
            with delete_lock:
                failed_count += 1
            return False
            
    except Exception as e:
        with delete_lock:
            failed_count += 1
        return False

def delete_all_pages_parallel(pages, max_workers=20):
    """Xóa tất cả pages SIÊU NHANH bằng ThreadPoolExecutor (20 threads)"""
    
    global deleted_count, failed_count
    
    print("\n🗑️  Bước 2: Xóa tất cả bản ghi (SIÊU NHANH - 20 Parallel Threads)...")
    print("-" * 70)
    print(f"   Xóa {len(pages)} items ngay lập tức...\n")
    
    deleted_count = 0
    failed_count = 0
    total = len(pages)
    
    # ✅ FIX: Tăng max_workers lên 20 (xóa lập tức!)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tất cả tasks
        futures = {executor.submit(delete_page, page['id']): page for page in pages}
        
        # Process results khi hoàn thành
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                if result:
                    progress = (deleted_count / total) * 100
                    print(f"  ✅ Đã xóa: {deleted_count}/{total} ({progress:.1f}%)", end='\r')
                    
            except Exception as e:
                print(f"  ⚠️ Lỗi: {str(e)[:40]}")
    
    print(f"\n\n✅ Hoàn thành: Đã xóa {deleted_count}/{total} bản ghi (Thất bại: {failed_count})")
    return deleted_count

def main():
    """Main function"""
    
    pages = get_all_pages()
    
    if not pages:
        print("\n⚠️ Không có bản ghi nào để xóa!")
        return
    
    # ✅ FIX 1: Bỏ câu hỏi confirm - xóa lập tức!
    print("\n" + "=" * 70)
    print(f"🚀 XÓA {len(pages)} BẢN GHI NGAY LẬP TỨC!")
    print("=" * 70)
    
    # Xóa ngay mà không hỏi
    deleted_count_result = delete_all_pages_parallel(pages, max_workers=20)
    
    print("\n" + "=" * 70)
    print("✅ XÓA DATABASE HOÀN TẤT!")
    print("=" * 70)
    print(f"📊 Tổng bản ghi: {len(pages)}")
    print(f"✨ Đã xóa: {deleted_count_result}")
    print(f"❌ Thất bại: {failed_count}")
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
