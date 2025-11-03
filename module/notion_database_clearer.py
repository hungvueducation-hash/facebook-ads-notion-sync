import os
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

class NotionDatabaseClearer:
    """Cái hộp xóa dữ liệu Notion"""
    
    def __init__(self, notion_api_key: Optional[str] = None):
        self.notion_api_key = notion_api_key or os.getenv("NOTION_API_KEY")
        self.headers = {
            "Authorization": f"Bearer {self.notion_api_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.base_url = "https://api.notion.com/v1"
    
    def get_all_pages(self, database_id: str, batch_size: int = 100) -> List[Dict]:
        all_pages = []
        has_more = True
        start_cursor = None
        
        while has_more:
            query_url = f"{self.base_url}/databases/{database_id}/query"
            payload = {"page_size": batch_size}
            if start_cursor:
                payload["start_cursor"] = start_cursor
            
            response = requests.post(query_url, headers=self.headers, json=payload)
            response.raise_for_status()
            
            data = response.json()
            all_pages.extend(data.get("results", []))
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")
            
            print(f"✓ Lấy {len(data.get('results', []))} trang")
        
        return all_pages
    
    def delete_page(self, page_id: str) -> bool:
        try:
            update_url = f"{self.base_url}/pages/{page_id}"
            response = requests.patch(update_url, headers=self.headers, json={"archived": True})
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"✗ Lỗi: {e}")
            return False
    
    def clear_database(self, database_id: str, dry_run: bool = False) -> Dict:
        print(f"Database: {database_id}")
        pages = self.get_all_pages(database_id)
        total_pages = len(pages)
        
        if total_pages == 0:
            return {"total_pages": 0, "deleted_pages": 0, "failed_pages": 0}
        
        if dry_run:
            print(f"🔍 Dry run: {total_pages} trang sẽ xóa")
            return {"total_pages": total_pages, "deleted_pages": 0, "failed_pages": 0}
        
        deleted_count = 0
        failed_count = 0
        
        for index, page in enumerate(pages, 1):
            if self.delete_page(page["id"]):
                deleted_count += 1
            else:
                failed_count += 1
        
        return {
            "total_pages": total_pages,
            "deleted_pages": deleted_count,
            "failed_pages": failed_count
        }


def clear_notion_database(database_id: str, notion_api_key: Optional[str] = None, dry_run: bool = False) -> Dict:
    clearer = NotionDatabaseClearer(notion_api_key)
    return clearer.clear_database(database_id, dry_run=dry_run)
