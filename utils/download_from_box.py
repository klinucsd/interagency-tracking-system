import requests
import re
import json
from typing import List, Dict, Optional

def download_box_shared_link(shared_url: str, output_dir: str = ".") -> List[str]:
    """
    Download files from a Box shared link (folder or file).
    
    Args:
        shared_url: The Box shared link URL (e.g., https://calfire.box.com/s/HASH)
        output_dir: Directory to save downloaded files (default: current directory)
    
    Returns:
        List of successfully downloaded file paths
    
    Example:
        downloaded = download_box_shared_link("https://calfire.box.com/s/9kt8s4pho5zo499macjiw932yim5ib5l")
        print(f"Downloaded {len(downloaded)} files")
    """
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract the shared hash from URL
    shared_hash = shared_url.split('/')[-1]
    
    # Get the shared link page
    session = requests.Session()
    response = session.get(shared_url)
    
    downloaded_files = []
    
    if response.status_code != 200:
        print(f"Failed to access shared link: {response.status_code}")
        return downloaded_files
    
    print(f"Accessing shared link...")
    
    # Try to find file information in the page
    files_found = _extract_file_info(response.text)
    
    if not files_found:
        print("Could not find file information in the page.")
        debug_file = os.path.join(output_dir, 'box_page_debug.html')
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"Saved page HTML to {debug_file} for manual inspection")
        return downloaded_files
    
    print(f"Found {len(files_found)} file(s)\n")
    
    # Download each file
    for i, file_info in enumerate(files_found, 1):
        file_id = file_info['id']
        file_name = file_info['name']
        output_path = os.path.join(output_dir, file_name)
        
        download_url = f"https://calfire.app.box.com/index.php?rm=box_download_shared_file&shared_name={shared_hash}&file_id=f_{file_id}"
        
        print(f"[{i}/{len(files_found)}] Downloading: {file_name}")
        
        try:
            dl_response = session.get(download_url, allow_redirects=True, timeout=300)
            
            if dl_response.status_code == 200 and len(dl_response.content) > 1000:
                with open(output_path, 'wb') as f:
                    f.write(dl_response.content)
                print(f"    ✓ Saved: {file_name} ({len(dl_response.content):,} bytes)\n")
                downloaded_files.append(output_path)
            else:
                print(f"    ✗ Failed: status {dl_response.status_code}, size: {len(dl_response.content)}\n")
        except Exception as e:
            print(f"    ✗ Error: {e}\n")
    
    return downloaded_files


def _extract_file_info(html_content: str) -> List[Dict[str, str]]:
    """
    Extract file information (ID and name) from Box shared link HTML.
    
    Args:
        html_content: The HTML content of the Box shared link page
    
    Returns:
        List of dictionaries with 'id' and 'name' keys
    """
    files_found = []
    
    # Pattern 1: Look for typedID and name pairs
    typed_id_pattern = r'"typedID":"f_(\d+)"[^}]*"name":"([^"]+)"'
    matches = re.findall(typed_id_pattern, html_content)
    for file_id, file_name in matches:
        if file_name and '.' in file_name:  # Has an extension
            files_found.append({'id': file_id, 'name': file_name})
    
    if files_found:
        return files_found
    
    # Pattern 2: Look for JSON data structures
    json_patterns = [
        r'Box\.postStreamData\s*=\s*({.*?});',
        r'var\s+initialData\s*=\s*({.*?});',
    ]
    
    for pattern in json_patterns:
        json_matches = re.findall(pattern, html_content, re.DOTALL)
        for json_str in json_matches:
            try:
                data = json.loads(json_str)
                extracted = _extract_files_from_json(data)
                if extracted:
                    files_found.extend(extracted)
            except:
                continue
        if files_found:
            return files_found
    
    # Pattern 3: Look for file_id and name separately
    file_id_matches = re.findall(r'"file_id["\s:]+(\d+)', html_content)
    name_matches = re.findall(r'"name["\s:]+([^"]+\.\w+)', html_content)
    
    if file_id_matches and name_matches:
        files_found.append({
            'id': file_id_matches[0],
            'name': name_matches[0]
        })
        return files_found
    
    # Pattern 4: Look for data-item-id attributes
    item_ids = re.findall(r'data-item-id="(\d+)"', html_content)
    if item_ids:
        title_match = re.search(r'<title>([^<]+)</title>', html_content)
        file_name = "download"
        if title_match:
            file_name = title_match.group(1).split('|')[0].strip()
            if '.' not in file_name:
                file_name = f"{file_name}.zip"
        
        files_found.append({
            'id': item_ids[0],
            'name': file_name
        })
    
    return files_found


def _extract_files_from_json(data: dict) -> List[Dict[str, str]]:
    """
    Recursively extract file information from JSON data.
    
    Args:
        data: JSON data structure
    
    Returns:
        List of dictionaries with 'id' and 'name' keys
    """
    files = []
    
    if isinstance(data, dict):
        # Check if this is a file entry
        if data.get('type') == 'file' and 'id' in data and 'name' in data:
            files.append({
                'id': str(data['id']),
                'name': data['name']
            })
        
        # Check for item_collection or entries
        if 'item_collection' in data:
            entries = data['item_collection'].get('entries', [])
            for entry in entries:
                files.extend(_extract_files_from_json(entry))
        elif 'entries' in data:
            for entry in data['entries']:
                files.extend(_extract_files_from_json(entry))
        
        # Recursively check other dict values
        for value in data.values():
            if isinstance(value, (dict, list)):
                files.extend(_extract_files_from_json(value))
    
    elif isinstance(data, list):
        for item in data:
            files.extend(_extract_files_from_json(item))
    
    return files
