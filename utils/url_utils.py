from urllib.parse import urlparse

def normalize_url(url):
    """
    Normalize a URL by removing common prefixes and standardizing format.
    
    Args:
        url (str): The URL to normalize
        
    Returns:
        str: Normalized URL
    """
    if not isinstance(url, str):
        return ""
    
    try:
        parsed_url = urlparse(url.lower())
        netloc = parsed_url.netloc
        path = parsed_url.path.rstrip('/')  # Remove trailing slash if present
        base_url = f"{netloc}{path}"
        return base_url.replace('www.', '').replace('http://', '').replace('https://', '').replace('.securecafe', '').strip()
    except Exception:
        return ""