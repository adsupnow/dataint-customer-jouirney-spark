import datetime

class TrieNode:
    """
    Node in a Trie data structure for efficient URL matching.
    """
    def __init__(self):
        self.children = {}
        self.is_end_of_url = False
        self.location = None
        self.location_name = None

class Trie:
    """
    Trie data structure for efficient URL matching.
    """
    def __init__(self):
        self.root = TrieNode()

    def insert(self, url, location, location_name):
        """
        Insert a URL into the Trie with associated location information.
        
        Args:
            url (str): The URL to insert
            location (str): The location ID associated with the URL
            location_name (str): The location name associated with the URL
        """
        node = self.root
        for char in url:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_url = True
        node.location = location
        node.location_name = location_name

    def search(self, url):
        """
        Search for the most specific URL match in the Trie.
        
        Args:
            url (str): The URL to search for
            
        Returns:
            tuple: (location, location_name) if found, (None, None) otherwise
        """
        node = self.root
        match_location = None
        match_location_name = None
        
        for char in url:
            if char in node.children:
                node = node.children[char]
                if node.is_end_of_url:
                    match_location = node.location
                    match_location_name = node.location_name
            else:
                break
                
        return match_location, match_location_name

# Counter for logging purposes
call_count = 0

def find_most_specific_match_trie(url, trie):
    """
    Find the most specific URL match in the Trie with logging.
    
    Args:
        url (str): The URL to search for
        trie (Trie): The Trie data structure to search in
        
    Returns:
        tuple: (location, location_name) if found, (None, None) otherwise
    """
    global call_count
    call_count += 1
    
    if call_count % 1000000 == 0:
        print(">> find_most_specific_match_trie called %d times - %s", call_count, datetime.datetime.now())
    
    return trie.search(url)