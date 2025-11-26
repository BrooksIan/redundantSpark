"""
Script to generate a redundant dataset with duplicates and variations.
This creates a CSV file with intentional duplicates for testing deduplication.
"""

import random
import csv
from typing import List, Tuple

def generate_sample_data(num_records: int = 1000) -> List[Tuple[str, str, str, str]]:
    """
    Generate sample data with intentional duplicates and variations.
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        List of tuples containing (id, name, email, address)
    """
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", 
                   "Robert", "Jessica", "William", "Ashley", "James", "Amanda",
                   "Christopher", "Melissa", "Daniel", "Michelle", "Matthew", "Kimberly"]
    
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson"]
    
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com"]
    
    streets = ["Main St", "Oak Ave", "Park Blvd", "Elm St", "Maple Dr", "Cedar Ln"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA"]
    
    data = []
    # Generate ~90% unique base records (10% will be duplicates/variations)
    base_records = int(num_records * 0.9)  # 90% unique records
    
    # Generate base records
    for i in range(base_records):
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}@{random.choice(domains)}"
        street_num = random.randint(100, 9999)
        address = f"{street_num} {random.choice(streets)}, {random.choice(cities)}, {random.choice(states)}"
        data.append((f"ID{i+1:04d}", name, email, address))
    
    # Create exact duplicates (~5% of total)
    num_duplicates = int(num_records * 0.05)
    num_duplicates = min(num_duplicates, len(data))
    duplicates = random.sample(data, num_duplicates)
    data.extend(duplicates)
    
    # Create variations (fuzzy duplicates) (~5% of total)
    variations = []
    num_variations = int(num_records * 0.05)
    num_variations = min(num_variations, len(data[:base_records]))
    for record in random.sample(data[:base_records], num_variations):
        id_val, name, email, address = record
        
        # Name variations
        name_variations = [
            name,  # Original
            name.replace(" ", ""),  # No space
            name.replace(" ", "-"),  # Hyphen
            name.upper(),  # Uppercase
            name.lower(),  # Lowercase
            name.replace("John", "Jon"),  # Common misspelling
            name.replace("Michael", "Mike"),  # Nickname
        ]
        
        # Email variations
        email_variations = [
            email,  # Original
            email.replace(".", ""),  # No dots
            email.replace("@", "at"),  # @ replaced
            email.upper(),  # Uppercase
        ]
        
        # Address variations
        address_variations = [
            address,  # Original
            address.replace("St", "Street"),  # Full form
            address.replace("Ave", "Avenue"),  # Full form
            address.replace("Blvd", "Boulevard"),  # Full form
            address.replace(",", ""),  # No comma
            address.lower(),  # Lowercase
        ]
        
        var_name = random.choice(name_variations)
        var_email = random.choice(email_variations)
        var_address = random.choice(address_variations)
        
        variations.append((f"ID{len(data)+len(variations)+1:04d}", var_name, var_email, var_address))
    
    data.extend(variations)
    
    # Fill remaining records to reach exact count
    # Add unique records to maintain ~10% duplicate rate
    remaining = num_records - len(data)
    if remaining > 0:
        for i in range(remaining):
            first = random.choice(first_names)
            last = random.choice(last_names)
            name = f"{first} {last}"
            email = f"{first.lower()}.{last.lower()}@{random.choice(domains)}"
            street_num = random.randint(100, 9999)
            address = f"{street_num} {random.choice(streets)}, {random.choice(cities)}, {random.choice(states)}"
            data.append((f"ID{len(data)+1:04d}", name, email, address))
    
    # Shuffle to mix duplicates with originals
    random.shuffle(data)
    
    # Trim to exact count if we went over
    data = data[:num_records]
    
    return data

def save_to_csv(data: List[Tuple[str, str, str, str]], filename: str = "redundant_data.csv"):
    """Save data to CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'name', 'email', 'address'])  # Header
        writer.writerows(data)
    
    print(f"Generated {len(data)} records and saved to {filename}")
    print(f"Expected unique records: ~{int(len(data) * 0.9)} (approximately 10% duplicates)")

if __name__ == "__main__":
    import sys
    
    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    output_file = sys.argv[2] if len(sys.argv) > 2 else "redundant_data.csv"
    
    print(f"Generating {num_records} records with duplicates and variations...")
    data = generate_sample_data(num_records)
    save_to_csv(data, output_file)

