"""
Script to generate duplicate files for testing file-level deduplication.
Creates files with random names where 90% are unique and 10% are duplicates.
"""

import os
import random
import string
import hashlib
from pathlib import Path


def generate_random_filename(length=12, extension='.txt'):
    """Generate a random filename."""
    chars = string.ascii_letters + string.digits
    random_name = ''.join(random.choice(chars) for _ in range(length))
    return f"{random_name}{extension}"


def generate_file_content(seed=None):
    """Generate random file content."""
    if seed is not None:
        random.seed(seed)
    
    # Generate random text content
    lines = []
    num_lines = random.randint(5, 20)
    
    for _ in range(num_lines):
        line_length = random.randint(20, 80)
        line = ''.join(random.choice(string.ascii_letters + string.digits + ' ') 
                      for _ in range(line_length))
        lines.append(line)
    
    content = '\n'.join(lines)
    
    if seed is not None:
        random.seed()  # Reset random seed
    
    return content


def create_duplicate_files(output_dir="data/duplicatefiles", num_files=25, unique_percentage=0.9):
    """
    Create duplicate files for testing.
    
    Args:
        output_dir: Directory to create files in
        num_files: Total number of files to create
        unique_percentage: Percentage of files that should be unique (0.0 to 1.0)
    """
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Calculate number of unique files needed
    num_unique = int(num_files * unique_percentage)
    num_duplicates = num_files - num_unique
    
    print(f"Creating {num_files} files:")
    print(f"  - {num_unique} unique files")
    print(f"  - {num_duplicates} duplicate files")
    print(f"  - Output directory: {output_dir}\n")
    
    # Generate unique file contents
    unique_contents = []
    unique_hashes = set()
    
    print("Generating unique file contents...")
    for i in range(num_unique):
        # Generate content with a seed to ensure uniqueness
        content = generate_file_content(seed=i)
        content_hash = hashlib.md5(content.encode()).hexdigest()
        
        # Ensure content is unique (no hash collisions)
        while content_hash in unique_hashes:
            content = generate_file_content(seed=i + random.randint(1000, 9999))
            content_hash = hashlib.md5(content.encode()).hexdigest()
        
        unique_hashes.add(content_hash)
        unique_contents.append(content)
    
    # Create unique files
    created_files = []
    print(f"\nCreating {num_unique} unique files...")
    for i, content in enumerate(unique_contents):
        filename = generate_random_filename()
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        file_hash = hashlib.md5(content.encode()).hexdigest()
        created_files.append({
            'filename': filename,
            'filepath': filepath,
            'content_hash': file_hash,
            'is_duplicate': False
        })
        print(f"  [{i+1}/{num_unique}] Created: {filename} (hash: {file_hash[:8]}...)")
    
    # Create duplicate files (copy content from some unique files)
    print(f"\nCreating {num_duplicates} duplicate files...")
    for i in range(num_duplicates):
        # Pick a random unique file to duplicate
        source_index = random.randint(0, num_unique - 1)
        source_content = unique_contents[source_index]
        source_file = created_files[source_index]
        
        # Create new file with different name but same content
        filename = generate_random_filename()
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(source_content)
        
        file_hash = hashlib.md5(source_content.encode()).hexdigest()
        created_files.append({
            'filename': filename,
            'filepath': filepath,
            'content_hash': file_hash,
            'is_duplicate': True,
            'duplicate_of': source_file['filename']
        })
        print(f"  [{i+1}/{num_duplicates}] Created duplicate: {filename} (same as: {source_file['filename']})")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Total files created: {len(created_files)}")
    print(f"Unique files: {num_unique}")
    print(f"Duplicate files: {num_duplicates}")
    
    # Group by hash to show duplicates
    hash_groups = {}
    for file_info in created_files:
        hash_val = file_info['content_hash']
        if hash_val not in hash_groups:
            hash_groups[hash_val] = []
        hash_groups[hash_val].append(file_info['filename'])
    
    duplicate_groups = {h: files for h, files in hash_groups.items() if len(files) > 1}
    
    print(f"\nDuplicate groups found: {len(duplicate_groups)}")
    for i, (hash_val, files) in enumerate(duplicate_groups.items(), 1):
        print(f"\n  Group {i} (hash: {hash_val[:8]}...):")
        for filename in files:
            print(f"    - {filename}")
    
    print("\n" + "="*70)
    print(f"All files saved to: {os.path.abspath(output_dir)}")
    print("="*70 + "\n")
    
    return created_files


def main():
    """Main function."""
    import sys
    
    # Parse command line arguments
    num_files = 25
    unique_percentage = 0.9
    output_dir = "data/duplicatefiles"
    
    if len(sys.argv) > 1:
        try:
            num_files = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number of files: {sys.argv[1]}")
            print("Usage: python generate_duplicate_files.py [num_files] [unique_percentage] [output_dir]")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            unique_percentage = float(sys.argv[2])
            if not 0.0 <= unique_percentage <= 1.0:
                raise ValueError("Percentage must be between 0.0 and 1.0")
        except ValueError as e:
            print(f"Invalid unique percentage: {e}")
            print("Usage: python generate_duplicate_files.py [num_files] [unique_percentage] [output_dir]")
            sys.exit(1)
    
    if len(sys.argv) > 3:
        output_dir = sys.argv[3]
    
    create_duplicate_files(output_dir, num_files, unique_percentage)


if __name__ == "__main__":
    main()

