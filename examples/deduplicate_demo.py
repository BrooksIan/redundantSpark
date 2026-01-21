"""
Simple demonstration of deduplication logic without Spark.
This shows the concepts that would be used in Spark.
"""

import csv
from collections import defaultdict

def exact_deduplication_csv(input_file, output_file, key_columns):
    """Remove exact duplicates from CSV (equivalent to Spark's dropDuplicates)."""
    seen = set()
    unique_records = []
    duplicate_groups = defaultdict(list)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        # Find indices of key columns
        key_indices = [headers.index(col) for col in key_columns]
        
        for row in reader:
            # Create key from specified columns
            key = tuple(row[col] for col in key_columns)
            
            if key not in seen:
                seen.add(key)
                unique_records.append(row)
            else:
                # Track duplicates
                duplicate_groups[key].append(row)
    
    # Write unique records
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(unique_records)
    
    return unique_records, duplicate_groups

def show_deduplication_stats(original_count, unique_count, duplicate_groups, sample_records):
    """Show statistics about deduplication."""
    duplicates_removed = original_count - unique_count
    
    print("\n" + "="*60)
    print("DEDUPLICATION RESULTS")
    print("="*60)
    print(f"Original records:        {original_count:,}")
    print(f"Unique records:          {unique_count:,}")
    print(f"Duplicates removed:      {duplicates_removed:,}")
    print(f"Deduplication rate:      {duplicates_removed/original_count*100:.2f}%")
    
    # Show some example duplicates
    if duplicate_groups:
        print("\nExample duplicate groups (showing first 3):")
        for i, (key, duplicates) in enumerate(list(duplicate_groups.items())[:3]):
            print(f"\n  Found {len(duplicates) + 1} records for: {key[0]} / {key[1]}")
            # Find the original record
            for record in sample_records:
                if (record['name'], record['email']) == key:
                    print(f"    - {record['id']}: {record['name']} | {record['email']} | {record['address']}")
                    break
            # Show duplicates
            for dup in duplicates[:2]:  # Show max 2 duplicates
                print(f"    - {dup['id']}: {dup['name']} | {dup['email']} | {dup['address']}")
            if len(duplicates) > 2:
                print(f"    ... and {len(duplicates) - 2} more")
    
    print("="*60 + "\n")
    
    return {
        'original_count': original_count,
        'unique_count': unique_count,
        'duplicates_removed': duplicates_removed,
        'deduplication_rate': duplicates_removed / original_count * 100
    }

def process_file(input_file, output_dir="data", key_columns=['name', 'email']):
    """Process a single file and return statistics."""
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_dir, f"deduplicated_{base_name}.csv")
    
    print(f"\n{'='*70}")
    print(f"Processing: {input_file}")
    print(f"{'='*70}")
    
    # Count original records
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        original_count = sum(1 for _ in reader)
    
    print(f"Loaded {original_count:,} records")
    
    # Show first few records
    print(f"\nFirst 3 records:")
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 3:
                break
            print(f"  {row['id']}: {row['name']} | {row['email']} | {row['address']}")
    
    # Perform exact deduplication
    print(f"\nPerforming exact deduplication on {key_columns} columns...")
    
    unique_records, duplicate_groups = exact_deduplication_csv(
        input_file, 
        output_file, 
        key_columns=key_columns
    )
    
    unique_count = len(unique_records)
    
    # Show statistics
    stats = show_deduplication_stats(
        original_count, 
        unique_count, 
        duplicate_groups,
        unique_records
    )
    
    print(f"Saved deduplicated data to: {output_file}")
    
    return stats

if __name__ == "__main__":
    import sys
    import os
    import glob
    
    # Determine input files
    if len(sys.argv) > 1:
        # If file(s) specified, use them
        input_files = sys.argv[1:]
    else:
        # Otherwise, process all CSV files in data/ directory
        data_dir = "data"
        if os.path.exists(data_dir):
            input_files = glob.glob(os.path.join(data_dir, "*.csv"))
            # Exclude already deduplicated files
            input_files = [f for f in input_files if not os.path.basename(f).startswith("deduplicated_")]
        else:
            # Fallback to current directory
            input_files = glob.glob("*.csv")
            input_files = [f for f in input_files if not f.startswith("deduplicated_")]
    
    if not input_files:
        print("No CSV files found to process!")
        print("Usage: python deduplicate_demo.py [file1.csv] [file2.csv] ...")
        print("Or place CSV files in 'data/' directory")
        sys.exit(1)
    
    print(f"Found {len(input_files)} file(s) to process:")
    for f in input_files:
        print(f"  - {f}")
    
    # Process each file
    all_stats = []
    for input_file in input_files:
        try:
            stats = process_file(input_file, output_dir="data", key_columns=['name', 'email'])
            stats['filename'] = os.path.basename(input_file)
            all_stats.append(stats)
        except Exception as e:
            print(f"\nError processing {input_file}: {e}")
            continue
    
    # Summary across all files
    if len(all_stats) > 1:
        print("\n" + "="*70)
        print("SUMMARY ACROSS ALL FILES")
        print("="*70)
        total_original = sum(s['original_count'] for s in all_stats)
        total_unique = sum(s['unique_count'] for s in all_stats)
        total_removed = sum(s['duplicates_removed'] for s in all_stats)
        
        print(f"\nTotal files processed:     {len(all_stats)}")
        print(f"Total original records:    {total_original:,}")
        print(f"Total unique records:       {total_unique:,}")
        print(f"Total duplicates removed:   {total_removed:,}")
        print(f"Overall deduplication rate: {total_removed/total_original*100:.2f}%")
        
        print("\nPer-file breakdown:")
        for stats in all_stats:
            print(f"  {stats['filename']:30} {stats['original_count']:>8,} -> {stats['unique_count']:>8,} "
                  f"({stats['deduplication_rate']:>5.2f}% removed)")
        print("="*70 + "\n")
