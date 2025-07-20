import pandas as pd
import numpy as np
import os
import csv

def process_row(row):
    """
    Process a row of data, handling the review_text field specially.
    First 6 fields and last 7 fields are kept as is, everything in between is review_text.
    """
    if len(row) < 14:  # Minimum required fields (6 + 1 + 7)
        return None
    
    # First 6 fields
    result = row[:6]
    
    # Everything in between is review_text
    review_text = ','.join(row[6:-7])
    
    # Last 7 fields
    result.extend([review_text] + row[-7:])
    
    return result

def split_data(input_file='processed_data.csv', default_ratio=20):
    """
    Split the input CSV file into a smaller sample based on the specified ratio.
    Handles text fields properly by considering the structure of the data.
    """
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found!")
        return
    
    try:
        print("Reading and processing the file...")
        
        # Read the file line by line
        processed_rows = []
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=',')  # Changed delimiter to comma
            header = next(reader)  # Get header
            
            # Process each row
            for row in reader:
                processed_row = process_row(row)
                if processed_row is not None:
                    processed_rows.append(processed_row)
        
        # Convert to DataFrame
        df = pd.DataFrame(processed_rows, columns=header)
        
        print(f"\nSuccessfully processed the file. Found {len(df)} valid rows and {len(df.columns)} columns.")
        print("Columns found:", df.columns.tolist())
        
        # Get the ratio from user input
        ratio = input("\nВведите долю данных от исходного файла которая останется в новом файле: ")
        
        # Convert to integer, use default if invalid input
        try:
            ratio = int(ratio)
            if ratio <= 0:
                print(f"Invalid ratio. Using default value: {default_ratio}")
                ratio = default_ratio
        except ValueError:
            print(f"Invalid input. Using default value: {default_ratio}")
            ratio = default_ratio
        
        # Shuffle the dataframe
        df_shuffled = df.sample(frac=1, random_state=42)
        
        # Calculate the number of rows to keep
        n_rows = len(df_shuffled) // ratio
        
        # Take the sample
        df_sample = df_shuffled.head(n_rows)
        
        # Create output filename
        output_file = f"processed_data_1_{ratio}.csv"
        
        # Save to CSV with comma separator and utf-8-sig encoding
        df_sample.to_csv(output_file, sep=',', encoding='utf-8-sig', index=False)
        
        print(f"\nSuccessfully created {output_file}")
        print(f"Original file size: {len(df)} rows")
        print(f"New file size: {len(df_sample)} rows")
        
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("\nPlease check if the input file is properly formatted and try again.")

if __name__ == "__main__":
    split_data() 