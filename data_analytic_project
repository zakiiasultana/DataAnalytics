import os
import pandas as pd

# Use current folder as input folder
input_folder = "."
output_folder = "./merged_datasets"  # Folder to save merged dataset
output_file = os.path.join(output_folder, "merged_filtered_dataset.csv")  # Change to .xlsx if needed

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Columns to keep
columns_to_keep = ["Seller Name", "Offline Shop", "Trade Licence", "Owner Name", "Seller Email Address", "Seller Phone Number", "Seller Address", "Category", "Product Drop Off Station"]

# List to store filtered DataFrames
filtered_dataframes = []

# Process each file in the folder
for file in os.listdir(input_folder):
    if file.endswith(".csv") or file.endswith(".xlsx"):  # Process CSV & Excel files
        file_path = os.path.join(input_folder, file)
        
        # Read the dataset
        if file.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # Keep only the specified columns
        df_filtered = df[[col for col in columns_to_keep if col in df.columns]]

        # Append to the list
        filtered_dataframes.append(df_filtered)

        print(f"Processed: {file}")

# Merge all DataFrames
if filtered_dataframes:
    merged_df = pd.concat(filtered_dataframes, ignore_index=True)
    
    # Save the merged dataset
    if output_file.endswith(".csv"):
        merged_df.to_csv(output_file, index=False)
    else:
        merged_df.to_excel(output_file, index=False)

    print(f"All datasets merged and saved in: {output_folder}/merged_filtered_dataset.csv")
else:
    print("No valid datasets found.")

# Categorization Process
# Read the merged dataset
if os.path.exists(output_file):
    df = pd.read_csv(output_file)
    column_name = "Seller Address"

    # Ensure the "Seller Address" column exists
    if column_name not in df.columns:
        raise KeyError(f"The dataset does not contain a {column_name} column.")

    # Define location categories with associated keywords (all lowercase for case-insensitive matching)
    location_categories = [
        ("agrabad", ["agrabad", "boropol"]),
        ("oxygen", ["oxygen"]),
        ("pahartali", ["pahartali"]),
        ("panchlaish", ["panchlaish"]),
        ("halishahar", ["halishahar"]),
        ("gec", ["gec", "khulshi"]),
        ("chandgaon", ["chandgaon"]),
        ("kotwali", ["kotwali"]),
        ("patenga", ["patenga"]),
        ("bayazid", ["bayazid"]),
        ("bakalia", ["bakalia"]),
        ("chawkbazar", ["chawkbazar"]),
        ("double_mooring", ["double mooring"])
    ]

    # Create a dictionary to collect rows for each location category
    categorized_data = {category: [] for category, _ in location_categories}
    categorized_data["uncategorized"] = []  # for rows that don't match any category

    # Process each row using only the "Seller Address" column
    for idx, row in df.iterrows():
        seller_address = str(row[column_name]).lower()  # Convert the seller address to lowercase
        matched = False
        for category, keywords in location_categories:
            if any(keyword in seller_address for keyword in keywords):
                categorized_data[category].append(row)
                matched = True
                break  # Stop after the first matching category
        if not matched:
            categorized_data["uncategorized"].append(row)

    # Create an output folder for categorized files
    categorized_output_folder = "categorized_output"
    os.makedirs(categorized_output_folder, exist_ok=True)

    # Save each category's data into separate CSV files
    for category, rows in categorized_data.items():
        if rows:  # Only save if there is data for the category
            df_category = pd.DataFrame(rows)
            output_path = os.path.join(categorized_output_folder, f"{category}.csv")
            df_category.to_csv(output_path, index=False)
            print(f"Saved {len(rows)} rows to {output_path}")

    print("Categorization complete.")
else:
    print("Merged dataset not found.")
