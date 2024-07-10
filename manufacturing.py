import pandas as pd
import random
import string
import numpy as np

# Load the existing CSV file
file_path = 'path_to_your_csv_file.csv'
df = pd.read_csv(file_path)

# Analyze the data
data_info = []
for col in df.columns:
    col_info = {
        'column_name': col,
        'dtype': df[col].dtype,
        'unique_values': set(df[col].unique())
    }
    if df[col].dtype in ['int64', 'float64']:
        col_info['min_value'] = df[col].min()
        col_info['max_value'] = df[col].max()
    else:
        col_info['min_value'] = None
        col_info['max_value'] = None
    data_info.append(col_info)

# Function to generate unique records
def generate_unique_record(data_info):
    new_record = {}
    for col_info in data_info:
        col_name = col_info['column_name']
        col_dtype = col_info['dtype']
        unique_values = col_info['unique_values']
        
        if col_dtype == 'int64':
            new_value = random.randint(col_info['min_value'], col_info['max_value'])
            while new_value in unique_values:
                new_value = random.randint(col_info['min_value'], col_info['max_value'])
        
        elif col_dtype == 'float64':
            new_value = random.uniform(col_info['min_value'], col_info['max_value'])
            while new_value in unique_values:
                new_value = random.uniform(col_info['min_value'], col_info['max_value'])
        
        elif col_dtype == 'object':
            new_value = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            while new_value in unique_values:
                new_value = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        
        elif col_dtype == 'datetime64[ns]':
            new_value = np.datetime64('2020-01-01') + np.timedelta64(random.randint(0, 365), 'D')
            while new_value in unique_values:
                new_value = np.datetime64('2020-01-01') + np.timedelta64(random.randint(0, 365), 'D')
        
        else:
            new_value = None  # Adjust as per your requirement
        
        new_record[col_name] = new_value
        unique_values.add(new_value)  # Update unique values to avoid duplicates in the same run
    
    return new_record

# Generate 16 lakh new records
new_records = [generate_unique_record(data_info) for _ in range(1600000)]

# Convert the new records to a DataFrame
df_new_records = pd.DataFrame(new_records)

# Combine original and new records
df_combined = pd.concat([df, df_new_records], ignore_index=True)

# Save the combined dataset
output_path = 'path_to_save_combined_csv_file.csv'
df_combined.to_csv(output_path, index=False)
