import pandas as pd

# Path to your CSV file
csv_file_path = "C:\\Users\\navin\\Downloads\\Airline_Data_Ingestion_Project-20240406T100510Z-001\\Airline_Data_Ingestion_Project\\flights.csv"

# Number of parts to divide the file into
n_parts = 5

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Calculate the number of rows in each part
rows_per_part = len(df) // n_parts

# Split the DataFrame into smaller parts
file_parts = [df.iloc[i:i+rows_per_part] for i in range(0, len(df), rows_per_part)]

# Save each part as a separate CSV file
for i, part in enumerate(file_parts):
    part.to_csv(f'airline-data-ingestion/flights_data_part{i+1}.csv', index=False)
