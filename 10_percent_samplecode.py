import pandas as pd

# Load the Excel file
file_path = "/Users/sandeepvasireddy/School/5010FinalProject/src/MDM_100Records.xlsx"
data = pd.read_excel(file_path)

# Calculate 10% of the data
sample_size = int(len(data) * 0.1)

# Randomly sample 10% of the data
sample_data = data.sample(n=sample_size, random_state=42)

# Save the sample to a new Excel file
sample_file_path = "/Users/sandeepvasireddy/School/5010FinalProject/src/MDM_10PercentSample.xlsx"
sample_data.to_excel(sample_file_path, index=False)

print(f"10% sample saved to {sample_file_path}")