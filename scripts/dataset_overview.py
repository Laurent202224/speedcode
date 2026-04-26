import pandas as pd
import sys
from collections import Counter

# Read the Excel file
excel_file = 'VF_Hackathon_Dataset_India_Large.xlsx'  # Update with your actual file path
df = pd.read_excel(excel_file)

# Calculate missing data counts and percentages
missing_data = pd.DataFrame({
    'Column': df.columns,
    'Missing_Count': df.isnull().sum().values,
    'Missing_Percentage': (df.isnull().sum().values / len(df) * 100).round(2)
})

# Sort by missing count in descending order
missing_data = missing_data.sort_values('Missing_Count', ascending=False).reset_index(drop=True)

# Analyze specialties
specialties_col = df['specialties']
total_records = len(df)
missing_specialties = specialties_col.isnull().sum()
records_with_specialties = total_records - missing_specialties

# Parse specialties (handle comma or semicolon-separated values)
all_specialties = []
for entry in specialties_col.dropna():
    entry_str = str(entry).strip()
    if entry_str and entry_str.lower() not in ['', 'nan', 'none']:
        if ';' in entry_str:
            specialties = [s.strip() for s in entry_str.split(';')]
        elif ',' in entry_str:
            specialties = [s.strip() for s in entry_str.split(',')]
        else:
            specialties = [entry_str]
        all_specialties.extend(specialties)

# Count specialties
specialty_counts = Counter(all_specialties)
specialty_data = pd.DataFrame({
    'Specialty': list(specialty_counts.keys()),
    'Count': list(specialty_counts.values()),
    'Percentage': [(count / len(all_specialties) * 100) for count in specialty_counts.values()]
}).round(2)

# Sort by count descending
specialty_data = specialty_data.sort_values('Count', ascending=False).reset_index(drop=True)

# Generate HTML
html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataset Overview</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
        }
        h2 {
            color: #555;
            margin-top: 30px;
            border-bottom: 2px solid #1976d2;
            padding-bottom: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        th {
            background-color: #1976d2;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background-color: #f9f9f9;
        }
        .missing-high {
            background-color: #ffcccc;
            font-weight: bold;
        }
        .missing-medium {
            background-color: #ffffcc;
        }
        .missing-low {
            background-color: #ccffcc;
        }
        .stats {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 15px;
            margin: 20px 0;
        }
        .stat-box {
            background-color: #e8f5e9;
            padding: 15px;
            border-radius: 4px;
            border-left: 4px solid #1976d2;
        }
        .stat-box h3 {
            margin: 0 0 5px 0;
            color: #555;
            font-size: 14px;
        }
        .stat-box .value {
            font-size: 24px;
            font-weight: bold;
            color: #1976d2;
        }
        .bar-container {
            background-color: #e0e0e0;
            border-radius: 4px;
            height: 25px;
            overflow: hidden;
        }
        .bar {
            background-color: #1976d2;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 8px;
            color: white;
            font-weight: bold;
            font-size: 12px;
        }
        .specialty-name {
            font-weight: 500;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Dataset Overview</h1>
        
        <h2>Missing Data Analysis</h2>
        <table>
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Missing Count</th>
                    <th>Missing Percentage</th>
                </tr>
            </thead>
            <tbody>
"""

# Add missing data rows with color coding
for idx, row in missing_data.iterrows():
    percentage = row['Missing_Percentage']
    if percentage > 50:
        row_class = 'missing-high'
    elif percentage > 20:
        row_class = 'missing-medium'
    else:
        row_class = 'missing-low'
    
    html_content += f"""
                <tr class="{row_class}">
                    <td>{row['Column']}</td>
                    <td>{row['Missing_Count']}</td>
                    <td>{row['Missing_Percentage']}%</td>
                </tr>
"""

html_content += f"""
            </tbody>
        </table>

        <h2>Specialties Analysis</h2>
        <div class="stats">
            <div class="stat-box">
                <h3>Total Records</h3>
                <div class="value">{total_records}</div>
            </div>
            <div class="stat-box">
                <h3>Records with Specialties</h3>
                <div class="value">{records_with_specialties}</div>
            </div>
            <div class="stat-box">
                <h3>Unique Specialties</h3>
                <div class="value">{len(specialty_data)}</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Specialty</th>
                    <th>Count</th>
                    <th>Percentage</th>
                    <th>Visual</th>
                </tr>
            </thead>
            <tbody>
"""

# Add specialties rows
if len(specialty_data) > 0:
    max_count = specialty_data['Count'].max()
    for idx, row in specialty_data.iterrows():
        bar_width = (row['Count'] / max_count * 100)
        html_content += f"""
                <tr>
                    <td class="specialty-name">{row['Specialty']}</td>
                    <td>{int(row['Count'])}</td>
                    <td>{row['Percentage']:.2f}%</td>
                    <td>
                        <div class="bar-container">
                            <div class="bar" style="width: {bar_width}%">{row['Percentage']:.1f}%</div>
                        </div>
                    </td>
                </tr>
"""

html_content += """
            </tbody>
        </table>
    </div>
</body>
</html>
"""

# Save to HTML
output_file = 'dataset_overview.html'
with open(output_file, 'w') as f:
    f.write(html_content)

print(f"Dataset overview saved to '{output_file}'")
print(f"\nMissing Data Summary:")
print(missing_data.head(10))
print(f"\nSpecialties Summary:")
print(f"Total records: {total_records}")
print(f"Records with specialties: {records_with_specialties}")
print(f"Unique specialties: {len(specialty_data)}")
print(f"\nTop 10 Specialties:")
print(specialty_data.head(10))