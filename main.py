import streamlit as st
import duckdb
import os
import re

# Get all CSV files in the 'data' folder
data_folder = "data/tennis_atp-master"
csv_files = [f for f in os.listdir(data_folder) if f.endswith(".csv")]

def read_csv(csv_files, table_name="df"):
    """
    Loads data from multiple CSV files into a DuckDB table.

    Args:
        csv_files (list): List of CSV file paths.
        table_name (str, optional): Name of the table to create in DuckDB. Defaults to "df".
    """
    conn = duckdb.connect(":memory:")

    # Create the table with schema inference from the first file
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM '{data_folder}/{csv_files[0]}'")

    # Insert data from remaining files
    for filename in csv_files[1:]:
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM '{data_folder}/{filename}'")
    
    conn.commit()
    conn.close()
    

def execute_sql(query_string):
    conn = duckdb.connect(":memory:")

    # Create the table with schema inference from the first file
    return conn.execute(query_string).fetchdf()
    

# Function to suggest file patterns
def suggest_patterns(filenames):
  # Extract patterns from filenames (basic approach)
  patterns = set()
  for filename in filenames:
    parts = filename.split("_")
    if len(parts) > 1:
      pattern = "_".join(parts[:-1]) + "*"
      patterns.add(pattern)
  return patterns

# Streamlit app layout
st.title("Data Analytics App")

# Selection method sidebar
selection_method = st.sidebar.selectbox("Select by:", ["File Name", "File Pattern"])

if selection_method == "File Name":
    # File selection by name
    selected_files = st.sidebar.multiselect("Select CSV files:", csv_files)
else:
    # File selection by pattern
    # Suggest patterns based on filenames
    suggested_patterns = suggest_patterns(csv_files)
    pattern_options = list(suggested_patterns)  # TODO: Add "Custom Pattern" option
    selected_pattern = st.sidebar.selectbox("Choose file pattern:", pattern_options)

    pattern = re.compile(selected_pattern)  # Assuming only one suggested pattern selected
    selected_files = [f for f in csv_files if pattern.match(f)]

if selected_files:
    # Display number of selected files
    num_files = len(selected_files)
    st.write(f"**{num_files} files selected:**")
    # Display list of selected files with formatting
    file_list = "\n".join(selected_files)
    st.write(f"<pre>{file_list}</pre>", unsafe_allow_html=True)  # Display as preformatted text
    # st.write(f"Selected files matching pattern '{pattern.pattern}': {', '.join(selected_files)}")

    # Analyze selected files
    df = read_csv(selected_files)
    # ... rest of the code for analyzing each file ...
    # Display top 100 rows of the DataFrame
    if df is not None:
        st.header("Top 100 Rows")
        st.dataframe(df.head(100))  # Display only the first 100 rows

    # User input for column name
    column_name = st.text_input("Enter a column name to see unique values:")

    # Display unique values if a column name is provided
    if column_name:
        unique_values = execute_sql(f"SELECT DISTINCT {column_name} from df")
        st.write(unique_values)

else:
  st.write("Select some CSV files to analyze.")
  


