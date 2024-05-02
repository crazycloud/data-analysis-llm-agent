import sqlite3 

EXTRA_SCHEMA_INFO = """
"""


async def run_db_query(sql_query):
    connection = None
    try:
        # Establish the connection
        connection = sqlite3.connect('../data/movies.db')

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the column names
        column_names = [desc[0] for desc in cursor.description]

        # Fetch all rows
        result = cursor.fetchall()
        
        return result, column_names
    except sqlite3.Error as error:
        print("Error while executing the query:", error)
        return [], []
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("SQLite connection is closed")


def generate_postgres_table_info_query(schema_table_pairs):
    query = """
    SELECT
        cols.table_schema,
        cols.table_name,
        cols.column_name,
        cols.data_type,
        coalesce(com.description, '') as column_description
    FROM
        information_schema.columns cols
    LEFT JOIN
        pg_class cl ON cl.relname = cols.table_name
    LEFT JOIN
        pg_description com ON com.objoid = cl.oid AND com.objsubid = cols.ordinal_position
    WHERE
        (cols.table_schema, cols.table_name) IN ({});
    """.format(', '.join(["('{}', '{}')".format(schema, table) for schema, table in schema_table_pairs]))

    return query

def generate_sqlite_table_info_query(schema_table_pairs):
 sql_query = """SELECT 
    sql
    FROM 
    sqlite_master m 
    WHERE 
    m.type='table' AND m.name NOT LIKE 'sqlite_%';"""

 return sql_query

def format_table_info(results, columns):
    table_info = ""
    current_table = None

    for row in results:
        table_schema = row[columns.index('table_schema')]
        table_name = row[columns.index('table_name')]
        column_name = row[columns.index('column_name')]
        data_type = row[columns.index('data_type')]
        column_description = row[columns.index('column_description')]

        if current_table != table_name:
            if current_table is not None:
                table_info += "\n\n"
            table_info += f"""Table Name: "{table_schema}"."{table_name}"\n"""
            table_info += f"----------\n"
            current_table = table_name
            table_info += f"Following are Column Name(Datatype) and Description:\n"

        table_info += f"{column_name}({data_type})"
        if column_description:
            table_info += f" - {column_description}\n"
        else:
            table_info += '\n'

    return table_info

def format_sample_data(column_names, data_records):
    formatted_data = ""
    for col_name in column_names:
        # Get unique non-empty values for the column
        values = set(record[column_names.index(col_name)] for record in data_records if record[column_names.index(col_name)] is not None and record[column_names.index(col_name)] != '')
        if values:  # Check if values exist
            formatted_data += f"{col_name}: "
            sample_values = ', '.join(str(value) for value in list(values)[:3])  # Display first 3 unique values
            if len(values) > 3:
                sample_values += ", ..."
            formatted_data += sample_values + '\n'

    return formatted_data

def generate_sample_data_query(schema, table, N):
    return f"""SELECT * FROM "{schema}"."{table}" ORDER BY RANDOM() LIMIT {N};"""


# formatting data
def convert_to_json1(rows, column_names):
    results = []

    for row in rows:
        # Convert row data into a dictionary with column names as keys
        row_dict = dict(zip(column_names, row))
        results.append(list(row_dict.values()))  # Append row values as a list

    # Construct the JSON data structure
    json_data = {"columns": column_names, "data": results}
    return json_data



# formatting data
def convert_to_json(rows, column_names):
    results = []
    for row in rows:
        row_dict = dict(zip(column_names, row))
        results.append(row_dict)

        # Serialize the results and column names into a JSON string
    json_data ={"columns": column_names, "data": results}
    return json_data


def json_to_markdown_table(json_data):
    # Extract columns and data from JSON
    columns = json_data["columns"]
    data = json_data["data"]

    # Generate Markdown table header
    markdown_table = "| " + " | ".join(columns) + " |\n"
    markdown_table += "| " + " | ".join(["---"] * len(columns)) + " |\n"

    # Generate Markdown table rows
    for row in data:
        markdown_table += "| " + " | ".join(str(row[column]) for column in columns) + " |\n"

    return markdown_table
