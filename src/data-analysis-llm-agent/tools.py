import psycopg2
import sqlite3
import os
import plotly.graph_objs as go
import plotly.io as pio
from utils import convert_to_json, json_to_markdown_table

# function calling
# avialable tools
tools_schema = [
    {
        "type": "function",
        "function": {
            "name": "query_db",
            "description": "Fetch data from postgres database",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "complete and correct sql query to fulfil user request.",
                    }
                },
                "required": ["sql_query"],
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "plot_chart",
            "description": "Plot Bar or Linechart to visualize the result of sql query",
            "parameters": {
                "type": "object",
                "properties": {
                    "plot_type": {
                        "type": "string",
                        "description": "which plot type either bar or line or scatter",
                    },
                    "x_values": {
                        "type": "array",
                        "description": "list of x values for plotting",
                        "items": {
                            "type": "string"
                        }
                    },
                    "y_values": {
                        "type": "array",
                        "description": "list of y axis values for plotting",
                        "items": {
                            "type": "number"
                        }
                    },
                    "plot_title": {
                        "type": "string",
                        "description": "Descriptive Title for the plot",
                    },
                    "x_label": {
                        "type": "string",
                        "description": "Label for the x axis",
                    },
                    "y_label": {
                        "type": "string",
                        "description": "label for the y axis",
                    }
                },
                "required": ["plot_type","x_values","y_values","plot_title","x_label","y_label"],
            },
        }
    }
]


async def run_postgres_query(sql_query, markdown=True):
    connection = None  # Initialize connection variable outside the try block
    try:
        # Establish the connection
        connection = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        print("Connected to the database!")

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the column names
        column_names = [desc[0] for desc in cursor.description]

        # Fetch all rows
        result = cursor.fetchall()
        if markdown:
            # get result in json
            json_data = convert_to_json(result,column_names)
            markdown_data = json_to_markdown_table(json_data)

            return markdown_data

        return result, column_names
    except (Exception, psycopg2.Error) as error:
        print("Error while executing the query:", error)
        if markdown:
            return f"Error while executing the query: {error}"
        return [], []

    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


async def run_sqlite_query(sql_query, markdown=True):
    connection = None
    try:
        # Establish the connection
        db_path = os.path.join(os.path.dirname(__file__), '../data/movies.db')
        print(db_path)
        connection = sqlite3.connect(db_path)

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the column names
        column_names = [desc[0] for desc in cursor.description]

        # Fetch all rows
        result = cursor.fetchall()
        if markdown:
            # get result in json
            json_data = convert_to_json(result,column_names)
            markdown_data = json_to_markdown_table(json_data)
            return markdown_data

        return result, column_names
    except sqlite3.Error as error:
        print("Error while executing the query:", error)
        if markdown:
            return f"Error while executing the query: {error}"
        return [], []

    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("SQLite connection is closed")

async def plot_chart(x_values, y_values, plot_title, x_label, y_label, plot_type='line', save_path="tmp/tmp.png"):
    """
    Generate a bar chart, line chart, or scatter plot based on input data using Plotly.

    Parameters:
    x_values (array-like): Input values for the x-axis.
    y_values (array-like): Input values for the y-axis.
    plot_type (str, optional): Type of plot to generate ('bar', 'line', or 'scatter'). Default is 'line'.
    save_path (str, optional): Path to save the plot image locally. If None, the plot image will not be saved locally.

    Returns:
    str: Data URI of the plot image.
    """
    # Validate input lengths
    if len(x_values) != len(y_values):
        raise ValueError("Lengths of x_values and y_values must be the same.")

    # Define plotly trace based on plot_type
    # Define plotly trace based on plot_type
    if plot_type == 'bar':
        trace = go.Bar(x=x_values, y=y_values, marker=dict(color='#24C8BF', line=dict(width=1)))
    elif plot_type == 'scatter':
        trace = go.Scatter(x=x_values, y=y_values, mode='markers', marker=dict(color='#df84ff', size=10, opacity=0.7, line=dict(width=1)))
    elif plot_type == 'line':
        trace = go.Scatter(x=x_values, y=y_values, mode='lines+markers', marker=dict(color='#ff9900', size=8, line=dict(width=1)), line=dict(width=2, color='#ff9900'))

    # Create layout for the plot
    layout = go.Layout(
        title=f'{plot_title} {plot_type.capitalize()} Chart',
        title_font=dict(size=20, family='Arial', color='#333'),
        xaxis=dict(title=x_label, titlefont=dict(size=18), tickfont=dict(size=14), gridcolor='#f0f0f0'),
        yaxis=dict(title=y_label, titlefont=dict(size=18), tickfont=dict(size=14), gridcolor='#f0f0f0'),
        margin=dict(l=60, r=60, t=80, b=60),
        plot_bgcolor='#f8f8f8',
        paper_bgcolor='#f8f8f8'
    )

    # Create figure and add trace to it
    fig = go.Figure(data=[trace], layout=layout)

    return fig
