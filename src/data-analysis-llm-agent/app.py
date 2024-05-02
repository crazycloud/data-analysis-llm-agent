import chainlit as cl
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv("../.env")
from plotly.graph_objs import Figure

from utils import generate_sqlite_table_info_query, format_table_info
from tools import tools_schema, run_sqlite_query, plot_chart
from bot import ChatBot

# Configure logging
logging.basicConfig(filename='chatbot.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger()
logger.addHandler(logging.FileHandler('chatbot.log'))

MAX_ITER = 5
schema_table_pairs = []

tool_run_sqlite_query = cl.step(type="tool", show_input="json", language="str")(run_sqlite_query)
tool_plot_chart = cl.step(type="tool", show_input="json", language="json")(plot_chart)
original_run_sqlite_query = tool_run_sqlite_query.__wrapped__
# cl.instrument_openai() 
# for automatic steps

@cl.on_chat_start
async def on_chat_start():
    # build schema query
    table_info_query = generate_sqlite_table_info_query(schema_table_pairs)

    # execute query
    result, column_names = await original_run_sqlite_query(table_info_query, markdown=False)

    # format result into string to be used in prompt
    # table_info = format_table_info(result, column_names)
    table_info = '\n'.join([item[0] for item in result])

    system_message = f"""You are an expert in data analysis. You will provided valuable insights for business user based on their request.
    Before responding, You will make sure that user ask pertains to data analysis on provided schema, else decline.
    If user request some data, you will build sql query based on the user request for sqlite db from the provided schema/table details and call query_db tools to fetch data from database with the correct/relevant query that gives correct result.
    You have access to tool to execute database query and get results and to plot the query results.
    One you have provided the data, you will do reflection to see if you have provided correct data or not. because you don't know the data beforehand but only the schema so you might discover some new insights while reflecting.

    Follow this Guidelines
    - It is very important that if you need certain inputs to proceed or are not sure about anything, you may ask question, but try to use your intelligence to understand user intention and also let user know if you make assumptions.
    - In the response message do not provide technical details like sql, table or column details, the response will be read by business user not technical person.
    - provide rich markdown response - if it is table data show it in markdown table format
    - In case you get a database error, you will reflect and try to call the correct sql query
    - Limit top N queries to 5 and let the user know that you have limited results
    - Limit number of columns to 5-8. Wisely Choose top columns to query in SQL queries based on the user request
    - when user asks for all records - limit results to 10 and tell them they you are limiting records
    - in SQL queries to fetch data, you must cast date and numeric columns into readable form(easy to read in string format)
    - Design robust sql queries that takes care of uppercase, lowercase or some variations because you don't know the complete data or list of enumerable values in columns.
    - Pay careful attention to the schema and table details I have provided below. Only use columns and tables mentioned in the schema details

    Here are complete schema details with column details:
    {table_info}"""

    # print(system_message)
    
    tool_functions = {
        "query_db": tool_run_sqlite_query,
	    "plot_chart": tool_plot_chart
    }

    cl.user_session.set("bot", ChatBot(system_message, tools_schema, tool_functions))


@cl.on_message
async def on_message(message: cl.Message):
    bot = cl.user_session.get("bot")

    msg = cl.Message(author="Assistant", content="")
    await msg.send()

    # step 1: user request and first response from the bot
    response_message = await bot(message.content)
    msg.content = response_message.content or ""
    
    # pending message to be sent
    if len(msg.content)>0:
        await msg.update()


    # step 2: check tool_calls - as long as there are tool calls and it doesn't cross MAX_ITER count, call iteratively
    cur_iter = 0
    tool_calls = response_message.tool_calls
    while cur_iter <= MAX_ITER:

        # if tool_calls:
        if tool_calls:
            bot.messages.append(response_message) # add tool call to messages before calling executing function calls
            response_message, function_responses = await bot.call_functions(tool_calls)

            # response_message is response after completing function calls and sending it back to the bot
            if response_message.content and len(response_message.content)>0:
                await cl.Message(author="Assistant", content=response_message.content).send()

            # reassign tool_calls from new response
            tool_calls = response_message.tool_calls

            # some responses like charts should be displayed explicitly
            function_responses_to_display = [res for res in function_responses if res['name'] in bot.exclude_functions]
            for function_res in function_responses_to_display:
                # plot chart
                if isinstance(function_res["content"], Figure):
                    chart = cl.Plotly(name="chart", figure=function_res['content'], display="inline")
                    await cl.Message(author="Assistant", content="", elements=[chart]).send()
        else:
            break
        cur_iter += 1
