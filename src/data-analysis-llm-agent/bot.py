import asyncio
import logging
import os
import json


import httpx
from openai import AsyncOpenAI

logging.info(f"User message")

model = "gpt-3.5-turbo-1106"
client = AsyncOpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")
)

# Main chatbot class
class ChatBot:
    def __init__(self, system, tools, tool_functions):
        self.system = system
        self.tools = tools
        self.exclude_functions = ["plot_chart"]
        self.tool_functions = tool_functions
        self.messages = []
        if self.system:
            self.messages.append({"role": "system", "content": system})

    async def __call__(self, message):
        self.messages.append({"role": "user", "content": f"""{message}"""})
        response_message = await self.execute()
        # for function call sometimes this can be empty
        if response_message.content:
            self.messages.append({"role": "assistant", "content": response_message.content})

        logging.info(f"User message: {message}")
        logging.info(f"Assistant response: {response_message.content}")

        return response_message

    async def execute(self):
        #print(self.messages)
        completion = await client.chat.completions.create(
            model=model,
            messages=self.messages,
            tools = self.tools
        )
        print(completion)
        assistant_message = completion.choices[0].message

        return assistant_message

    async def call_function(self, tool_call):
        function_name = tool_call.function.name
        function_to_call = self.tool_functions[function_name]
        function_args = json.loads(tool_call.function.arguments)
        logging.info(f"Calling {function_name} with {function_args}")
        function_response = await function_to_call(**function_args)

        return {
            "tool_call_id": tool_call.id,
            "role": "tool",
            "name": function_name,
            "content": function_response,
        }

    async def call_functions(self, tool_calls):

        # Use asyncio.gather to make function calls in parallel
        function_responses = await asyncio.gather(
            *(self.call_function(tool_call) for tool_call in tool_calls)
            )

        # Extend conversation with all function responses
        responses_in_str = [{**item, "content": str(item["content"])} for item in function_responses]

        # Log each tool call object separately
        for res in function_responses:
            logging.info(f"Tool Call: {res}")

        self.messages.extend(responses_in_str)

        response_message = await self.execute()
        return response_message, function_responses
