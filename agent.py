import asyncio
import sys
import streamlit as st
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
# from langgraph.prebuilt import create_react_agent # REMOVE: We are building manually
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, BaseMessage, SystemMessage # Added BaseMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages # Helper to add messages to state
from langgraph.prebuilt import ToolNode # Use prebuilt ToolNode
from typing import Annotated, Sequence # For state definition
import operator # For state definition
import traceback
import os
import datetime

# Get current date
now = datetime.date.today()
# Map month number to Portuguese name
month_names_pt = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
CURRENT_MONTH_NAME_PT = month_names_pt[now.month]
CURRENT_YEAR_STR = str(now.year)


# Load environment variables from .env file
#load_dotenv()
MODEL_NAME = "gpt-4.1-mini-2025-04-14" # Model name
SERVER_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "comexstat.py")
# --- MOVE CONSTANT HERE ---
TOOL_LOAD_TIMEOUT = 45.0 # Timeout for MCP operations like init and tool loading

# --- Custom System Prompt ---
# Define your custom instructions for the agent here
CUSTOM_SYSTEM_PROMPT = f"""
Você é um assistente útil especializado em dados de comércio exterior brasileiro (ComexStat).
Você tem acesso a ferramentas que podem consultar estatísticas gerais de comércio e procurar códigos em tabelas auxiliares (como países, Grupo CUCI (SITCGroup), etc.).
Quando uma pergunta for feita:
1. Primeiro, entenda a solicitação do usuário.
2. Sempre verifique nas ferramentas de tabelas auxiliares os códigos corretos de país e produto antes de inseri-los na ferramenta. Inclua na sua resposta estes parâmetros utilizados.
4. Formate os argumentos para a ferramenta corretamente.
5. Chame a ferramenta. 
6. Se encontrar um erro ao usar uma ferramenta, informe o usuário sobre o erro.
7. Com base na resposta da ferramenta, formule uma resposta clara e concisa para o usuário.
8. Sempre forneça a classificação, o código e a descrição do produto que você está usando na resposta.
8. Não faça perguntas ao usuário. Se necessário, peça para ele reformular a pergunta e sugira que forneça mais detalhes.


Orientações importantes:
- O ano atual é {CURRENT_YEAR_STR} e o mês atual é {CURRENT_MONTH_NAME_PT}.
- O parâmetro 'flow' é 'export' ou 'import'. NUNCA 'both'. 
- Os parâmetros 'details' e 'filters' NUNCA incluem 'year'.
- Os dados disponíveis vão de 1997 a {CURRENT_YEAR_STR}.
- Consulte o para o ano de {str(now.year - 1)}, a não ser que o usuário especifique outro período.
- Sempre indique o ano ou período de consulta na sua resposta.
- Ao fornecer uma lista de itens, formate-a como uma tabela markdown.
- Apresente os valores numéricos com o indicador de moeda apropriado, no formato "US$ [número]".
- Caso haja algum filtro ou detalhamento de produto na consulta, sempre inclua na resposta qual a classificação de utilizada (CUCI, NCM, Sistema Harmonizado, etc.) e os respectivos códigos.
- Consulte produtos pelo Grupo CUCI (SITCGroup na tabela auxiliar product-categories), a não ser que o usuário especifique outro sistema de classificação de produtos.
- Traduza os termos para o português. 
- Seja preciso e refira-se à fonte de dados (ComexStat) quando apropriado.


Conceitos importantes: 
- Corrente de comércio (ou corrente comercial) refere-se à soma das exportações (FOB) e importações (CIF). 
- Saldo comercial é a diferença entre exportações (FOB) e importações (CIF) (exportações FOB - importações CIF). 
- Corrente de comércio e saldo comercial podem se aplicar a um ano específico ou a um parceiro comercial. Por exemplo, "saldo comercial com a China em 2024", "corrente de comércio em 2020".


"""
# --- End Custom System Prompt ---


# --- End Configuration ---

# Set the event loop policy for Windows if needed (often required for subprocesses)
# This needs to be done *before* any asyncio code runs, especially when using Streamlit
if sys.platform == "win32":
    try:
        # Check if a policy is already set and if it's the correct one
        current_policy = asyncio.get_event_loop_policy()
        if not isinstance(current_policy, asyncio.WindowsProactorEventLoopPolicy):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            print("Setting WindowsProactorEventLoopPolicy for asyncio.")
        else:
            print("WindowsProactorEventLoopPolicy already set.")
    except Exception as e:
        st.error(f"Failed to set asyncio policy: {e}")


# --- Cache only the Model ---
@st.cache_resource
def get_model():
    """Loads and caches the ChatOpenAI model."""
    print("Loading ChatOpenAI model...")
    # Bind the stop sequence appropriate for function calling if needed (depends on model)
    # model = ChatOpenAI(model=MODEL_NAME, temperature=0.0).bind_tools(tools) # Bind tools later
    model = ChatOpenAI(model=MODEL_NAME, temperature=0.0)
    print("Model loaded.")
    return model

# --- Agent State Definition ---
class AgentState(dict):
    messages: Annotated[Sequence[BaseMessage], operator.add]

# --- Graph Node Functions ---

# Node to decide whether to call tools or end
def should_continue(state: AgentState) -> str:
    """Checks the latest AI message for tool calls."""
    # Ensure messages exist before accessing the last one
    if not state or not state.get('messages'):
         # This case should ideally not happen in a normal flow after the first message
         print("[WARN] should_continue called with empty state or no messages.")
         return "end"
    last_message = state['messages'][-1]
    # If there are no tool calls, finish
    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        return "end"
    # Otherwise call tools
    return "continue"

# Node that calls the LLM
def call_model(state: AgentState, model_with_tools, agent_prompt):
    """Invokes the LLM with the current state and prompt."""
    print(">>> Calling Model Node")
    # Create the prompt with current messages
    prompt_value = agent_prompt.invoke(state)
    # Call the model
    response = model_with_tools.invoke(prompt_value)
    print(f"<<< Model Response: {response}")
    # Return *only the new message* to be added to the state by the graph
    return {"messages": [response]} # MODIFIED LINE


# --- Heavily Modified run_agent_async ---
async def run_agent_async(model, user_query: str) -> str:
    """
    Establishes MCP connection, loads tools, creates a custom agent graph,
    runs invocation asynchronously using ainvoke, extracts the final response,
    and appends tool usage information.
    """
    print(f"\nInvoking agent with query: '{user_query}'")

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[SERVER_SCRIPT_PATH],
        # --- ADD environment variable ---
        env={"PYTHONUTF8": "1", **os.environ}, # Force UTF-8 for the subprocess
    )
    agent_result = "Error: Agent execution did not complete as expected."
    # TOOL_LOAD_TIMEOUT = 45.0 # REMOVE from here

    try:
        # ... (MCP connection, session, tool loading, graph building remain the same) ...
        print("Establishing MCP connection for agent run...")
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                print("MCP Session active.")

                # --- Load Tools and Build Graph within the active session ---
                app = None # Compiled graph
                try:
                    # ... (Initialization, Tool Loading, Model Binding, Prompt, Graph Building - all remain the same) ...
                    # --- Initialize session ---
                    try:
                        print("Initializing MCP session...")
                        # Now uses the module-level constant
                        await asyncio.wait_for(session.initialize(), timeout=TOOL_LOAD_TIMEOUT)
                        print("MCP session initialized.")
                    except asyncio.TimeoutError:
                         # Now uses the module-level constant
                         raise TimeoutError(f"Timeout ({TOOL_LOAD_TIMEOUT}s) occurred during MCP session initialization.")
                    except Exception as init_e:
                         raise RuntimeError(f"Error during MCP session initialization: {init_e}")

                    # --- Load tools ---
                    print("Loading tools using active session...")
                    try:
                        loaded_tools = await asyncio.wait_for(
                            load_mcp_tools(session),
                            # Now uses the module-level constant
                            timeout=TOOL_LOAD_TIMEOUT
                        )
                        if not loaded_tools:
                            raise ValueError("No tools loaded from the MCP server.")
                        tools = loaded_tools
                        print(f"Tools loaded successfully: {[tool.name for tool in tools]}")
                    except asyncio.TimeoutError:
                         # Now uses the module-level constant
                         raise TimeoutError(f"Timeout ({TOOL_LOAD_TIMEOUT}s) occurred during tool loading.")
                    except Exception as load_e:
                         raise RuntimeError(f"Error during tool loading: {load_e}")

                    # --- Bind tools to model ---
                    model_with_tools = model.bind_tools(tools)

                    # --- Define Prompt ---
                    agent_prompt = ChatPromptTemplate.from_messages(
                        [
                            SystemMessage(content=CUSTOM_SYSTEM_PROMPT),
                            MessagesPlaceholder(variable_name="messages"),
                        ]
                    )

                    # --- Define Graph Nodes ---
                    bound_call_model = lambda state: call_model(state, model_with_tools, agent_prompt)
                    tool_node = ToolNode(tools)

                    # --- Build Graph ---
                    print("Building agent graph...")
                    graph = StateGraph(AgentState)
                    graph.add_node("agent", bound_call_model)
                    graph.add_node("action", tool_node)
                    graph.set_entry_point("agent")
                    graph.add_conditional_edges(
                        "agent", should_continue, {"continue": "action", "end": END}
                    )
                    graph.add_edge("action", "agent")
                    app = graph.compile()
                    print("Agent graph compiled.")

                except (TimeoutError, ValueError, RuntimeError) as setup_e:
                    error_message = f"Error during setup: {setup_e}"
                    print(error_message)
                    traceback.print_exc()
                    return error_message
                except Exception as setup_e:
                    error_message = f"Unexpected error during setup: {setup_e}"
                    print(error_message)
                    traceback.print_exc()
                    return error_message


                # --- Run the agent graph using ainvoke ---
                if app:
                    agent_input = {"messages": [HumanMessage(content=user_query)]}
                    final_state = None

                    try:
                        print("Starting agent graph execution (using ainvoke)...")
                        final_state = await app.ainvoke(agent_input)
                        print("Agent execution finished.")
                        print(f"[DEBUG] Value of final_state after ainvoke: Type={type(final_state)}, Value={final_state}")

                        # --- Extract result AND tool calls from the FINAL state ---
                        final_content = None
                        tool_calls_info = [] # List to store formatted tool calls

                        if final_state and isinstance(final_state, dict) and "messages" in final_state:
                            messages = final_state["messages"]

                            # --- Extract Tool Calls from History ---
                            for message in messages:
                                if isinstance(message, AIMessage) and message.tool_calls:
                                    for tool_call in message.tool_calls:
                                        tool_name = tool_call.get("name")
                                        tool_args = tool_call.get("args")
                                        # Format the tool call info
                                        tool_calls_info.append(f"* **Tool:** `{tool_name}`\n* **Arguments:** `{tool_args}`")

                            # --- Extract Final Content ---
                            if messages and isinstance(messages[-1], AIMessage):
                                last_ai_message = messages[-1]
                                if not last_ai_message.tool_calls and hasattr(last_ai_message, 'content'):
                                    content = str(last_ai_message.content or "").strip()
                                    if content:
                                        print(f"[DEBUG] Extracted final content from last AIMessage: '{content[:100]}...'")
                                        final_content = content # Store final content separately
                                    else:
                                        print("[WARN] Final AIMessage has no content.")
                                        agent_result = "Agent finished, but the final message was empty."
                                elif last_ai_message.tool_calls:
                                     print("[WARN] Agent finished on an AIMessage with tool calls.")
                                     agent_result = "Agent finished unexpectedly while planning to use tools."
                                else:
                                     print("[WARN] Final AIMessage has no tool calls but no content attribute?")
                                     agent_result = "Agent finished, but couldn't extract content from the final message."
                            elif messages and isinstance(messages[-1], ToolMessage):
                                 last_tool_msg = messages[-1]
                                 if getattr(last_tool_msg, 'status', None) == 'error':
                                      tool_error_content = f"Tool '{last_tool_msg.name}' failed: {last_tool_msg.content}"
                                      print(f"[ERROR] {tool_error_content}")
                                      agent_result = f"An error occurred during tool execution: {tool_error_content}"
                                 else:
                                      print("[WARN] Agent finished after a successful tool execution without a final AI response.")
                                      agent_result = f"Agent finished after using tool '{last_tool_msg.name}', but didn't provide a final summary."
                            else:
                                print("[WARN] Final state has no messages or last message is not AI/Tool.")
                                agent_result = "Agent finished, but the final state is unexpected."

                            # --- Combine final content and tool calls ---
                            if final_content:
                                agent_result = final_content # Start with the main answer
                                if tool_calls_info:
                                    # Append tool call info if any were found
                                    agent_result += "\n\n---\n**Tools Used:**\n" + "\n\n".join(tool_calls_info)
                            # If final_content is None, agent_result might already be set to an error/warning message

                        else:
                            print("[WARN] final_state after ainvoke is not a valid dict with 'messages'.")
                            agent_result = "Agent execution did not produce the expected final state structure."

                    except Exception as agent_run_e:
                        error_message = f"An error occurred during agent execution (ainvoke): {agent_run_e}"
                        print(error_message)
                        traceback.print_exc()
                        agent_result = error_message

        print("MCP Session closed.")

    # ... (Outer exception handling remains the same) ...
    except ConnectionRefusedError:
        error_message = f"Error: Connection refused. Is the MCP server ({SERVER_SCRIPT_PATH}) running or startable?"
        print(error_message)
        agent_result = error_message
    except asyncio.TimeoutError:
         error_message = "Error: Timeout occurred during MCP connection/setup."
         print(error_message)
         agent_result = error_message
    except Exception as conn_e:
        error_message = f"An unexpected error occurred during MCP connection/setup phase: {conn_e}"
        print(error_message)
        traceback.print_exc()
        agent_result = error_message

    return agent_result

# --- Streamlit App ---

st.title("🚢 Bem-vinda(o) ao ComexChat!")
st.caption("💬 Consultas interativas às estatísticas brasileiras de comércio exterior")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Load Only Model (Cached) ---
# We load the model once. Agent Executor is created per query.
model = get_model()
if model is None:
    st.error("Failed to load the language model. The application cannot start.")
    st.stop()

# React to user input
if prompt := st.chat_input("Faça sua pergunta sobre os dados do ComexStat..."):
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Get agent response
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        message_placeholder.markdown("Pesquisando...")
        response = ""
        run_successful = False
        try:
            print("[Streamlit DEBUG] Calling asyncio.run(run_agent_async)...")
            # Pass the cached model to the run function
            returned_value = asyncio.run(run_agent_async(model, prompt)) # Pass model
            run_successful = True

            # --- Debugging and Response Handling (mostly unchanged) ---
            print(f"[Streamlit DEBUG] run_agent_async returned: '{returned_value}' (Type: {type(returned_value)})")
            if returned_value is not None:
                 response = str(returned_value).strip()
                 print(f"[Streamlit DEBUG] Response variable after strip: '{response}'")
                 if not response: # Check if empty after strip
                      response = "Error: Agent returned an empty response."
                      print("[Streamlit DEBUG] Response is empty after processing return value.")
            else:
                 response = "Error: Agent returned None."
                 print("[Streamlit DEBUG] Agent returned None.")

            # Display the processed response (could be success or error message from run_agent_async)
            if "error" in response.lower() or "failed" in response.lower():
                 message_placeholder.error(response) # Display errors using st.error
            else:
                 message_placeholder.markdown(response) # Display success

        except RuntimeError as e:
             # --- Handle asyncio loop issues with nest_asyncio (Retry logic) ---
             if "cannot be called from a running event loop" in str(e):
                 print("[Streamlit DEBUG] Detected running event loop. Applying nest_asyncio.")
                 try:
                     import nest_asyncio

                     nest_asyncio.apply()
                     print("[Streamlit DEBUG] Retrying asyncio.run(run_agent_async) after nest_asyncio...")
                     returned_value = asyncio.run(run_agent_async(model, prompt)) # Pass model on retry
                     run_successful = True

                     # --- Debugging and Response Handling (Retry Path) ---
                     print(f"[Streamlit DEBUG] run_agent_async (retry) returned: '{returned_value}' (Type: {type(returned_value)})")
                     if returned_value is not None:
                          response = str(returned_value).strip()
                          print(f"[Streamlit DEBUG] Response variable after retry strip: '{response}'")
                          if not response:
                               response = "Error: Agent returned an empty response on retry."
                               print("[Streamlit DEBUG] Response is empty after processing retry return value.")
                     else:
                          response = "Error: Agent returned None on retry."
                          print("[Streamlit DEBUG] Agent returned None on retry.")

                     # Display the processed response (Retry)
                     if "error" in response.lower() or "failed" in response.lower():
                          message_placeholder.error(response)
                     else:
                          message_placeholder.markdown(response)

                 except ImportError:
                     response = "Error: nest_asyncio not installed. Cannot run async agent in this Streamlit context."
                     message_placeholder.error(response)
                 except Exception as nested_e:
                     response = f"An error occurred even after applying nest_asyncio: {nested_e}"
                     message_placeholder.error(response)
                     traceback.print_exc()
             else:
                 response = f"A runtime error occurred: {e}"
                 message_placeholder.error(response)
                 traceback.print_exc()
        except Exception as e:
             response = f"An unexpected error occurred: {e}"
             message_placeholder.error(response)
             traceback.print_exc()

        # Ensure response has a value before adding to history if run wasn't successful
        # and response wasn't already set by an exception handler
        if not run_successful and not response:
             response = "An error occurred, and no specific message was captured."
             print("[Streamlit DEBUG] Setting default error message as response was empty after exception.")


    # Add assistant response to chat history (even if it's an error message)
    # Ensure content is always a string before adding
    st.session_state.messages.append({"role": "assistant", "content": str(response)})
