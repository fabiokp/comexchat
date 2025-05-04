from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List, Optional # Make sure Dict, List, Optional are imported

# Initialize FastMCP server
mcp = FastMCP("comexstat")


# Define these constants or import them
COMEXSTAT_API_BASE = "https://api-comexstat.mdic.gov.br"  # Replace with the actual base URL
USER_AGENT = "MCP/1.0 (your-email@example.com)" # Replace with a descriptive User-Agent

async def _fetch_comexstat_data(url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Internal helper function to perform the HTTP request and basic error handling.

    Args:
        url: The API endpoint URL.
        payload: The request payload (body).
        headers: The request headers.

    Returns:
        The parsed JSON response as a dictionary, or None if an error occurs.
    """
    async with httpx.AsyncClient(verify=False) as client:
        try:
            print(f"Sending request to {url} with payload: {payload}") # Debug print
            response = await client.post(url, json=payload, headers=headers, timeout=60.0) # Increased timeout
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            data = response.json()
            print("Request successful, returning JSON data.")
            return data
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
            try:
                print(f"Error details: {exc.response.json()}") # Try to print JSON error details
            except Exception:
                pass # Ignore if response body is not JSON
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}: {exc}")
        except Exception as e:
            print(f"An unexpected error occurred during data fetching: {e}")
        return None
    

@mcp.tool()
async def dados_gerais(
    flow: str = "import",
    monthDetail: bool = False,
    period: Dict[str, str] = {"from": "2024-01", "to": "2024-12"},
    filters: Optional[List[Dict[str, Any]]] = None,
    details: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """
    Busca dados gerais da API ComexStat de forma assíncrona
    e os retorna como um dicionário (estrutura JSON).
    Filtros de mês ou ano devem ser aplicados apenas no argumento 'period'. Nunca informe 'year' com filters ou details.
    O ID do país nos filtros deve ser sempre uma string de 3 dígitos, ex: '063' para Argentina.

    Args:
        flow (str): Fluxo desejado ("import" ou "export"). Padrão: "import". Nunca 'both'.
        monthDetail (bool): Booleano indicando se detalhes mensais devem ser incluídos. Padrão: False.
        period (Dict[str, str]): Dicionário com itens "from" e "to" no formato "AAAA-MM".
                                 O intervalo "MM" indica os meses que serão recuperados para todos os anos "AAAA" no intervalo.
                                 Padrão: {"from": "2024-01", "to": "2024-12"}.
        filters (Optional[List[Dict[str, Any]]]): Lista opcional de filtros. Cada filtro é um dicionário
                                                  com "filter" e "values". 
                                                  Exemplo: [{"filter": "country", "values": ['063']}]. Nunca 'year'.
                                                  Padrão: None.
        details (Optional[List[str]]): Lista opcional de detalhes desejados. 
                                       Exemplos: ("country", "state", "SITCGroup", etc.). 
                                       Padrão: None.
        metrics (Optional[List[str]]): Lista opcional de métricas desejadas. Exemplos: ("metricFOB", "metricKG", etc.).
                                       Padrão: None.

        Filtros e detalhes disponíveis:
          'country': Países
          'economicBlock': Blocos
          'state': UF do produto
          'via': Via
          'urf': URF
          'SITCSection': CUCI Seção
          'SITCDivision': CUCI Capítulo
          'SITCGroup': CUCI Grupo (produto)         
          'SITCSubGroup': CUCI Subposição
          'SITCBasicHeading': CUCI Item
          'ncm': NCM - Nomenclatura Comum do Mercosul
          'subHeading': Subposição do Sistema Harmonizado (SH6) 
          'heading': Posição do Sistema Harmonizado (SH4)
          'chapter': Capítulo do Sistema Harmonizado (SH2)
          'section': Seção do Sistema Harmonizado
          'BECLevel1': CGCE Nível 1
          'BECLevel2': CGCE Nível 2
          'BECLevel3': CGCE Nível 3
          'ISICSection': ISIC Seção 
          'ISICDivision': ISIC Divisão
          'ISICGroup': ISIC Grupo
          'ISICClass': ISIC Classe

        Métricas disponíveis:
          'metricFOB': Valor US$ FOB
          'metricKG': Quilograma Líquido
          'metricStatistic': Quantidade Estatística (depende do filtro 'ncm')
          'metricFreight': US$ Frete (depende de flow='import')
          'metricInsurance': US$ Seguro (depende de flow='import')
          'metricCIF': Valor US$ CIF (depende de flow='import')

    Returns:
        Optional[Dict[str, Any]]: Um dicionário contendo os dados buscados em sua estrutura JSON original
                                  (especificamente a lista sob a chave 'data'), ou None se ocorrer um erro.
    """
    url = f"{COMEXSTAT_API_BASE}/general" # Ensure the endpoint path starts with /

    # Modify details: remove 'year' if present using list comprehension
    details = [d for d in (details if details is not None else []) if d != 'year']

    # Construct payload using potentially modified arguments
    payload = {
        "flow": flow,
        "monthDetail": monthDetail,
        "period": period,
        "filters": filters, # Use modified filters
        "details": details, # Use modified details
        "metrics": metrics if metrics is not None else []
    }


    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT  # Use the defined User-Agent
    }

    # Call the helper function to fetch data
    data = await _fetch_comexstat_data(url, payload, headers)

    if data:
        print("Data fetched successfully, returning raw JSON structure.")
        return data.get("data", {}).get("list")  # Return the 'data' key if it exists, else return an empty dict
    else:
        # _fetch_comexstat_data already prints errors
        print("Returning None due to fetch error.")
        return None


@mcp.tool()
async def dados_municipio(
    flow: str = "export",
    monthDetail: bool = False,
    period: Dict[str, str] = {"from": "2024-01", "to": "2024-12"},
    filters: Optional[List[Dict[str, Any]]] = None,
    details: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """
    Busca dados em nível de município da API ComexStat de forma assíncrona
    e os retorna como um dicionário (estrutura JSON).
    Filtros de mês ou ano devem ser aplicados apenas no argumento 'period'. Nunca informe 'year' com filters ou details.
    O ID do país nos filtros deve ser sempre uma string de 3 dígitos, ex: '063' para Argentina.

    Args:
        flow (str): Fluxo desejado ("import" ou "export"). Nunca "both". Padrão: "export".
        monthDetail (bool): Booleano indicando se detalhes mensais devem ser incluídos. Padrão: False.
        period (Dict[str, str]): Dicionário com itens "from" e "to" no formato "AAAA-MM".
                                 O intervalo "MM" indica os meses que serão recuperados para todos os anos "AAAA" no intervalo.
                                 Padrão: {"from": "2024-01", "to": "2024-12"}.
        filters (Optional[List[Dict[str, Any]]]): Lista opcional de filtros. Cada filtro é um dicionário
                                                  com "filter" e "values". .
                                                  Exemplo: [{"filter": "state", "values": [26]}]. Nunca 'year'
                                                  Padrão: None.
        details (Optional[List[str]]): Lista opcional de detalhes desejados. 
                                       Exemplos: ("country", "state", "city", "heading", etc.). Nunca 'year'.
                                       Padrão: None.
        metrics (Optional[List[str]]): Lista opcional de métricas desejadas. Exemplos: ("metricFOB", "metricKG").
                                       Padrão: None.

        Filtros e detalhes disponíveis:
          'country': Países
          'economicBlock': Blocos
          'state': UF do município
          'city': Munícipio
          'heading': Posição (SH4)
          'chapter': Capítulo (SH2)
          'section': Seção

        Métricas disponíveis:
          'metricFOB': Valor US$ FOB
          'metricKG': Quilograma Líquido


    Returns:
        Optional[Dict[str, Any]]: Um dicionário contendo os dados buscados em sua estrutura JSON original
                                  (especificamente a lista sob a chave 'data'), ou None se ocorrer um erro.
    """
    url = f"{COMEXSTAT_API_BASE}/cities" # Endpoint for city data

    # Modify details: remove 'year' if present using list comprehension
    details = [d for d in (details if details is not None else []) if d != 'year']


    # Construct payload using potentially modified arguments
    payload = {
        "flow": flow,
        "monthDetail": monthDetail,
        "period": period,
        "filters": filters, # Use modified filters
        "details": details, # Use modified details
        "metrics": metrics if metrics is not None else []
    }



    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT
    }

    # Call the helper function to fetch data
    data = await _fetch_comexstat_data(url, payload, headers)

    if data:
        # Return the relevant part of the JSON response
        # Adjust data.get("data", {}).get("list") if the structure is different
        print("City data fetched successfully, returning raw JSON structure.")
        return data.get("data", {}).get("list") # Return the list part of the data
    else:
        # _fetch_comexstat_data already prints errors
        print("Returning None due to fetch error for city data.")
        return None

# Example usage for dados_municipio (assuming async context)
# async def main_municipio():
#     # Example using default parameters from the original snippet
#     df_municipio_default = await dados_municipio()
#     print("--- Municipio Data (Default Filters) ---")
#     print(df_municipio_default.head())

#     # Example with custom parameters
#     df_municipio_custom = await dados_municipio(
#         flow="import",
#         period={"from": "2024-01", "to": "2024-12"},
#         filters=[{"filter": "state", "values": [35]}], # Sao Paulo state
#         details=["city", "heading"],
#         metrics=["metricFOB"]
#     )
#     print("\n--- Municipio Data (Custom Filters) ---")
#     print(df_municipio_custom.head())

# if __name__ == "__main__":
#     import asyncio
#     # asyncio.run(main()) # Keep example for dados_gerais if needed
#     asyncio.run(main_municipio())


# Example usage (assuming an async context like Jupyter or using asyncio.run)
# async def main():
#     df = await dados_gerais(details=["country"], metrics=["metricFOB"])
#     print(df.head())
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())

# Now call the function directly using await in a separate cell
# await fetch_comexstat_data()


@mcp.tool()
async def fetch_auxiliary_table(
    table_name: str,
    add: Optional[str] = None,
    language: Optional[str] = None,
    page: Optional[int] = None,
    perPage: Optional[int] = None,
    search: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Busca uma tabela auxiliar da API ComexStat de forma assíncrona
    e a retorna como um dicionário (estrutura JSON original).
    Filtros de mês ou ano devem ser aplicados apenas no argumento 'period'. Nunca informe 'year' com filters ou details.
    Procura códigos ou descrições em tabelas auxiliares do ComexStat como países, produto CUCI, UF, etc.
    Forneça o nome da tabela e o valor a ser pesquisado.

    Esta função suporta parâmetros de consulta opcionais para tabelas específicas.

    Args:
        table_name: O nome do endpoint da tabela. Opções disponíveis:
                    - "countries": Lista de países e seus códigos.
                    - "uf": Lista de Unidades Federativas (Estados) brasileiras e seus códigos.
                    - "cities": Lista de Cidades brasileiras e seus códigos.
                    - "ways": Lista de vias de transporte (ex: marítima, aérea) e seus códigos.
                    - "urf": Lista de Unidades da Receita Federal e seus códigos.
                    - "economic-blocks": Lista de blocos econômicos (ex: Mercosul) e seus códigos.
                    - "product-categories": Lista de códigos e descrições de produtos CUCI (Classificação por Grandes Categorias Econômicas), equivalente a SITC.
                    - "ncm": Lista de códigos e descrições da NCM (Nomenclatura Comum do Mercosul).
                    - "hs": Lista de códigos e descrições do SH (Sistema Harmonizado).
                    - "nbm": Lista de códigos e descrições da NBM (Nomenclatura Brasileira de Mercadorias).
                    - "classifications": Lista de códigos e descrições de produtos CGCE (Classificação por Grandes Categorias Econômicas).
        add (Optional[str]): Parâmetro adicional (aplicável a: ncm, hs, nbm,
                             classifications, product-categories, economic-blocks).
        language (Optional[str]): Código do idioma ('pt', 'en', 'es') (aplicável a: ncm, hs,
                                  nbm, classifications, product-categories, economic-blocks).
        page (Optional[int]): Número da página para paginação (aplicável a: product-categories, ncm, hs, nbm,classifications).
        perPage (Optional[int]): Itens por página para paginação (aplicável a: product-categories, ncm, hs, nbm, classifications).
        search (Optional[str]): Termo de busca (aplicável a: product-categories, ncm, hs, nbm, classifications, economic-blocks).

    Returns:
        Optional[Dict[str, Any]]: Um dicionário contendo os dados da tabela em sua estrutura JSON original,
                                  ou None se ocorrer um erro.
    """
    url = f"{COMEXSTAT_API_BASE}/tables/{table_name}"
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT
    }

    # Define which tables support which parameters
    params_all = {"product-categories", "ncm", "hs", "nbm", "classifications"}
    params_limited = {"economic-blocks"}

    # Build query parameters dictionary
    query_params: Dict[str, Any] = {}
    if table_name in params_all or table_name in params_limited:
        if add is not None:
            query_params["add"] = add
        if language is not None:
            query_params["language"] = language
        if search is not None:
            query_params["search"] = search

    if table_name in params_all:
        if page is not None:
            query_params["page"] = page
        if perPage is not None:
            query_params["perPage"] = perPage

    async with httpx.AsyncClient(verify=False) as client:
        try:
            print(f"Fetching '{table_name}' table from {url} with params: {query_params}") # Debug print
            response = await client.get(url, headers=headers, params=query_params, timeout=60.0) # Pass params here
            response.raise_for_status()
            data = response.json()
            print(f"'{table_name}' table data received, returning raw JSON structure.")
            return data.get("data").get("list") # Return the raw JSON data

        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred while fetching '{table_name}' table: {exc.response.status_code} - {exc.response.text}")
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting '{table_name}' table {exc.request.url!r}: {exc}")
        except Exception as e:
            # This catches JSON decoding errors, etc.
            print(f"An unexpected error occurred during '{table_name}' table fetching: {e}")

    # Return None if fetching failed
    print(f"Returning None for '{table_name}' table due to previous errors.")
    return None



@mcp.tool()
async def fetch_single_item_detail(table_name: str, item_id: Any) -> Optional[Dict[str, Any]]:
    """
    Função auxiliar interna para buscar detalhes de um único item de uma tabela auxiliar.
    Fornece informações adicionais, descrição ou códigos para um país, cidade/município, produto, etc.
    Args:
        table_name: O nome do endpoint da tabela. Opções válidas e o tipo de item_id correspondente:
                    - "uf": Busca informações detalhadas para uma Unidade Federativa (UF) brasileira específica. `item_id` deve ser o ID da UF (inteiro).
                    - "cities": Busca informações detalhadas para uma Cidade brasileira específica:
                            'coMunGeo' (ID da Cidade, inteiro), 'noMun' (Nome da cidade em maiúsculas), 'noMunMin' (Nome da cidade), 'sgUf' (Sigla do estado, string).
                            `item_id` deve ser o ID da Cidade (inteiro).
                    - "countries": Busca informações detalhadas para um País específico:
                            'id' (ID inteiro do país no banco de dados ComexStat), 'country' (nome do país no idioma selecionado),
                                'coPaisIson3' (código do país em ISO 3166-1 alfa-3), 'coPaisIson' (código do país em ISO 3166-1 numérico).
                            `item_id` deve ser o ID do País (inteiro).
                    - "ways": Busca informações detalhadas para uma Via de transporte específica: 'coVia' (ID da Via, inteiro) e 'noVia' (Nome da Via)
                            `item_id` deve ser o ID da Via (coVia, inteiro).
                    - "urf": Busca informações detalhadas para uma Unidade da Receita Federal (URF) específica: 'coUrf' (ID da UF ou URF, inteiro) e 'noUrf' (Nome da UF ou URF)
                            `item_id` deve ser o ID da URF (coUrf, inteiro).
                    - "nbm": Busca informações detalhadas para um código NBM específico: 'coNbm' (ID NBM, inteiro) e 'nbm' (Descrição NBM).
                            `item_id` deve ser o ID NBM (coNbm, inteiro).
                    - "ncm": Busca informações detalhadas para um código NCM específico: 'id' (ID NCM, inteiro) e 'text' (Descrição NCM).
                            `item_id` deve ser o ID NCM (coNcm, inteiro).

        item_id: O ID específico do item a ser buscado, correspondente ao `table_name`.
    Returns:
        Optional[Dict[str, Any]]: Um dicionário contendo os detalhes do item em sua estrutura JSON original,
                                  ou None se ocorrer um erro ou o item não for encontrado.
    """
    url = f"{COMEXSTAT_API_BASE}/tables/{table_name}/{item_id}"
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT
    }
    async with httpx.AsyncClient(verify=False) as client:
        try:
            print(f"Fetching detail for item '{item_id}' from '{table_name}' table at {url}")
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            print(f"Detail for item '{item_id}' received, extracting item data.")
            item_data = None
            # Check if the response itself is the data dictionary
            if isinstance(data, dict):
                 # Check if there's a 'data' key containing the actual item dictionary
                if 'data' in data and isinstance(data['data'], dict):
                    item_data = data['data']
                else:
                    # Assume the top-level dictionary is the item data if 'data' key isn't present or isn't a dict
                    # This might need adjustment based on actual API response structure for single items
                    item_data = data
            if item_data:
                print(f"Detail for item '{item_id}' successfully extracted.")
                return item_data # Return the dictionary directly
            else:
                print(f"Could not extract item data dictionary from the response for '{table_name}/{item_id}'.")
                print(f"Response JSON structure: {data}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                print(f"Item '{item_id}' not found in '{table_name}' table (404).")
            else:
                print(f"HTTP error occurred while fetching detail for '{table_name}/{item_id}': {exc.response.status_code} - {exc.response.text}")
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting detail for '{table_name}/{item_id}' {exc.request.url!r}: {exc}")
        except Exception as e:
            print(f"An unexpected error occurred during detail fetching or processing for '{table_name}/{item_id}': {e}")
    print(f"Returning None for detail of '{table_name}/{item_id}' due to previous errors.")
    return None


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')