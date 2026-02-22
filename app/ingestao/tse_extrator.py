import os
import logging
from curl_cffi import requests
from bs4 import BeautifulSoup
from app.utils.vars_envs import Settings_Env

import logging
LOGGER = logging.getLogger(__name__)


class ExtratorDados():
    """Extrai os dados eleitorais de um determimnado estado e ano postos no site do TSE"""

    def __init__(self):
        """
            Parâmetros:
                BASE_URL = f"https://dadosabertos.tse.jus.br/dataset/resultados-{ano_a_ser_buscado}"
                estado = Sigla do estado a ser buscado no site do TSE. Exemplo de padrão para identificação do arquivo: CE - Votação por seção eleitoral - 2002
        """
        self.BASE_URL = f"https://dadosabertos.tse.jus.br/dataset/resultados-"
        self.BASE_CAPTURA = f"base_captura/TSE/"


    def raspar_dados_tse(self, ano : int, sigla_estado : str):
        """
            Acessa o site do TSE, identifica um arquivo de votação pelo ano e estado,
            trata o html da página e devolve o link direto para download.

            Ex de documentos encontrados:
                Link TSE: https://dadosabertos.tse.jus.br/dataset/resultados-2002
                SIGLA_UF - Votação por seção eleitoral - YYYY.
                Ex: CE - Votação por seção eleitoral - 2002
        """

        LOGGER.info(f"{sigla_estado}: {ano}")

        url_pagina = f"{self.BASE_URL}{ano}"
        response = requests.get(url_pagina, headers=self.get_headers(), impersonate="chrome110")
        
        link_util = self.parse_response(response, sigla_estado)
        # caminho_destino = f"{self.BASE_CAPTURA}{sigla_estado}"
        # self.baixar_zip(url=link_util, pasta_destino=caminho_destino)
        return link_util


    def baixar_em_fatias(self, url, chunk_size_bytes=10 * 1024 * 1024):
        """
            Args:
                url (str):  Link direto para o download do arquivo (ex: dados do TSE).
                chunk_size_bytes (int): Tamanho desejado para cada fatia (part) gerada. 
                    O padrão é 10.485.760 bytes (10MB).

            Yields:
                tuple: Uma tupla contendo (bytes, int):
                    - buffer (bytes): O conteúdo binário da fatia atual.
                    - parte (int): O número sequencial da fatia (ex: 1, 2, 3...).

            Raises:
                requests.exceptions.HTTPError: Se a resposta do servidor for um erro (4xx ou 5xx).        
        """
        chunk_size_padrao = 1024 * 1024 # 1 Mb = 1024 * 1024 bytes
        response = None
        try:
            response = requests.get(url, headers=self.get_headers(), stream=True, impersonate="chrome110")
            response.raise_for_status()
            
            parte = 1
            buffer = b""
            
            LOGGER.info(f"Extraindo/baixando {parte}ª parte...")
            for chunk in response.iter_content(chunk_size=chunk_size_padrao):
                if chunk:
                    buffer += chunk
                
                if len(buffer) >= chunk_size_bytes:
                    yield buffer, parte
                    buffer = b"" 
                    parte += 1
            
            if buffer:
                yield buffer, parte
                    
        except Exception as e:
            print(f"Erro no stream de fatias: {e}")
        finally:
            if response:
                response.close() # Garante o fechamento da conexão.


    def parse_response(self, response : str, uf_estado : str):
        """
            Filtra e trata o HTML devolvendo a URL limpa do arquivo a ser baixado
        """
        uf_estado_a_ser_baixado = uf_estado
        link_final = None
        
        soup = BeautifulSoup(response.text, "html.parser")
        modulo_content = soup.find("ul", class_="resource-list")
        resource_item = modulo_content.find_all("li", class_="resource-item")

        for recurso in resource_item:
            ancora_titulo = recurso.find('a', class_='heading')
            titulo = ancora_titulo.get('title', '') if ancora_titulo else ""

            if f"{uf_estado_a_ser_baixado} -" in titulo:
                    LOGGER.info(f"Recurso encontrado: {titulo}")
                    ancora_download = recurso.find('a', class_='resource-url-analytics')

                    if ancora_download:
                        link_final = ancora_download.get('href')
                        LOGGER.info(f"Link Final: {link_final}")
                        print("=" * 15, end="\n\n")
                        
                    if link_final:
                        return link_final


    def baixar_zip(self, url: str, pasta_destino: str) -> str:
        """"""
        os.makedirs(pasta_destino, exist_ok=True)

        nome_arquivo = url.split("/")[-1]
        caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)

        LOGGER.info(f"Baixando arquivo: {nome_arquivo}")

        r = requests.get(
            url,
            headers=self.get_headers(),
            impersonate="chrome110",
            stream=True
        )

        with open(caminho_arquivo, "wb") as f:
            for chunk in r.iter_content(chunk_size=1_048_576):  # ! Mb por chunk.
                if chunk:
                    f.write(chunk)

        LOGGER.info(f"Download concluído: {caminho_arquivo}")
        return caminho_arquivo


    def get_headers(self):
        """Cabeçalho da requisição"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        return headers