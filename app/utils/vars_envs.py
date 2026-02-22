from dotenv import load_dotenv
import os
load_dotenv()

class Settings_Env:
    """
        Classe que gerencia e centraliza as variáveis de ambiente.
    """

    PATH_GOOGLE_OAUTH_CLIENT_SECRET= os.getenv("PATH_GOOGLE_OAUTH_CLIENT_SECRET") # caminho local onde as credenciais estão salvas para realizar autenticação.
    PATH_TOKEN_PICKLE = os.getenv("PATH_TOKEN_PICKLE")

    ID_DADOS_BRUTOS_BUCKET_GOOGLE_DRIVE = os.getenv("ID_DADOS_BRUTOS_BUCKET_GOOGLE_DRIVE") # id do bucket onde os dados brutos serão salvos.
    ID_PASTA_BRONZE = os.getenv("ID_PASTA_BRONZE")
    ID_PASTA_SILVER = os.getenv("ID_PASTA_SILVER")
    ID_PASTA_GOLD = os.getenv("ID_PASTA_GOLD")