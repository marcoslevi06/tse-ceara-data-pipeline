import io
import logging
from app.storage.google_drive import GoogleDriveClient
from app.processamento.gold_transformer import transformar_silver_para_gold
from app.utils.vars_envs import Settings_Env

LOGGER = logging.getLogger(__name__)

def executar_pipeline_gold():
    """
    """
    
    LOGGER.info("Iniciando Camada Gold: Agregação de Resultados")
    
    drive = GoogleDriveClient(
        client_secret_file=Settings_Env.PATH_GOOGLE_OAUTH_CLIENT_SECRET,
        token_file=Settings_Env.PATH_TOKEN_PICKLE
    )
    
    id_silver = Settings_Env.ID_PASTA_SILVER
    id_gold = Settings_Env.ID_PASTA_GOLD

    # 1. Busca arquivos na Silver
    arquivos_silver = drive.listar_arquivos(id_silver)

    for arq in arquivos_silver:
        if "_silver.parquet" in arq['name']:
            nome_gold = arq['name'].replace("_silver.parquet", "_gold_municipio.parquet")

            if drive.arquivo_existe(nome_gold, id_gold):
                LOGGER.info(f"PULANDO: {nome_gold} já consolidado.")
                continue

            try:
                # 2. Download Silver -> RAM
                bytes_silver = drive.download_file(arq['id'])
                
                # 3. Transformação em Ouro (Agregação)
                df_gold = transformar_silver_para_gold(io.BytesIO(bytes_silver))

                # 4. Upload para Gold
                buffer_gold = io.BytesIO()
                df_gold.to_parquet(buffer_gold, index=False)
                
                buffer_gold.seek(0) 

                drive.upload_buffer(buffer_gold, nome_gold, id_gold)
                LOGGER.info(f"SUCESSO: Tabela Gold {nome_gold} gerada e salva em: {id_gold}")

            except Exception as e:
                LOGGER.error(f"Erro na Camada Gold para {arq['name']}: {e}")