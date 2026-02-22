import io
import logging
from app.storage.google_drive import GoogleDriveClient
from app.processamento.silver_transformer import transformar_bronze_para_silver
from app.utils.vars_envs import Settings_Env

LOGGER = logging.getLogger(__name__)

def executar_pipeline_silver():
    LOGGER.info("Iniciando Pipeline Silver...")
    
    drive = GoogleDriveClient(
        client_secret_file=Settings_Env.PATH_GOOGLE_OAUTH_CLIENT_SECRET,
        token_file=Settings_Env.PATH_TOKEN_PICKLE
    ) 
    
    id_bronze = Settings_Env.ID_PASTA_BRONZE
    id_silver = Settings_Env.ID_PASTA_SILVER

    # 1. Lista arquivos Parquet na Bronze
    arquivos_bronze = drive.listar_arquivos(id_bronze)

    for arq in arquivos_bronze:
        if arq['name'].endswith('.parquet'):
            nome_silver = arq['name'].replace(".parquet", "_silver.parquet")

            # 2. Verifica se já foi processado
            if drive.arquivo_existe(nome_silver, id_silver):
                LOGGER.info(f"PULANDO: {nome_silver} já existe.")
                continue

            try:
                # 3. Download da Bronze
                bytes_bronze = drive.download_file(arq['id'])
                
                # 4. Transformação (Limpeza e Regras de Negócio)
                df_silver = transformar_bronze_para_silver(io.BytesIO(bytes_bronze))

                # 5. Upload para Silver
                buffer_silver = io.BytesIO()
                df_silver.to_parquet(buffer_silver, index=False)
                buffer_silver.seek(0)

                drive.upload_buffer(buffer_silver, nome_silver, id_silver)
                LOGGER.info(f"✅ SUCESSO: {nome_silver} gerado.")

            except Exception as e:
                LOGGER.error(f"❌ Erro ao processar silver para {arq['name']}: {e}")