import io
from app.storage.google_drive import GoogleDriveClient
from app.utils.vars_envs import Settings_Env
import pandas as pd
import zipfile

import logging
LOGGER = logging.getLogger(__name__)


def transformar_para_parquet(zip_bytes: bytes) -> bytes:
    """
        Extrai o conteúdo de um ZIP e converte para formato Parquet.
    """

    with io.BytesIO(zip_bytes) as buffer_entrada:
        with zipfile.ZipFile(buffer_entrada) as z:
            # Localiza o arquivo de dados dentro do ZIP (csv ou txt)
            nome_interno = [n for n in z.namelist() if n.endswith(('.csv', '.txt'))][0]
            
            with z.open(nome_interno) as f:
                # Lendo com os padrões do TSE (Separador ; e Encoding Latin-1)
                df = pd.read_csv(f, sep=';', encoding='latin-1', low_memory=False)
                
                # Criando o buffer para o Parquet
                buffer_saida = io.BytesIO()
                df.to_parquet(buffer_saida, index=False, engine='pyarrow')
                return buffer_saida.getvalue()


def executar_pipeline_bronze():
    """

    """
    LOGGER.info("Iniciando pipeline BRONZE")

    drive = GoogleDriveClient(
        client_secret_file=Settings_Env.PATH_GOOGLE_OAUTH_CLIENT_SECRET,
        token_file=Settings_Env.PATH_TOKEN_PICKLE,
    )

    pasta_raw = Settings_Env.ID_DADOS_BRUTOS_BUCKET_GOOGLE_DRIVE
    pasta_bronze_id = Settings_Env.ID_PASTA_BRONZE

    arquivos_raw = drive.listar_arquivos(folder_id=pasta_raw)

    LOGGER.info(f"{len(arquivos_raw)} arquivos encontrados na RAW")

    for arquivo in arquivos_raw:
        nome_parquet = arquivo["name"].replace(".zip", ".parquet")
        LOGGER.info(f"Processando: {nome_parquet}")

        if drive.arquivo_existe(nome_arquivo=nome_parquet, folder_id=pasta_bronze_id):
            LOGGER.info(f"Pulando arquivo {pasta_bronze_id}, já existe na camada bronze.")
            continue

        conteudo_zip = drive.download_file(arquivo["id"])

        conteudo_parquet = transformar_para_parquet(zip_bytes=conteudo_zip)

        with io.BytesIO(conteudo_parquet) as final_buffer:
            drive.upload_buffer(buffer=final_buffer, file_name=nome_parquet, folder_drive_id=pasta_bronze_id)
        
    LOGGER.info("Pipeline BRONZE finalizado")
