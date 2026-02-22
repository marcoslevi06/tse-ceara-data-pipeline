import io
import logging

from app.ingestao.tse_extrator import ExtratorDados
from app.storage.google_drive import GoogleDriveClient
from app.utils.vars_envs import Settings_Env

LOGGER = logging.getLogger(__name__)


def executar_pipeline_ingestao(ano: int, sigla_estado: str) -> None:
    """
    """

    LOGGER.info("Iniciando pipeline TSE (Consolidado)")
    LOGGER.info(f"Ano: {ano} | Estado: {sigla_estado}")

    extrator = ExtratorDados()
    drive = GoogleDriveClient(
        client_secret_file=Settings_Env.PATH_GOOGLE_OAUTH_CLIENT_SECRET,
        token_file="credenciais/token.pickle",
    )
    dados_brutos_folder_id = Settings_Env.ID_DADOS_BRUTOS_BUCKET_GOOGLE_DRIVE


    # 1. Raspagem (Supondo que retorna uma lista ou um link único)
    link = extrator.raspar_dados_tse(ano=ano, sigla_estado=sigla_estado)
    nome_arquivo = link.split("/")[-1]

    if drive.arquivo_existe(nome_arquivo=nome_arquivo, folder_id=dados_brutos_folder_id):
        LOGGER.info(f"O arquivo {nome_arquivo}  já existe no DataLake. Parando processo.")
        return
    
    
    arquivo_completo = io.BytesIO()
    try:
        for fatia_dados, num_parte in extrator.baixar_em_fatias(link):
            LOGGER.info(f"Recebendo parte {num_parte}")
            arquivo_completo.write(fatia_dados)

        arquivo_completo.seek(0)
        LOGGER.info(f"Enviando {nome_arquivo} completo para o DataLake...")
        file_id = drive.upload_buffer(
            buffer=arquivo_completo,
            file_name=nome_arquivo,
            folder_drive_id=dados_brutos_folder_id
        )
        if file_id:
            LOGGER.info(f"Arquivo salvo com ID: {file_id}")

    except Exception as error:
        LOGGER.info(f"[executar_pipeline_ingestao] - Erro: {error}")
    finally:
        arquivo_completo.close()

    LOGGER.info(f"Pipeline de ingestão finalizado com sucesso {nome_arquivo}. ")