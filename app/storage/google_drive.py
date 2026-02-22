import os
import pickle
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import io


class GoogleDriveClient:
    """
        Classe responsável pela autenticação e envio de arquivos
        para o Google Drive usando OAuth 2.0
    """

    def __init__(self, client_secret_file: str,
                 token_file: str = "credenciais/token.pickle",
                 scopes: list[str] | None = None
                 ):
        """
            Attributes:
                scopes (list[str]): Lista de permissões solicitadas à API do Google Drive.
                client_secret_file (str): Caminho para o arquivo de credenciais OAuth.
                token_file (str): Caminho do arquivo onde o token de acesso será armazenado.
                credentials (Credentials): Credenciais autenticadas obtidas via OAuth 2.0.
                service: Objeto de serviço da API do Google Drive (versão v3).
        """

        self.scopes = scopes or ["https://www.googleapis.com/auth/drive"]
        self.client_secret_file = client_secret_file
        self.token_file = token_file

        self.credentials = self._authenticate()
        self.service = build("drive", "v3", credentials=self.credentials, cache_discovery=False)


    def upload_buffer(self, buffer: io.BytesIO, file_name: str, folder_drive_id: str) -> str:
        """Faz o upload de um objeto em memória (buffer) para o Drive.        
            Args:
                buffer (io.BytesIO): Um objeto do tipo fluxo de bytes contendo os dados 
                    a serem enviados. Deve estar posicionado no início (seek 0).
                file_name (str): O nome que o arquivo receberá dentro do Google Drive.
                folder_drive_id (str): O ID da pasta de destino no Google Drive 
                    (extraído da URL da pasta ou via API).

            Returns:
                str | None: O ID único do arquivo gerado no Google Drive em caso de sucesso, 
                    ou None caso ocorra uma falha no upload.

            Raises:
                googleapiclient.errors.HttpError: Caso ocorra um erro de permissão ou 
                    limite na API do Google.
        """
        metadata = {"name": file_name, "parents": [folder_drive_id]}
        media = MediaIoBaseUpload(buffer, mimetype="application/zip", resumable=True)
        
        try:
            file = self.service.files().create(
                body=metadata,
                media_body=media,
                fields="id"
            ).execute()
            return file.get("id")
        except Exception as e:
            print(f"Erro ao subir fatia: {e}")


    def _authenticate(self):
        """
            Realiza a autenticação OAuth 2.0 com o Google e gerencia o uso do token de acesso.

            O método tenta reutilizar credenciais salvas em disco. Caso o token esteja
            expirado, realiza a renovação automática. Se não houver credenciais válidas,
            inicia o fluxo interativo de autenticação via navegador.

            Após a autenticação, o token é persistido localmente para evitar novas
            autorizações em execuções futuras.

            Returns:
            Credentials: Credenciais autenticadas para acesso à API do Google Drive.
        """
        credencial = None

        if os.path.exists(self.token_file):
            with open(self.token_file, "rb") as token:
                credencial = pickle.load(token)

        if not credencial or not credencial.valid:
            if credencial and credencial.expired and credencial.refresh_token:
                credencial.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.client_secret_file,
                    self.scopes
                )
                credencial = flow.run_local_server(port=0)

            with open(self.token_file, "wb") as token:
                pickle.dump(credencial, token)

        # print("Autenticado com sucesso: ", credencial, end="\n\n")
        return credencial


    def download_file(self, file_id: str) -> bytes:
        """Baixa o arquivo do Drive para a memória RAM."""
        try:
            from googleapiclient.http import MediaIoBaseDownload
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            return fh.getvalue()
        except Exception as error:
            # LOGGER.error(f"Erro no download do ID {file_id}: {e}")
            raise f"{error}"


    def arquivo_existe(self, nome_arquivo: str, folder_id: str) -> str | None:
        """
        Verifica se um arquivo com o nome exato existe em uma pasta específica.
        Args:
            nome_arquivo (str): Nome completo do arquivo (ex: 'votacao_secao_2022_CE.zip').
            folder_id (str): ID da pasta onde a busca deve ser realizada.
        Returns:
            str | None: Retorna o ID do arquivo se ele existir, ou None caso não exista.
        """
        try:
            # Query: Filtra por nome, pasta pai e ignora arquivos na lixeira
            query = (
                f"name = '{nome_arquivo}' and "
                f"'{folder_id}' in parents and "
                f"trashed = false"
            )

            # Executa a busca pedindo apenas o ID por performance
            response = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1 # Só precisamos saber se existe pelo menos um.
            ).execute()

            arquivos = response.get('files', [])

            if arquivos:
                file_id = arquivos[0]['id']
                # LOGGER.info(f"Arquivo já existe: {nome_arquivo} (ID: {file_id})")
                return file_id
                
            return None

        except Exception as e:
            # LOGGER.error(f"Erro ao verificar existência do arquivo {nome_arquivo}: {e}")
            return None


    def listar_arquivos(self, folder_id : str):
        """
        """
        try:
            query = f"'{folder_id}' in parents and trashed = false"

            results = (
                self.service.files().list(
                    q=query,
                    fields="files(id, name)",
                    pageSize=1000,
                ).execute()
            )

            arquivos = results.get("files", [])
            return arquivos

        except HttpError as error:
            raise RuntimeError(f"Erro ao listar arquivos: {error}")