# 🗳️ Eng. de Dados – Política Ceará (TSE)

Pipeline de Engenharia de Dados para coleta, processamento e organização de dados eleitorais do **TSE (Tribunal Superior Eleitoral)** referentes ao estado do **Ceará**, utilizando o **Google Drive como Data Lake** com arquitetura em camadas **Medalão**.

---

## 🏗️ Arquitetura do Projeto

```
Eng_de_Dados_Politica_Ceara/
│
├── app/
│   ├── ingestao/
│   │   └── tse_extrator.py          # Web scraping dos dados do TSE
│   │
│   ├── orquestracao/
│   │   ├── pipeline_ingestao.py     # Orquestra a extração e carga na camada dados_brutos
│   │   ├── pipeline_bronze.py       # Orquestra o processamento Bronze
│   │   ├── pipeline_silver.py       # Orquestra o processamento Silver
│   │   └── pipeline_gold.py         # Orquestra o processamento Gold
│   │
│   ├── processamento/
│   │   ├── silver_transformer.py    # Transformações da camada Silver
│   │   └── gold_transformer.py      # Transformações e agregações da camada Gold
│   │
│   └── storage/
│       └── google_drive.py          # Integração com Google Drive (Data Lake)
│
├── utils/
│   ├── logging_config.py            # Configuração de logs
│   └── vars_envs.py                 # Variáveis de ambiente
│
├── credenciais/
│   ├── client_secret.json           # Credenciais OAuth Google Drive
│   └── token.pickle                 # Token de autenticação
│
├── base_captura/                    # Dados locais temporários
├── .env                             # Variáveis de ambiente
├── .gitignore
├── main.py                          # Ponto de entrada da aplicação
└── requirements.txt
```

---

## 🔄 Fluxo do Pipeline

O pipeline é executado de forma sequencial a partir do `main.py`, passando pelas seguintes etapas:

### 1. 🌐 Ingestão (`pipeline_ingestao`)
- Realiza **web scraping** no portal do TSE
- Coleta dados eleitorais do **Ceará (CE)** para o ano configurado (ex: 2022)
- Armazena os arquivos brutos no Google Drive → camada **`dados_brutos`**

### 2. 🥉 Bronze (`pipeline_bronze`)
- Lê os dados da camada `dados_brutos`
- Realiza uma **limpeza mínima** (remoção de duplicatas, padronização de encoding)
- Salva na camada **`bronze`** no Google Drive

### 3. 🥈 Silver (`pipeline_silver`)
- Aplica **transformações estruturais** via `silver_transformer.py`
- Tipagem de colunas, normalização de nomes, filtragem de registros inválidos
- Salva na camada **`silver`** no Google Drive

### 4. 🥇 Gold (`pipeline_gold`)
- Aplica **agregações e regras de negócio** via `gold_transformer.py`
- Gera tabelas analíticas prontas para consumo (dashboards, relatórios)
- Salva na camada **`gold`** no Google Drive

---

## ☁️ Data Lake – Google Drive (Arquitetura Medallion)

| Camada       | Descrição                                              |
|--------------|--------------------------------------------------------|
| `dados_brutos` | Arquivos originais do TSE, sem nenhuma alteração     |
| `bronze`     | Dados com limpeza básica e padronização               |
| `silver`     | Dados transformados e estruturados                    |
| `gold`       | Dados agregados e prontos para análise                |

---

## ⚙️ Configuração e Execução

### Pré-requisitos

```bash
pip install -r requirements.txt
```

### Variáveis de Ambiente (`.env`)

```env
SIGLA_ESTADO=CE
ANO=2022
ID_DADOS_BRUTOS_BUCKET_GOOGLE_DRIVE=<id_da_pasta_dados_brutos>
ID_PASTA_BRONZE=<id_da_pasta_bronze>
ID_PASTA_SILVER=<id_da_pasta_silver>
ID_PASTA_GOLD=<id_da_pasta_gold>
PATH_GOOGLE_OAUTH_CLIENT_SECRET=./credenciais/client_secret.json
PATH_TOKEN_PICKLE=./credenciais/token.pickle
```

### Credenciais Google Drive

Adicione o arquivo `client_secret.json` na pasta `credenciais/` com as credenciais OAuth 2.0 do Google Cloud Console do seu perfil pessoal.

### Executar o Pipeline

```bash
python main.py
```

---

## 🛠️ Tecnologias Utilizadas

- **Python** – Linguagem principal
- **Requests / BeautifulSoup** – Web scraping do portal TSE
- **Google Drive API** – Armazenamento em nuvem (Data Lake)
- **Pandas** – Transformação e processamento de dados
- **Python-dotenv** – Gerenciamento de variáveis de ambiente
- **Logging** – Rastreamento de execução do pipeline

---

## 📊 Dados Coletados

Dados eleitorais do **TSE** referentes ao estado do **Ceará**, incluindo resultados de votação por município, informações de candidatos e partidos, e dados de seções e zonas eleitorais.

---

## 👤 Autor

**Marcos Levi Pinheiro Moreira**  
Engenheiro de Dados | Ceará, Brasil