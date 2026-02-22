import pandas as pd
import io

def transformar_bronze_para_silver(buffer_bronze: io.BytesIO) -> pd.DataFrame:
    """
    Lê o dado bruto da Bronze e devolve um DataFrame refinado para a Silver.
    """
    # 1. Carregamento
    df = pd.read_parquet(buffer_bronze)

    # 2. Definição das colunas de interesse
    colunas_silver = [
        'ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO',
        'NR_ZONA', 'NR_SECAO', 'DS_CARGO', 'NR_VOTAVEL', 'NM_VOTAVEL', 'QT_VOTOS'
    ]
    
    # Filtra apenas as colunas que existem (evita erro se o layout mudar)
    df_silver = df[[col for col in colunas_silver if col in df.columns]].copy()

    # 3. Limpeza de Strings e Normalização
    cols_string = ['NM_MUNICIPIO', 'NM_VOTAVEL', 'DS_CARGO']
    for col in cols_string:
        if col in df_silver.columns:
            df_silver[col] = df_silver[col].str.strip().str.upper()

    # 4. Tipagem Estrita
    df_silver['QT_VOTOS'] = pd.to_numeric(df_silver['QT_VOTOS'], errors='coerce').fillna(0).astype(int)
    
    # 5. Enriquecimento: Classificação do Tipo de Voto
    # Isso facilita muito criar dashboards depois
    df_silver['TP_VOTO'] = 'NOMINAL'
    df_silver.loc[df_silver['NM_VOTAVEL'] == 'VOTO BRANCO', 'TP_VOTO'] = 'BRANCO'
    df_silver.loc[df_silver['NM_VOTAVEL'] == 'VOTO NULO', 'TP_VOTO'] = 'NULO'

    # 6. Ordenação lógica (Opcional, mas ajuda na inspeção visual)
    df_silver = df_silver.sort_values(['NM_MUNICIPIO', 'NR_ZONA', 'NR_SECAO'])

    return df_silver