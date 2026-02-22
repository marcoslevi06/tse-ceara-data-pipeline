import pandas as pd
import io

def transformar_silver_para_gold(buffer_silver: io.BytesIO) -> pd.DataFrame:
    """
        Agrega os dados da Silver para criar uma visão de resultados por município.
    """
    # 1. Leitura do dado limpo
    df = pd.read_parquet(buffer_silver)

    # 2. Agregação Principal: Votos por Candidato/Cargo por Município
    gold_df = df.groupby(
        ['ANO_ELEICAO', 'NR_TURNO', 'SG_UF', 'NM_MUNICIPIO', 'DS_CARGO', 'NM_VOTAVEL', 'TP_VOTO']
    )['QT_VOTOS'].sum().reset_index()

    # 3. Cálculo de Percentual de Votos Válidos por Cidade/Cargo
    total_validos = gold_df[gold_df['TP_VOTO'] == 'NOMINAL'].groupby(['NM_MUNICIPIO', 'DS_CARGO'])['QT_VOTOS'].transform('sum')
    
    # Criamos a métrica de share (proporção)
    gold_df['PERC_VOTOS_VALIDOS'] = 0.0
    mask_nominal = gold_df['TP_VOTO'] == 'NOMINAL'
    gold_df.loc[mask_nominal, 'PERC_VOTOS_VALIDOS'] = (gold_df['QT_VOTOS'] / total_validos) * 100

    # 4. Ordenação: Município alfabético e Candidatos por votação (descendente)
    gold_df = gold_df.sort_values(
        by=['NM_MUNICIPIO', 'DS_CARGO', 'QT_VOTOS'], 
        ascending=[True, True, False]
    )

    return gold_df