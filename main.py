from app.orquestracao.pipeline_ingestao import executar_pipeline_ingestao
from app.orquestracao.pipeline_bronze import executar_pipeline_bronze
from app.orquestracao.pipeline_silver import executar_pipeline_silver
from app.orquestracao.pipeline_gold import executar_pipeline_gold

from app.utils.logging_config import setup_logging

def main():

    SIGLA_ESTADO = "CE"
    ANO = 2022

    setup_logging()
    executar_pipeline_ingestao(ano=ANO, sigla_estado=SIGLA_ESTADO)
    executar_pipeline_bronze()
    executar_pipeline_silver()
    executar_pipeline_gold()


if __name__ == "__main__":
    main()