import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import datetime
import logging
import pyspark.pandas as ps
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def setup_logging():
    """Configura o log da aplicação."""
    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s"
    )


def init_spark() -> SparkSession:
    """Inicializa e retorna uma sessão do Spark.

    Returns:
        SparkSession: Sessão do Spark inicializada.
    """
    logging.info("Inicializando SparkSession.")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    logging.info("SparkSession inicializada com sucesso.")
    return spark


def load_data(
    path: str = "../data_output/raw/", task_number: int = None
) -> ps.DataFrame:
    """Carrega os dados do parquet.

    Args:
        path (str): Caminho para os dados.
        task_number (int): Número da tarefa.

    Returns:
        ps.DataFrame: DataFrame carregado com os dados.
    """
    logging.info(f"Carregando dados do arquivo 'task{task_number}'.")
    data = ps.read_parquet(os.path.join(path, f"task{task_number}"))
    logging.info(f"Dados do arquivo 'task{task_number}' carregados com sucesso.")
    return data


def transform_data(df_pandas: ps.DataFrame) -> DataFrame:
    """Transforma o DataFrame do pandas e retorna um DataFrame Spark.

    Args:
        df_pandas (ps.DataFrame): DataFrame do pandas para transformação.

    Returns:
        DataFrame: DataFrame Spark após transformação.
    """
    logging.info("Iniciando transformação do DataFrame.")
    month_dict = {
        "Jan": 1,
        "Fev": 2,
        "Mar": 3,
        "Abr": 4,
        "Mai": 5,
        "Jun": 6,
        "Jul": 7,
        "Ago": 8,
        "Set": 9,
        "Out": 10,
        "Nov": 11,
        "Dez": 12,
    }

    uf_dict = {
        "ACRE": "AC",
        "ALAGOAS": "AL",
        "AMAZONAS": "AM",
        "AMAPÁ": "AP",
        "BAHIA": "BA",
        "CEARÁ": "CE",
        "DISTRITO FEDERAL": "DF",
        "ESPÍRITO SANTO": "ES",
        "GOIÁS": "GO",
        "MARANHÃO": "MA",
        "MINAS GERAIS": "MG",
        "MATO GROSSO DO SUL": "MS",
        "MATO GROSSO": "MT",
        "PARÁ": "PA",
        "PARAÍBA": "PB",
        "PERNAMBUCO": "PE",
        "PIAUÍ": "PI",
        "PARANÁ": "PR",
        "RIO DE JANEIRO": "RJ",
        "RIO GRANDE DO NORTE": "RN",
        "RONDÔNIA": "RO",
        "RORAIMA": "RR",
        "RIO GRANDE DO SUL": "RS",
        "SANTA CATARINA": "SC",
        "SERGIPE": "SE",
        "SÃO PAULO": "SP",
        "TOCANTINS": "TO",
    }
    logging.info("Despivotando dados e aplicando alterações em meses e UFs.")
    df_pandas_melt = (
        ps.melt(
            df_pandas,
            id_vars=["ANO", "ESTADO", "COMBUSTÍVEL", "UNIDADE"],
            value_vars=month_dict.keys(),
            var_name="MES",
            value_name="VOLUME",
        )
        .fillna(0)
        .replace({"MES": month_dict})
        .replace({"ESTADO": uf_dict})
    )
    logging.info("Transformação do DataFrame concluída com sucesso.")
    return df_pandas_melt.to_spark()


def format_columns(df_spark: DataFrame, now: datetime.datetime) -> DataFrame:
    """Formata as colunas.

    Args:
        df_spark (DataFrame): DataFrame Spark para formatação.
        now (datetime.datetime): Data e hora atual para a coluna "created_at".

    Returns:
        DataFrame: DataFrame após formatação.
    """
    logging.info("Formatando colunas.")
    df_fmt = (
        df_spark.withColumn("ANO", f.col("ANO").cast("integer"))
        .withColumn(
            "MES",
            f.when(f.length(f.col("MES")) == 1, f.concat(f.lit("0"), f.col("MES")))
            .otherwise(f.col("MES"))
            .cast("integer"),
        )
        .withColumn(
            "year_month",
            f.to_date(f.concat_ws("/", f.col("MES"), f.col("ANO")), "MM/yyyy"),
        )
        .withColumn("uf", f.col("ESTADO"))
        .withColumn("product", f.regexp_replace("COMBUSTÍVEL", "\s\([^\)]*\)", ""))
        .withColumn("unit", f.col("UNIDADE"))
        .withColumn("volume", f.col("VOLUME").cast("double"))
        .withColumn(
            "created_at", f.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
        )
        .select("year_month", "uf", "product", "unit", "volume", "created_at")
    )
    logging.info("Colunas formatadas com sucesso.")
    return df_fmt


def validate_data(df_spark: DataFrame, df_pandas: ps.DataFrame) -> DataFrame:
    """Valida os dados entre DataFrame Spark e DataFrame pandas.

    Args:
        df_spark (DataFrame): DataFrame Spark para validação.
        df_pandas (ps.DataFrame): DataFrame do pandas para comparação.

    Returns:
        DataFrame: DataFrame Spark após validação.
    """
    logging.info("Iniciando validação dos dados.")
    df_pandas_validation = df_pandas.to_spark()
    df_validation = df_spark.groupBy(
        [
            f.substring(f.col("year_month").cast("string"), 0, 4).alias("year"),
            "uf",
            "product",
        ]
    ).agg(f.sum("volume").alias("amount"))
    df_validation_f = df_validation.join(
        df_pandas_validation,
        (df_validation.year == df_pandas_validation.ANO)
        & (df_validation.uf == df_pandas_validation.ESTADO)
        & (f.regexp_replace("COMBUSTÍVEL", "\\([^\\)]*\\)", "") == f.col("product")),
        "inner",
    )

    logging.info("Validação dos dados concluída.")
    df_val = df_validation_f.select(
        "year",
        "uf",
        "product",
        f.col("TOTAL").alias("amount_pandas").cast("float"),
        f.col("amount").alias("amount_spark").cast("float"),
    ).filter(f.col("amount").cast("float") != f.col("TOTAL").cast("float"))
    logging.info("Validação dos dados concluída.")
    return df_val


def save_data(
    df_spark: DataFrame,
    path: str = "../data_output/processed/",
    task_number: int = None,
):
    """Salva o DataFrame Spark como parquet.

    Args:
        df_spark (DataFrame): DataFrame Spark para salvar.
        path (str): Caminho base para salvar os dados.
        task_number (int): Número da tarefa para construir o caminho completo.
    """
    logging.info(f"Salvando dados no diretório 'processed/task{task_number}'.")
    df_spark.write.mode("overwrite").partitionBy(["uf", "product"]).format(
        "parquet"
    ).save(os.path.join(path, f"task{task_number}"))
    logging.info(f"Dados salvos com sucesso.")


def main(task_number: int):
    """Função para processamento e validação dos dados.

    Args:
        task_number (int): Número da tarefa para carregar e salvar dados.
    """
    logging.info(f"Iniciando processamento para 'task{task_number}'.")
    setup_logging()
    spark = init_spark()

    now = datetime.datetime.now()
    df_pandas = load_data(task_number=task_number)
    df_spark = transform_data(df_pandas)
    df_spark = format_columns(df_spark, now)

    df_validation_f = validate_data(df_spark, df_pandas)
    df_validation_f.show()

    save_data(df_spark, task_number=task_number)
    logging.info(f"Processamento para 'task{task_number}' concluído com sucesso.")


if __name__ == "__main__":
    for i in range(2):
        main(i + 1)
