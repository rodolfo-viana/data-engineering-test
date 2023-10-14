import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import datetime
from pyspark import SparkContext
import pyspark.pandas as ps
import pyspark.sql.functions as psf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import smtplib
from email.mime.text import MIMEText


def send_email():
    """Envia e-mail caso haja diferenças na validação."""
    sender = "results023"
    password = "uogifljnlomqwyds"
    recipients = ["eu@rodolfoviana.com.br"]

    msg = MIMEText("AVISO! Validação de DataFrames encontrou diferenças.")
    msg["Subject"] = "Resultado da Validação de DataFrames"
    msg["From"] = f"{sender}@gmail.com"
    msg["To"] = ", ".join(recipients)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())


def init_spark() -> SparkSession:
    """Inicializa e retorna uma sessão do Spark.

    Returns:
        SparkSession: Sessão do Spark inicializada.
    """
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.getOrCreate().conf.set(
        "spark.sql.legacy.timeParserPolicy", "LEGACY"
    )  # evita erro ao parsear data
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
    return ps.read_parquet(os.path.join(path, f"task{task_number}"), index_col="index")


def transform_data(df_pandas: ps.DataFrame) -> DataFrame:
    """Transforma o DataFrame do pandas e retorna um DataFrame Spark.

    Args:
        df_pandas (ps.DataFrame): DataFrame do pandas para transformação.

    Returns:
        DataFrame: DataFrame Spark após transformação.
    """
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
    return df_pandas_melt.to_spark(index_col="index")


def format_columns(df_spark: DataFrame, now: datetime.datetime) -> DataFrame:
    """Formata as colunas.

    Args:
        df_spark (DataFrame): DataFrame Spark para formatação.
        now (datetime.datetime): Data e hora atual para a coluna "created_at".

    Returns:
        DataFrame: DataFrame após formatação.
    """
    return (
        df_spark.withColumn("ANO", psf.col("ANO").cast("integer"))
        .withColumn(
            "MES",
            psf.when(
                psf.length(psf.col("MES")) == 1,
                psf.concat(psf.lit("0"), psf.col("MES")),
            )
            .otherwise(psf.col("MES"))
            .cast("integer"),
        )
        .withColumn(
            "year_month",
            psf.to_date(psf.concat_ws("/", psf.col("MES"), psf.col("ANO")), "MM/yyyy"),
        )
        .withColumn("uf", psf.col("ESTADO"))
        .withColumn("product", psf.regexp_replace("COMBUSTÍVEL", "\s\([^\)]*\)", ""))
        .withColumn("unit", psf.col("UNIDADE"))
        .withColumn("volume", psf.col("VOLUME").cast("double"))
        .withColumn(
            "created_at", psf.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
        )
        .select("year_month", "uf", "product", "unit", "volume", "created_at")
    )


def agg_data(df_spark: DataFrame) -> DataFrame:
    """Realiza agregação com funções de window.

    Args:
        df_spark (DataFrame): DataFrame de Spark.

    Returns:
        DataFrame: DataFrame de Spark com colunas adicionais.
    """
    window_spec = Window.partitionBy(
        psf.substring(psf.col("year_month").cast("string"), 0, 4), "uf", "product"
    )

    return df_spark.withColumn(
        "amount_spark", psf.sum("volume").over(window_spec)
    ).withColumn("count_spark", psf.count("*").over(window_spec))


def validate(df_spark: DataFrame, df_pandas: ps.DataFrame) -> (DataFrame, DataFrame):
    """Validação com uso de funções de window.

    Args:
        df_spark (DataFrame): DataFrame de Spark com agregações.
        df_pandas (ps.DataFrame): DataFrame de pandas original.

    Returns:
        DataFrame, DataFrame: Resultados de validações para soma e contagem.
    """
    df_pandas_validation = df_pandas.to_spark(index_col="index")

    validate_amount = (
        df_spark.join(
            df_pandas_validation,
            (
                psf.substring(psf.col("year_month").cast("string"), 0, 4)
                == df_pandas_validation.ANO
            )
            & (df_spark.uf == df_pandas_validation.ESTADO)
            & (
                psf.regexp_replace("COMBUSTÍVEL", "\\([^\\)]*\\)", "")
                == df_spark.product
            ),
            "inner",
        )
        .select(
            psf.substring(psf.col("year_month").cast("string"), 0, 4).alias("year"),
            "uf",
            "product",
            psf.col("TOTAL").alias("amount_pandas").cast("float"),
            psf.col("amount_spark").cast("float"),
        )
        .filter(psf.col("amount_spark").cast("float") != psf.col("TOTAL").cast("float"))
    )

    validate_count = (
        df_spark.join(
            df_pandas_validation,
            (
                psf.substring(psf.col("year_month").cast("string"), 0, 4)
                == df_pandas_validation.ANO
            )
            & (df_spark.uf == df_pandas_validation.ESTADO)
            & (
                psf.regexp_replace("COMBUSTÍVEL", "\\([^\\)]*\\)", "")
                == df_spark.product
            ),
            "inner",
        )
        .select(
            psf.substring(psf.col("year_month").cast("string"), 0, 4).alias("year"),
            "uf",
            "product",
            psf.col("count_spark"),
            psf.count("*")
            .over(Window.partitionBy("ANO", "ESTADO", "COMBUSTÍVEL"))
            .alias("count_pandas"),
        )
        .filter(psf.col("count_spark") != psf.col("count_pandas"))
    )

    return validate_amount, validate_count


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
    df_spark.write.mode("overwrite").partitionBy(["uf", "product"]).format(
        "parquet"
    ).save(os.path.join(path, f"task{task_number}"))


def main(task_number: int):
    """Função para carregamento, processamento e validação dos dados.

    Args:
        task_number (int): Número da tarefa para carregar e salvar dados.
    """
    init_spark()

    now = datetime.datetime.now()
    df_pandas = load_data(task_number=task_number)
    df_spark = transform_data(df_pandas)
    df_spark = format_columns(df_spark, now)

    df_spark_aggregated = agg_data(df_spark)
    validate_amount, validate_count = validate(df_spark_aggregated, df_pandas)
    validate_amount.show()
    validate_count.show()
    if validate_amount.count() > 0 or validate_count.count() > 0:
        send_email()

    save_data(df_spark, task_number=task_number)


if __name__ == "__main__":
    for i in range(2):
        main(i + 1)
