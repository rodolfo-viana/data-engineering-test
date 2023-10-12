import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import numpy as np
import pyspark.pandas as pd
import win32com.client as win32
from openpyxl import load_workbook
from openpyxl.pivot.fields import Missing
import logging

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] - %(message)s"
)


def convert_xls_xlsx(file_path: str) -> None:
    """Converte um arquivo .xls para .xlsx

    Args:
        file_path (str): Caminho absoluto para o arquivo .xls.
    """
    logging.info("Convertendo .xls para .xlsx")
    excel_app = win32.gencache.EnsureDispatch("Excel.Application")
    workbook = excel_app.Workbooks.Open(file_path)

    new_path = file_path + "x"
    workbook.SaveAs(new_path, FileFormat=51)
    workbook.Close()
    excel_app.Application.Quit()
    logging.info(f"Arquivo salvo como {new_path}")


def extract_pivot_data(worksheet, dynamic_table: str, task_num: int) -> None:
    """Extrai os dados da tabela dinâmica e salva como arquivo parquet.

    Args:
        worksheet: Planilha.
        dynamic_table (str): Nome da tabela dinâmica.
        task_num (int): Número da tarefa para nomeação do arquivo de saída.
    """
    logging.info(f"Extraindo dados da tabela {dynamic_table}")
    pivot = next(p for p in worksheet._pivots if p.name == dynamic_table)

    field_map = {}
    for field in pivot.cache.cacheFields:
        if field.sharedItems.count > 0:
            field_map[field.name] = [item.v for item in field.sharedItems._fields]

    column_labels = [field.name for field in pivot.cache.cacheFields]
    records = []
    for record in pivot.cache.records.r:
        record_values = [
            f.v if not isinstance(f, Missing) else np.nan for f in record._fields
        ]
        record_dict = dict(zip(column_labels, record_values))

        for key in field_map:
            record_dict[key] = field_map[key][record_dict[key]]

        records.append(record_dict)

    df = pd.DataFrame.from_dict(records)
    output_path = f"../data_output/raw/task{task_num + 1}"
    df.to_parquet(output_path)
    logging.info(f"Dados salvos em {output_path}")


def main(xls_file: str, wb_name: str):
    """Executa as funções em ordem.

    Args:
        xls_file (str): Nome do arquivo xls.
        wb_name (str): Nome do workbook.

    Returns:
        sheet (openpyxl.worksheet.worksheet.Worksheet): Dados da sheet.
    """
    file_path = os.path.abspath(xls_file)
    convert_xls_xlsx(file_path)

    workbook_path = file_path + "x"
    workbook = load_workbook(workbook_path)
    return workbook[wb_name]


if __name__ == "__main__":
    file = "../assets/vendas-combustiveis-m3.xls"
    wb = "Plan1"
    sheet = main(file, wb)
    dynamic_tables = ["Tabela dinâmica1", "Tabela dinâmica3"]
    for index, table in enumerate(dynamic_tables):
        extract_pivot_data(sheet, table, index)
