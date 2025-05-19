from pathlib import Path
from typing import List
import duckdb


def load_files_to_duckdb(
        paths: List[str],
        db_connection: duckdb.DuckDBPyConnection,
        table_name: str,
        overwrite: bool = True,
        types: dict = None,
        nullstr: str = None
):
    """
    Загружает файлы в DuckDB (в файл базы данных).
    """
    # Проверка входных данных
    for path in paths:
        if not Path(path).exists():
            raise FileNotFoundError(f"File not found: {path}")

    # Удаляем старые данные
    if overwrite:
        db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Формируем функцию чтения с параметрами
    read_func_str = f'read_csv_auto({paths}, header=True'
    if types:
        read_func_str += f', types={types}'
    if nullstr:
        read_func_str += f', nullstr="{nullstr}"'
    read_func_str += ')'

    # Формируем SQL для загрузки
    q = f"""
            CREATE TABLE {table_name} AS
            SELECT *
            FROM {read_func_str}
        """
    db_connection.execute(q)
    print(f"Loaded {len(paths)} file(s) into table '{table_name}'")


def get_all_data_joined(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE data 
        AS
            SELECT 
                *,
                CONCAT_WS(' - ', "Товарная группа 1", "Товарная группа 2", "Товарная группа 3") AS "Id группы"
            FROM sales 
                LEFT JOIN products p USING("Id номенклатуры в лояльности")
                LEFT JOIN sale_points s USING("Id магазина в лояльности")
                LEFT JOIN (SELECT "Id клиента" AS "Id карты", * FROM clients) c USING("Id карты")
    """
    conn.execute(q)


def calc_clients(conn: duckdb.DuckDBPyConnection):
    q = """
    COPY (
        WITH card_stats AS (
            SELECT 
                "Id карты", 
                SUM("Кол-во товара") as prod_count
            FROM data 
            GROUP BY "Id карты"
        ),
        overall_avg AS (
            SELECT AVG(prod_count) as avg_prod_count
            FROM card_stats
        )
        SELECT
            cs."Id карты",
            cs.prod_count,
            2 * cs.prod_count / oa.avg_prod_count as ratio_to_avg
        FROM card_stats cs
        CROSS JOIN overall_avg oa
    ) TO 'output.csv' (HEADER, DELIMITER ';');
    """
    conn.execute(q)


def main(load_files: bool):
    conn = duckdb.connect('analysis.db')
    if load_files:
        print("Loading files to DuckDB...")
        load_files_to_duckdb(
            paths=['static/input_sales/1-8.csv', 'static/input_sales/9-16.csv', 'static/input_sales/17-22.csv',
                   'static/input_sales/23-31.csv'],
            db_connection=conn, table_name="sales", types={"PropProgramKey": "VARCHAR"}
        )
        load_files_to_duckdb(
            paths=['static/references/clients.csv'], db_connection=conn, table_name="clients", nullstr='-'
        )
        load_files_to_duckdb(
            paths=['static/references/products.csv'], db_connection=conn, table_name="products"
        )
        load_files_to_duckdb(
            paths=['static/references/sale_points.csv'], db_connection=conn, table_name="sale_points"
        )
        get_all_data_joined(conn)

    print('Joining tables...')
    calc_clients(conn)
    conn.close()


if __name__ == '__main__':
    main(load_files=False)
