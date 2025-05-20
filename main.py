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
                CONCAT_WS(' - ', "Товарная группа 1", "Товарная группа 2", "Товарная группа 3") AS group_id
            FROM sales 
                LEFT JOIN products p USING("Id номенклатуры в лояльности")
                LEFT JOIN sale_points s USING("Id магазина в лояльности")
                LEFT JOIN (SELECT "Id клиента" AS "Id карты", * FROM clients) c USING("Id карты")
    """
    conn.execute(q)


def calc_clients_coefficients(conn: duckdb.DuckDBPyConnection):
    q = """
    CREATE OR REPLACE TABLE data_clients_coeffs AS (
        WITH card_stats AS (
            SELECT 
                "Id карты" as card_id, 
                SUM("Кол-во товара") as prod_count,
                SUM("Сумма") as buy_sum,
                COUNT(DISTINCT strftime("Дата", '%Y-%m')) as month_count,
                COUNT(DISTINCT CONCAT_WS(' - ',  strftime("Дата", '%Y-%m'), "Временной диапазон")) as visits_count,
                visits_count / month_count as monthly_visits_count
            FROM data
            WHERE "Тип чека" = 'Продажа'
            GROUP BY "Id карты"
        ),
        overall_avg AS (
            SELECT 
                AVG(prod_count) as avg_prod_count,
                AVG(monthly_visits_count) as avg_monthly_visits_count,
                AVG(buy_sum) as avg_buy_sum,
            FROM card_stats
        )
        SELECT
            cs.card_id,
            cs.prod_count,
            cs.buy_sum,
            cs.monthly_visits_count,
            cs.buy_sum / cs.visits_count as avg_buy,
            2 * cs.prod_count / oa.avg_prod_count as prod_count_coeff,
            2 * cs.monthly_visits_count / oa.avg_monthly_visits_count as visits_count_coeff,
            2 * cs.buy_sum / oa.avg_buy_sum as buy_sum_coeff,
            prod_count_coeff + visits_count_coeff + buy_sum_coeff as group_coeff,
            '' as cluster_name
        FROM card_stats cs
        CROSS JOIN overall_avg oa
    );
    """
    conn.execute(q)


def set_clients_clusters(conn: duckdb.DuckDBPyConnection):
    q = """
        UPDATE 
            data_clients_coeffs 
        SET cluster_name =
            CASE
                WHEN group_coeff BETWEEN 3 AND 5 THEN '2 Редкие/ низ чек'
                WHEN group_coeff BETWEEN 5 AND 7 THEN '3 Умеренные'
                WHEN group_coeff BETWEEN 7 AND 10 THEN '4 Частые/ выс чек'
                WHEN group_coeff BETWEEN 10 AND 20 THEN '5 Постоянные'
                WHEN group_coeff > 20 THEN '6 Лояльные/ выс чек'
                ELSE '1 Разовые/низ чек'
            END
    """
    conn.execute(q)


def calc_products_coefficients(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE data_products_coeffs AS (
            WITH overall_stats AS (
                SELECT
                    COUNT(DISTINCT "Id чека") tx_count
                FROM data
            ),
            product_stats AS (
                SELECT
                    group_id,
                    "Id номенклатуры в лояльности" as product_id,
                    COUNT(*) as freq,
                    freq / os.tx_count as hit_rate
                FROM data
                CROSS JOIN overall_stats AS os
                GROUP BY group_id, "Id номенклатуры в лояльности", os.tx_count
            ),
            product_stats_averages AS (
                SELECT
                    AVG(freq) as avg_freq,
                    AVG(hit_rate) as avg_hit_rate
                FROM product_stats
            )
            SELECT
                group_id,
                product_id,
                freq,
                hit_rate,
                freq / osa.avg_freq as freq_coeff,
                hit_rate / osa.avg_hit_rate as hit_rate_coeff,
                freq_coeff + hit_rate_coeff as summ_coeff,
                '' as rank
            FROM product_stats ps
            CROSS JOIN product_stats_averages osa
        );
    """
    conn.execute(q)


def set_products_ranks(conn: duckdb.DuckDBPyConnection):
    q = """
        UPDATE
            data_products_coeffs
        SET rank =
            CASE
                WHEN summ_coeff BETWEEN 0.3 AND 1 THEN '2'
                WHEN summ_coeff BETWEEN 1 AND 3 THEN '3'
                WHEN summ_coeff BETWEEN 3 AND 7 THEN '4'
                WHEN summ_coeff BETWEEN 7 AND 15 THEN '5'
                WHEN summ_coeff BETWEEN 15 AND 50 THEN '6'
                WHEN summ_coeff > 50 THEN '7'
                ELSE '1'
            END
    """
    conn.execute(q)


clusters = ['cluster1', 'cluster2', 'cluster3', 'cluster4', 'cluster5', 'cluster6']


def join_client_product_coeffs(conn: duckdb.DuckDBPyConnection):
    q = f"""
        CREATE OR REPLACE TABLE data_coeffs AS (
            SELECT
                d.group_id,
                p.product_id,
                p.rank,
                COUNT(DISTINCT c.cluster_name) AS unique_cluster_count,
                SUM(CASE WHEN c.cluster_name = '1 Разовые/низ чек' THEN 1 ELSE 0 END) as {clusters[0]},
                SUM(CASE WHEN c.cluster_name = '2 Редкие/ низ чек' THEN 1 ELSE 0 END) as {clusters[1]},
                SUM(CASE WHEN c.cluster_name = '3 Умеренные' THEN 1 ELSE 0 END) as {clusters[2]},
                SUM(CASE WHEN c.cluster_name = '4 Частые/ выс чек' THEN 1 ELSE 0 END) as {clusters[3]},
                SUM(CASE WHEN c.cluster_name = '5 Постоянные' THEN 1 ELSE 0 END) as {clusters[4]},
                SUM(CASE WHEN c.cluster_name = '6 Лояльные/ выс чек' THEN 1 ELSE 0 END) as {clusters[5]},
                CONCAT_WS('_', p.rank, unique_cluster_count) AS rank_cross
            FROM data d
                LEFT JOIN data_clients_coeffs c ON d."Id карты" = c.card_id
                LEFT JOIN data_products_coeffs p ON d."Id номенклатуры в лояльности" = p.product_id
            GROUP BY d.group_id, p.product_id, p.rank
        );
    """
    conn.execute(q)


def calc_product_cluster_rank(conn: duckdb.DuckDBPyConnection):
    for cluster in clusters:
        q = f"""
            CREATE OR REPLACE TABLE data_{cluster} AS (
                WITH group_stats AS (
                    SELECT
                        group_id,
                        SUM({cluster}) as sum_cluster
                    FROM data_coeffs
                    GROUP BY group_id
                ),
                product_cluster_data AS (
                    SELECT
                        group_id,
                        product_id,
                        {cluster},
                        {cluster} / os.sum_cluster as share_cluster
                    FROM data_coeffs d
                        JOIN group_stats os USING(group_id)
                    WHERE {cluster} > 0
                ),
                product_cluster_averages AS (
                    SELECT
                        AVG({cluster}) as avg_cluster,
                        AVG(share_cluster) as avg_share_cluster
                    FROM product_cluster_data
                )
                SELECT 
                    d.*,
                    2 * d.{cluster} / a.avg_cluster as cluster_coeff,
                    2 * d.share_cluster / a.avg_share_cluster as share_cluster_coeff,
                    cluster_coeff + share_cluster_coeff as summ_cluster_coeff,
                    '' as {cluster}_rank
                FROM product_cluster_data d
                    CROSS JOIN product_cluster_averages a
                ORDER BY summ_cluster_coeff DESC
            );
        """
        conn.execute(q)


def set_product_cluster_ranks(conn: duckdb.DuckDBPyConnection):
    for cluster in clusters:
        q = f"""
                UPDATE
                    data_{cluster}
                SET {cluster}_rank =
                        CASE
                            WHEN summ_cluster_coeff BETWEEN 1 AND 2 THEN '2'
                            WHEN summ_cluster_coeff BETWEEN 2 AND 3 THEN '3'
                            WHEN summ_cluster_coeff BETWEEN 3 AND 5 THEN '4'
                            WHEN summ_cluster_coeff BETWEEN 5 AND 10 THEN '5'
                            WHEN summ_cluster_coeff BETWEEN 10 AND 50 THEN '6'
                            WHEN summ_cluster_coeff > 50 THEN '7'
                            ELSE '1'
                            END
                """
        conn.execute(q)


def save_data_to_csv(conn: duckdb.DuckDBPyConnection):
    q = """
        COPY data_clients_coeffs TO 'output_client_coeffs.csv' (HEADER, DELIMITER ';');
        COPY data_products_coeffs TO 'output_product_coeffs.csv' (HEADER, DELIMITER ';');
        COPY data_coeffs TO 'output_coeffs.csv' (HEADER, DELIMITER ';');
    """
    conn.execute(q)

    for cluster in clusters:
        q = f"""COPY data_{cluster} TO 'output_{cluster}.csv' (HEADER, DELIMITER ';');"""
        conn.execute(q)


def main(load_files: bool):
    conn = duckdb.connect('analysis.db')
    if load_files:
        print("Loading files to DuckDB...")
        load_files_to_duckdb(
            paths=['static/input_sales/1-8.csv', 'static/input_sales/9-16.csv', 'static/input_sales/17-22.csv',
                   'static/input_sales/23-31.csv'],
            db_connection=conn, table_name="sales", types={"PropProgramKey": "VARCHAR", "Кол-во товара": "DOUBLE", "Сумма": "DOUBLE"}
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
        print('Joining tables...')
        get_all_data_joined(conn)

    print('Calculating clients coefficients...')
    calc_clients_coefficients(conn)

    print('Setting clients clusters...')
    set_clients_clusters(conn)

    print('Calculating products coefficients...')
    calc_products_coefficients(conn)

    print('Setting products ranks...')
    set_products_ranks(conn)

    print('Joining coeffs into data...')
    join_client_product_coeffs(conn)

    print('Calculating product cluster ranks...')
    calc_product_cluster_rank(conn)

    print('Setting product cluster ranks...')
    set_product_cluster_ranks(conn)

    print('Saving data...')
    save_data_to_csv(conn)
    conn.close()


if __name__ == '__main__':
    main(load_files=False)
