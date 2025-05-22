import os
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
                WHEN group_coeff BETWEEN 0 AND 3 THEN '1 Разовые/низ чек'
                WHEN group_coeff BETWEEN 3 AND 5 THEN '2 Редкие/ низ чек'
                WHEN group_coeff BETWEEN 5 AND 7 THEN '3 Умеренные'
                WHEN group_coeff BETWEEN 7 AND 10 THEN '4 Частые/ выс чек'
                WHEN group_coeff BETWEEN 10 AND 20 THEN '5 Постоянные'
                WHEN group_coeff > 20 THEN '6 Лояльные/ выс чек'
                ELSE ''
            END
    """
    conn.execute(q)


def calc_products_coefficients(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE data_products_coeffs AS (
            WITH overall_stats AS (
                SELECT
                    group_id,
                    COUNT(DISTINCT "Id чека") tx_count
                FROM data
                GROUP BY "group_id"
            ),
            product_stats AS (
                SELECT
                    group_id,
                    "Id номенклатуры в лояльности" as product_id,
                    COUNT(*) as freq,
                    freq / os.tx_count as hit_rate
                FROM data
                LEFT JOIN overall_stats AS os USING(group_id)
                WHERE "Id номенклатуры в лояльности" IS NOT NULL
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
                2 * freq / osa.avg_freq as freq_coeff,
                2 * hit_rate / osa.avg_hit_rate as hit_rate_coeff,
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
                WHEN summ_coeff BETWEEN 0 AND 0.3 THEN '1'
                WHEN summ_coeff BETWEEN 0.3 AND 1 THEN '2'
                WHEN summ_coeff BETWEEN 1 AND 3 THEN '3'
                WHEN summ_coeff BETWEEN 3 AND 7 THEN '4'
                WHEN summ_coeff BETWEEN 7 AND 15 THEN '5'
                WHEN summ_coeff BETWEEN 15 AND 50 THEN '6'
                WHEN summ_coeff > 50 THEN '7'
                ELSE ''
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
                            WHEN summ_cluster_coeff BETWEEN 0 AND 1 THEN '1'
                            WHEN summ_cluster_coeff BETWEEN 1 AND 2 THEN '2'
                            WHEN summ_cluster_coeff BETWEEN 2 AND 3 THEN '3'
                            WHEN summ_cluster_coeff BETWEEN 3 AND 5 THEN '4'
                            WHEN summ_cluster_coeff BETWEEN 5 AND 10 THEN '5'
                            WHEN summ_cluster_coeff BETWEEN 10 AND 50 THEN '6'
                            WHEN summ_cluster_coeff > 50 THEN '7'
                            ELSE ''
                        END
                """
        conn.execute(q)


def calc_products_by_price(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE product_price_coeffs AS (
            WITH group1_agg AS (
                SELECT 
                    "Товарная группа 1" as group1_id,
                    COUNT(*) as group1_products_count,
                    SUM("Стоимость товара" / "Вес товара") / group1_products_count as group1_price_kg
                FROM data
                GROUP BY "Товарная группа 1"
            ),
            group2_agg AS (
                SELECT 
                    "Товарная группа 1" as group1_id,
                    "Товарная группа 2" as group2_id,
                    COUNT(*) as group2_products_count,
                    SUM("Стоимость товара" / "Вес товара") / group2_products_count as group2_price_kg
                FROM data
                GROUP BY "Товарная группа 1", "Товарная группа 2"
            ),
            group3_agg AS (
                SELECT 
                    "Товарная группа 1" as group1_id,
                    "Товарная группа 2" as group2_id,
                    "Товарная группа 3" as group3_id,
                    COUNT(*) as group3_products_count,
                    SUM("Стоимость товара" / "Вес товара") / group3_products_count as group3_price_kg
                FROM data
                GROUP BY "Товарная группа 1", "Товарная группа 2", "Товарная группа 3"
            ),
            product_prices AS (
                SELECT
                    "Id номенклатуры в лояльности" as product_id,
                    AVG("Стоимость товара") as price
                FROM data
                GROUP BY "Id номенклатуры в лояльности"
            )
            SELECT
                "Товарная группа 1" as group1_id,
                "Товарная группа 2" as group2_id,
                "Товарная группа 3" as group3_id,
                "Id номенклатуры в лояльности" as product_id,
                pp.price,
                "Вес товара" as weight,
                pp.price / weight as price_kg,
                g1.group1_price_kg,
                g2.group2_price_kg,
                g3.group3_price_kg,
                g3.group3_products_count,
                CASE
                    WHEN g3.group3_products_count > 6 THEN price_kg / g3.group3_price_kg
                    WHEN g3.group3_products_count BETWEEN 3 AND 6 THEN price_kg / g2.group2_price_kg
                    ELSE price_kg / g1.group1_price_kg
                END as price_kg_coeff,
                '' as price_segment
            FROM products p
                LEFT JOIN group3_agg g3 ON p."Товарная группа 1" = g3.group1_id AND p."Товарная группа 2" = g3.group2_id AND p."Товарная группа 3" = g3.group3_id
                LEFT JOIN group2_agg g2 ON p."Товарная группа 1" = g2.group1_id AND p."Товарная группа 2" = g2.group2_id
                LEFT JOIN group1_agg g1 ON p."Товарная группа 1" = g1.group1_id
                LEFT JOIN product_prices pp ON p."Id номенклатуры в лояльности" = pp.product_id
            ORDER BY price_kg_coeff DESC
        );
    """
    conn.execute(q)


def set_products_by_price_ranks(conn: duckdb.DuckDBPyConnection):
    q = """
        UPDATE
            product_price_coeffs
        SET price_segment =
            CASE
                WHEN price_kg_coeff BETWEEN 0 AND 1.7 THEN 'Низкий'
                WHEN price_kg_coeff BETWEEN 1.7 AND 2.6 THEN 'Средний'
                WHEN price_kg_coeff BETWEEN 2.6 AND 10 THEN 'Высокий'
                WHEN price_kg_coeff > 10 THEN 'Максимальный'
                ELSE ''
            END
    """
    conn.execute(q)


def calc_clients_by_price(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE client_price_coeffs AS (
            SELECT
                "Id карты" as client_id,
                COUNT(*) as product_count,
                SUM(CASE WHEN ppc.price_segment = 'Низкий' THEN 1 ELSE 0 END) / product_count as low_segment_share,
                SUM(CASE WHEN ppc.price_segment = 'Средний' THEN 1 ELSE 0 END) / product_count as middle_segment_share,
                SUM(CASE WHEN ppc.price_segment = 'Высокий' THEN 1 ELSE 0 END) / product_count as high_segment_share,
                SUM(CASE WHEN ppc.price_segment = 'Максимальный' THEN 1 ELSE 0 END) / product_count as max_segment_share,
                CASE
                    WHEN low_segment_share > 0.35 THEN 'Низкий'
                    WHEN high_segment_share + max_segment_share > 0.35 THEN 'Высокий'
                    ELSE 'Средний'
                END as price_segment
            FROM data d
                LEFT JOIN product_price_coeffs ppc ON d."Id номенклатуры в лояльности" = ppc.product_id
            GROUP BY "Id карты"
        );
    """
    conn.execute(q)


def calc_sale_points_clients_segments(conn: duckdb.DuckDBPyConnection):
    q = """
        CREATE OR REPLACE TABLE sale_points_segments AS (
            WITH sale_points_stats AS (
                SELECT
                    "Id магазина в лояльности" as sale_point_id,
                    COUNT(DISTINCT "Id карты") as overall_clients_count
                FROM data
                GROUP BY "Id магазина в лояльности"
            ),
            segmented_counts AS (
                SELECT
                    d."Id магазина в лояльности" as sale_point_id,
                    s.overall_clients_count,
                    cpc.price_segment,
                    COUNT(DISTINCT d."Id карты") as clients_count
                FROM data d
                LEFT JOIN client_price_coeffs cpc ON d."Id карты" = cpc.client_id
                LEFT JOIN sale_points_stats s ON d."Id магазина в лояльности" = s.sale_point_id
                GROUP BY d."Id магазина в лояльности", s.overall_clients_count, cpc.price_segment
            )
            SELECT
                sale_point_id as "Id магазина в лояльности",
                ROUND(SUM(CASE WHEN price_segment = 'Низкий' THEN clients_count ELSE 0 END) * 100.0 / overall_clients_count, 2) as "Низкий",
                ROUND(SUM(CASE WHEN price_segment = 'Средний' THEN clients_count ELSE 0 END) * 100.0 / overall_clients_count, 2) as "Средний",
                ROUND(SUM(CASE WHEN price_segment = 'Высокий' THEN clients_count ELSE 0 END) * 100.0 / overall_clients_count, 2) as "Высокий"
            FROM segmented_counts
            GROUP BY sale_point_id, overall_clients_count
            ORDER BY sale_point_id
        )
    """
    conn.execute(q)


def save_data_to_csv(conn: duckdb.DuckDBPyConnection):
    if not os.path.exists('res'):
        os.mkdir('res')

    q = """
        COPY data_clients_coeffs TO 'res/output_clients_coeffs.csv' (HEADER, DELIMITER ';');
        COPY data_products_coeffs TO 'res/output_products_coeffs.csv' (HEADER, DELIMITER ';');
        COPY data_coeffs TO 'res/output_coeffs.csv' (HEADER, DELIMITER ';');
        COPY product_price_coeffs TO 'res/output_products_price_coeffs.csv' (HEADER, DELIMITER ';');
        COPY client_price_coeffs TO 'res/output_clients_price_coeffs.csv' (HEADER, DELIMITER ';');
        COPY sale_points_segments TO 'res/output_sale_points_segments.csv' (HEADER, DELIMITER ';');
    """
    conn.execute(q)

    for cluster in clusters:
        q = f"""COPY data_{cluster} TO 'res/output_{cluster}.csv' (HEADER, DELIMITER ';');"""
        conn.execute(q)


def main(load_files: bool):
    conn = duckdb.connect('analysis.db')
    if load_files:
        print("Loading files to DuckDB...")
        load_files_to_duckdb(
            paths=['static/input_sales/1-8.csv', 'static/input_sales/9-16.csv', 'static/input_sales/17-22.csv',
                   'static/input_sales/23-31.csv'],
            db_connection=conn, table_name="sales", types={"PropProgramKey": "VARCHAR", "Кол-во товара": "DOUBLE", "Сумма": "DOUBLE", "Стоимость товара": "DOUBLE"}
        )
        load_files_to_duckdb(
            paths=['static/references/clients.csv'], db_connection=conn, table_name="clients", nullstr='-'
        )
        load_files_to_duckdb(
            paths=['static/references/products.csv'], db_connection=conn, table_name="products", types={"Вес товара": "DOUBLE"}
        )
        load_files_to_duckdb(
            paths=['static/references/sale_points.csv'], db_connection=conn, table_name="sale_points"
        )
        print('Joining tables...')
        get_all_data_joined(conn)

    print('Calculating clients coefficients...')
    calc_clients_coefficients(conn)
    set_clients_clusters(conn)

    print('Calculating products coefficients...')
    calc_products_coefficients(conn)
    set_products_ranks(conn)

    print('Joining coeffs into data...')
    join_client_product_coeffs(conn)

    print('Calculating product cluster ranks...')
    calc_product_cluster_rank(conn)
    set_product_cluster_ranks(conn)

    print('Calculating products by price...')
    calc_products_by_price(conn)
    set_products_by_price_ranks(conn)

    print('Calculating clients by price...')
    calc_clients_by_price(conn)

    print('Calculating sale points clients segments...')
    calc_sale_points_clients_segments(conn)

    print('Saving data...')
    save_data_to_csv(conn)
    conn.close()


if __name__ == '__main__':
    main(load_files=False)
