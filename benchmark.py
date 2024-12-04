import duckdb
import time
import statistics

def gen_tpch():
    con = duckdb.connect()
    tables = ['supplier','lineitem','orders','nation']
    con.execute("CALL dbgen(sf=20);")
    for table in tables:
        con.execute(f"COPY {table} to '{table}.csv'")
        con.execute(f"COPY {table} to '{table}.parquet'")
        con.execute(f"COPY {table} TO '{table}_zstd.parquet' (FORMAT 'parquet', CODEC 'zstd', COMPRESSION_LEVEL 1);")

schema = '''
CREATE TABLE lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DOUBLE PRECISION not null,
    l_extendedprice  DOUBLE PRECISION not null,
    l_discount    DOUBLE PRECISION not null,
    l_tax         DOUBLE PRECISION not null,
    l_returnflag  CHAR(1) not null,
    l_linestatus  CHAR(1) not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct CHAR(25) not null,
    l_shipmode     CHAR(10) not null,
    l_comment      VARCHAR(44) not null
);
'''


extensions = ['.csv','.parquet', '_zstd.parquet']

def load_data():
    print ("Create Table")
    for extension in extensions:
        times = []
        for i in range(5):
            con = duckdb.connect()
            con.execute('SET max_temp_directory_size = \'0GB\'')
            con.execute('SET preserve_insertion_order = false;')
            con.execute(schema)
            start_time = time.time()
            con.execute(f"COPY lineitem FROM \'lineitem{extension}\'")
            end_time = time.time()
            
            times.append(end_time - start_time)  

        median_time = statistics.median(times) 
        print(f"{extension} Median Time: {median_time:.4f} seconds")  

def q_01():
    print ("Q 01")
    for extension in extensions:
        times = []
        for i in range(5):
            con = duckdb.connect()
            con.execute('SET max_temp_directory_size = \'0GB\'')
            con.execute('SET preserve_insertion_order = false;')
            con.execute(schema)
            start_time = time.time()
            con.execute(f'''
                        SELECT
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity) AS sum_qty,
                    sum(l_extendedprice) AS sum_base_price,
                    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                    avg(l_quantity) AS avg_qty,
                    avg(l_extendedprice) AS avg_price,
                    avg(l_discount) AS avg_disc,
                    count(*) AS count_order
                FROM
                    lineitem{extension}
                GROUP BY
                    l_returnflag,
                    l_linestatus
                ORDER BY
                    l_returnflag,
                    l_linestatus;
                        ''')
            end_time = time.time()
            
            times.append(end_time - start_time)  

        median_time = statistics.median(times) 
        print(f"{extension} Median Time: {median_time:.4f} seconds")  


def q_21():
    print ("Q 21")
    for extension in extensions:
        times = []
        for i in range(5):
            con = duckdb.connect()
            con.execute('SET max_temp_directory_size = \'0GB\'')
            con.execute('SET preserve_insertion_order = false;')
            con.execute(schema)
            start_time = time.time()
            con.execute(f'''
                        SELECT
                            s_name,
                            count(*) AS numwait
                        FROM
                            supplier{extension} supplier,
                            lineitem{extension} l1,
                            orders{extension} orders,
                            nation{extension} nation
                        WHERE
                            s_suppkey = l1.l_suppkey
                            AND o_orderkey = l1.l_orderkey
                            AND o_orderstatus = 'F'
                            AND l1.l_receiptdate > l1.l_commitdate
                            AND EXISTS (
                                SELECT
                                    *
                                FROM
                                    lineitem{extension} l2
                                WHERE
                                    l2.l_orderkey = l1.l_orderkey
                                    AND l2.l_suppkey <> l1.l_suppkey)
                            AND NOT EXISTS (
                                SELECT
                                    *
                                FROM
                                    lineitem{extension} l3
                                WHERE
                                    l3.l_orderkey = l1.l_orderkey
                                    AND l3.l_suppkey <> l1.l_suppkey
                                    AND l3.l_receiptdate > l3.l_commitdate)
                            AND s_nationkey = n_nationkey
                            AND n_name = 'SAUDI ARABIA'
                        GROUP BY
                            s_name
                        ORDER BY
                            numwait DESC,
                            s_name
                        LIMIT 100;
                        ''' )
            end_time = time.time()
            
            times.append(end_time - start_time)  

        median_time = statistics.median(times) 
        print(f"{extension} Median Time: {median_time:.4f} seconds")      

# gen_tpch()

# load_data()

# q_01()

q_21()