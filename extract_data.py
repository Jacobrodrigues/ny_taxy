import os
import glob
import duckdb

tipos = ['yellow', 'green', 'fhv', 'fhvhv']

for tipo in tipos:    
    for ano in range(2019, 2023):
        conn = duckdb.connect(database=f"/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/datasets/duckdb/{tipo}_{ano}.duckdb")

        PATH = f'/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/datasets/{tipo}{ano}'
        FILES = glob.glob(os.path.join(PATH, '*.parquet'))

        if len(FILES) == 0:
            continue
 
        conn.execute(f"CREATE OR REPLACE TABLE {tipo}{ano}_ny_taxy_raw_data AS SELECT * FROM read_parquet({FILES}, union_by_name = true)")

        result = conn.execute(f"SELECT COUNT(*) FROM {tipo}{ano}_ny_taxy_raw_data").fetchone()
        print(f"Total rows in {tipo}{ano}_ny_taxy_raw_data table:", result[0])

        conn.close()
