import os
import glob
import duckdb
import pandas as pd

diretorio = '/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/datasets/duckdb/'
arquivos = glob.glob(os.path.join(diretorio, f'*.duckdb'))

#print(arquivos)

conn = duckdb.connect(database="/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/datasets/duckdb/yellow_2021.duckdb")

result = conn.execute("SELECT * FROM yellow2021_ny_taxy_raw_data").fetchall()
print(result)
#df = pd.DataFrame(result)
conn.close()