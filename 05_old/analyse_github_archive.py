import duckdb
import time
import json
import os

url = "https://data.gharchive.org/2025-03-01-12.json.gz"

# Vérification de la taille du fichier
file_size_mb = os.path.getsize(json_file) / (1024 * 1024)
print(f"Taille du fichier: {file_size_mb:.2f} MB")


start_time = time.time()

# Connexion à DuckDB (en mémoire pour l'exploration)
con = duckdb.connect(":memory:")


# Chargement de l'extension httpfs pour l'accès aux fichiers distants
con.load_extension("httpfs")

# Définition d'une table virtuelle directement sur l'URL

# Nous utilisons un fichier d'une heure spécifique (1er janvier 2023, 15h UTC)
con.sql(f"create table gh_archive as from read_json_auto('{url}',ignore_errors=true)")
duckdb_load_time = time.time() - start_time
print(f"Temps de chargement avec DuckDB: {duckdb_load_time:.2f} secondes")

# Affichage des 5 premiers enregistrements
gh_archive_df = con.sql("select * from gh_archive limit 5").df()

print(gh_archive_df)