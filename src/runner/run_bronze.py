import duckdb
import pathlib

con = duckdb.connect("md:commodity_pipeline")

ddl_dir = pathlib.Path("sql/ddl")
transform_dir = pathlib.Path("sql/transform")

# create tables
for file in sorted(ddl_dir.glob("*.sql")):
    with open(file) as f:
        con.execute(f.read())

# run transforms
for file in sorted(transform_dir.glob("*.sql")):
    with open(file) as f:
        con.execute(f.read())

print("Bronze pipeline finished.")
