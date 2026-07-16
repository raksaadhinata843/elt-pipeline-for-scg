import os
import threading
import duckdb
import pandas as pd
from flask import Flask, jsonify, render_template

app = Flask(__name__)

DB_NAME = os.environ.get("MOTHERDUCK_DB", "my_db")

_local = threading.local()

def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a per-thread cached MotherDuck connection (thread-safe)."""
    if not hasattr(_local, "conn") or _local.conn is None:
        token = os.environ["MOTHERDUCK_TOKEN"]
        _local.conn = duckdb.connect(f"md:{DB_NAME}?motherduck_token={token}")
    return _local.conn


def query_json(sql: str) -> list[dict]:
    conn = get_conn()
    df: pd.DataFrame = conn.execute(sql).df()
    # Convert any datetime-like column to "YYYY-MM" string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m")
    return df.to_dict(orient="records")


# ── Pages ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Data API ─────────────────────────────────────────────────────────────────

@app.route("/api/index-data")
def index_data():
    rows = query_json(f"""
        SELECT obs_month, cement_idx, coal_idx, oil_idx
        FROM   {DB_NAME}.gold_indexed
        ORDER  BY obs_month
    """)
    return jsonify(rows)


@app.route("/api/gap-data")
def gap_data():
    rows = query_json(f"""
        SELECT obs_month, gap_coal, gap_oil, gap_total_energy
        FROM   {DB_NAME}.gold_gap
        ORDER  BY obs_month
    """)
    return jsonify(rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
