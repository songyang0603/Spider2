import argparse
import json
from pathlib import Path

import snowflake.connector
import pandas as pd


DEFAULT_CRED_PATH = Path("methods/spider-agent-tc/credentials/snowflake_credential.json")


def load_credentials(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fetch_sample_rows(conn, database: str, schema: str, table: str, limit: int) -> pd.DataFrame:
    sql = f'SELECT * FROM "{database}"."{schema}"."{table}" LIMIT {limit}'
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


def list_databases(conn) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute("SHOW DATABASES")
        return [row[1] for row in cur.fetchall()]
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="Quickly view Spider2-Snow tables")
    parser.add_argument(
        "--cred_path",
        type=Path,
        default=DEFAULT_CRED_PATH,
        help="Path to snowflake_credential.json",
    )
    parser.add_argument("--database", required=True, help="Database name, e.g., ADVENTUREWORKS")
    parser.add_argument("--schema", required=True, help="Schema name, e.g., ADVENTUREWORKS")
    parser.add_argument("--table", required=True, help="Table name, e.g., CURRENCYRATE")
    parser.add_argument("--limit", type=int, default=5, help="Number of rows to preview")
    args = parser.parse_args()

    cred = load_credentials(args.cred_path)
    if "username" in cred and "user" not in cred:
        cred["user"] = cred["username"]

    print(f"Connecting to Snowflake account {cred.get('account')} as {cred.get('user') or cred.get('username')}...")
    conn = snowflake.connector.connect(**cred)

    try:
        print("\nAvailable databases (first 10):")
        dbs = list_databases(conn)
        for name in dbs[:10]:
            print(f"- {name}")

        print(f"\nPreviewing {args.database}.{args.schema}.{args.table} (limit {args.limit})...")
        df = fetch_sample_rows(conn, args.database, args.schema, args.table, args.limit)
        if df.empty:
            print("Table is empty or not found.")
        else:
            print(df.to_markdown(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
