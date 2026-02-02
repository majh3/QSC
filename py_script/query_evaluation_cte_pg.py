#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTE query evaluation (PostgreSQL).

Multi-process safe output protocol:
- Always prints a single machine-readable line:
  QSC_RESULT_JSON: {...}
"""
import json
import sys
import time
from typing import List, Dict
try:
    import psycopg2
except ImportError:
    print("Please install psycopg2: pip install psycopg2-binary")
    sys.exit(1)

def load_cfg(p: str) -> Dict:
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_sqls(fp: str) -> List[str]:
    with open(fp, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip()]

def exec_sql(cur, sql: str):
    cur.execute(sql)
    if cur.description is not None:
        cur.fetchall()

def main():
    if len(sys.argv) != 2:
        print("Usage: python query_evaluation_cte_pg.py <config>")
        sys.exit(1)

    cfg = load_cfg(sys.argv[1])
    dsn = cfg['database'].get('dsn') or cfg['database'].get('path')
    if not dsn:
        print("Missing database dsn/path in config")
        sys.exit(1)
    qfile = cfg['queries'].get('cte_query_file')
    if not qfile:
        print("Missing cte_query_file in config")
        sys.exit(1)

    sqls = load_sqls(qfile)
    if len(sqls) == 0:
        print("Query file is empty")
        sys.exit(1)

    mat_sqls = sqls[:-1]
    main_sql = sqls[-1]

    mat_time = 0       
    creation_time = 0     
    main_time = 0

    cache = cfg['database'].get('cache')
    threads_param = cfg['database'].get('threads')
    if threads_param:
        # Keep only as informational log
        print(f"threads (info): {threads_param}")

    def run_mat(sql: str) -> int:
        with psycopg2.connect(dsn) as c, c.cursor() as cur:
            c.autocommit = True
            start = time.perf_counter_ns()
            try:
                cur.execute(sql)
                if cur.description is not None:
                    cur.fetchall()
                import re
                match = re.search(r'CREATE\s+(?:MATERIALIZED\s+VIEW|TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)', sql, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    
                    cur.execute("SELECT count(*) FROM %s", (psycopg2.extensions.AsIs(table_name),))
                else:
                    print("Failed to extract table name from SQL")
                    return 0

                end = time.perf_counter_ns()
                return end - start
            except Exception as e:
                print(f"Query execution failed: {e}")
                return 0

    if mat_sqls:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        max_workers = cfg.get('parallelism') or min(8, len(mat_sqls))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for fut in as_completed([pool.submit(run_mat, s) for s in mat_sqls]):
                mat_time += fut.result()

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    if cache:
        try:
            with conn.cursor() as c:
                c.execute(f"SET work_mem TO '{cache}';")
                c.execute(f"SET temp_buffers TO '{cache}';")
        except Exception as e:
            print(f"Failed to set cache parameters: {e}")

    with conn.cursor() as cur:
        start_main = time.perf_counter_ns()
        exec_sql(cur, main_sql)
        main_time = time.perf_counter_ns() - start_main

    total = mat_time + creation_time + main_time
    payload = {
        "materialization_time_ns": int(mat_time),
        "temp_table_time_ns": int(creation_time),
        "main_query_time_ns": int(main_time),
        "total_time_ns": int(total),
    }
    print("QSC_RESULT_JSON: " + json.dumps(payload, ensure_ascii=False))

if __name__ == '__main__':
    main() 