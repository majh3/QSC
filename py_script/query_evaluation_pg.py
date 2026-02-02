#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import sys
import time
from typing import List, Dict, Any

try:
    import psycopg2
except ImportError:
    print("Please install psycopg2: pip install psycopg2-binary")
    sys.exit(1)

def load_config(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_queries(fp: str) -> List[str]:
    with open(fp, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip()]

def execute_queries(dsn: str, sqls: List[str], cache: str = None) -> int:
    total_ns = 0
    conn = psycopg2.connect(dsn)
    conn.autocommit = True

    # 设置会话级内存参数
    if cache:
        try:
            with conn.cursor() as c:
                c.execute(f"SET work_mem TO '{cache}';")
                c.execute(f"SET temp_buffers TO '{cache}';")
        except Exception as e:
            print(f"缓存参数设置失败: {e}")

    with conn.cursor() as cur:
        for sql in sqls:
            start = time.perf_counter_ns()
            cur.execute(sql)
            if cur.description is not None:
                cur.fetchall()
            total_ns += time.perf_counter_ns() - start
    conn.close()
    return total_ns

def main():
    if len(sys.argv) != 2:
        print("Usage: python query_evaluation_pg.py <config_file>")
        sys.exit(1)

    cfg = load_config(sys.argv[1])
    dsn = cfg['database'].get('dsn') or cfg['database'].get('path')
    if not dsn:
        print("Missing database dsn/path in config")
        sys.exit(1)
    cache = cfg['database'].get('cache')
    qfile = cfg['queries'].get('direct_query_file')
    if not qfile:
        print("Missing direct_query_file in config")
        sys.exit(1)

    sqls = load_queries(qfile)
    if not sqls:
        print("Query file is empty")
        sys.exit(1)

    total = execute_queries(dsn, sqls, cache)
    payload = {"direct_query_time_ns": int(total), "total_time_ns": int(total)}
    print("QSC_RESULT_JSON: " + json.dumps(payload, ensure_ascii=False))

def main_from_external(config_path: str):
    sys.argv = [sys.argv[0], config_path]
    main()


if __name__ == '__main__':
    main()