#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_query_python_cte.py
 duckdb  → query_evaluation_cte.py
 pg      → query_evaluation_cte_pg.py
"""
import json
import os
import sys
import importlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)


def main():
    if len(sys.argv) != 2:
        print("用法: python run_query_python_cte.py <config.json>")
        sys.exit(1)

    cfg_path = sys.argv[1]
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    db_type = cfg.get('database', {}).get('type', '').lower()

    if db_type in ("duckdb", "duck", ""):
        mod = importlib.import_module('py_script.query_evaluation_cte')
        mod.main_from_external(cfg_path)
    elif db_type in ("pg", "postgres", "postgresql"):
        mod = importlib.import_module('py_script.query_evaluation_cte_pg')
        mod.main_from_external(cfg_path)
    else:
        print(f"暂不支持数据库类型: {db_type}")
        sys.exit(1)


if __name__ == '__main__':
    main()
