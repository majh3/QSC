#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import sys
import subprocess
import importlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    if len(sys.argv) != 2:
        print("用法: python run_query_python.py <config.json>")
        sys.exit(1)

    cfg_path = sys.argv[1]
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"读取配置失败: {e}")
        sys.exit(1)

    db_type = cfg.get('database', {}).get('type', '').lower()

    if db_type in ("duckdb", "duck", ""):
        # 动态导入 duckdb 原脚本
        mod = importlib.import_module('py_script.query_evaluation')
        mod.main_from_external(cfg_path)
    elif db_type in ("pg", "postgres", "postgresql"):
        mod = importlib.import_module('py_script.query_evaluation_pg')
        mod.main_from_external(cfg_path)
    else:
        print(f"暂不支持数据库类型: {db_type}")
        sys.exit(1)


if __name__ == '__main__':
    main() 