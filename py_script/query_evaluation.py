#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Query evaluation (DuckDB).

Multi-process safe output protocol:
- Always prints a single machine-readable line:
  QSC_RESULT_JSON: {...}
"""

import json, sys, os, time, duckdb
from typing import Dict, List, Any, Tuple
import pandas as pd

class QueryEvaluator:
    def __init__(self, config_path: str):

        self.config = self._load_config(config_path)
        self.db_connection = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load config: {e}")
            sys.exit(1)
    
    def _connect_database(self):

        try:
            db_path = self.config['database']['path']
            self.db_connection = duckdb.connect(db_path, read_only=True)
            cache = self.config['database'].get('cache')
            threads = self.config['database'].get('threads')
            if cache:
                try:
                    self.db_connection.execute(f"PRAGMA memory_limit='{cache}'")
                except Exception:
                    pass
            if threads:
                self.db_connection.execute(f"PRAGMA threads={int(threads)}")
            print(f"Connected to DuckDB: {db_path}")
        except Exception as e:
            print(f"Failed to connect database: {e}")
            sys.exit(1)
    
    def _load_queries_from_file(self, file_path: str) -> List[str]:

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Failed to load query file: {e}")
            return []
    
    def execute_query(self, query: str) -> Tuple[pd.DataFrame, int]:
        start_time = time.perf_counter_ns()
        try:
            df = self.db_connection.execute(query).fetchall()
            end_time = time.perf_counter_ns()
            return df, end_time - start_time
        except Exception as e:
            print(f"Query execution failed: {e}")
            return pd.DataFrame(), 0
    
    def execute_queries_sequential(self, queries: List[str]) -> Tuple[List, int]:
        start_time = time.perf_counter_ns()
        all_results = set()
        
        for query in queries:
            try:
                df = self.db_connection.execute(query).df()
                # 将DataFrame结果转换为元组以便加入集合
                for _, row in df.iterrows():
                    all_results.add(tuple(row))
            except Exception as e:
                print(f"Sequential execution failed: {e}")
        
        end_time = time.perf_counter_ns()
        return list(all_results), end_time - start_time
    
    def evaluate_queries(self) -> Dict[str, int]:
        results = {}
        
        # 执行直接查询
        direct_query_file = self.config.get('queries', {}).get('direct_query_file', '')
        if direct_query_file and os.path.exists(direct_query_file):
            print("Running direct queries...")
            direct_queries = self._load_queries_from_file(direct_query_file)
            if direct_queries:
                # 执行所有直接查询并记录总时间
                total_time = 0
                for query in direct_queries:
                    _, query_time = self.execute_query(query)
                    total_time += query_time
                results['direct_query_time_ns'] = total_time
                results['total_time_ns'] = total_time
    
        return results
    
    def close(self):
        if self.db_connection:
            self.db_connection.close()


def main():
    if len(sys.argv) != 2:
        print("Usage: python query_evaluation.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    if not os.path.exists(config_file):
        print(f"Config file not found: {config_file}")
        sys.exit(1)
    
    # 创建查询评估器
    evaluator = QueryEvaluator(config_file)
    
    try:
        # 连接数据库
        evaluator._connect_database()
        
        # 执行查询评估
        results = evaluator.evaluate_queries()

        # machine-readable output (single line)
        print("QSC_RESULT_JSON: " + json.dumps(results, ensure_ascii=False))
            
    finally:
        evaluator.close()


if __name__ == "__main__":
    main()
