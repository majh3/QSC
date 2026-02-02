#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CTE query evaluation (DuckDB).

Multi-process safe output protocol:
- Always prints a single machine-readable line:
  QSC_RESULT_JSON: {...}

Notes:
- Avoid opening the database in write mode: keep read_only=True to reduce contention
  when multiple processes evaluate the same database concurrently.
"""

import json, sys, os, time, duckdb
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

class CTEQueryEvaluator:
    def __init__(self, config_path: str):

        self.config = self._load_config(config_path)
        self.connection = None
        self.temp_files = []
        self.temp_tables = []
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load config: {e}")
            sys.exit(1)
    
    def _connect_database(self):
        """
        连接数据库
        """
        try:
            db_path = self.config['database']['path']
            # 初始使用只读模式连接DuckDB
            self.connection = duckdb.connect(db_path, read_only=True)
            cache = self.config['database'].get('cache')
            threads = self.config['database'].get('threads')
            if cache:
                try:
                    self.connection.execute(f"PRAGMA memory_limit='{cache}'")
                except Exception:
                    pass
            if threads:
                self.connection.execute(f"PRAGMA threads={int(threads)}")
            print(f"Connected to DuckDB: {db_path} (read_only)")
        except Exception as e:
            print(f"Failed to connect database: {e}")
            sys.exit(1)
    
    def _set_readonly_mode(self):

        try:
            # 关闭当前连接并重新以只读模式连接
            if self.connection:
                self.connection.close()
                self.connection = None
            db_path = self.config['database']['path']
            self.connection = duckdb.connect(db_path, read_only=True)
            print("Switched to read-only mode")
        except Exception as e:
            print(f"Failed to set read-only mode: {e}")
    
    # NOTE: write mode is intentionally disabled for multi-process safety
    
    def close_connection(self):

        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                print("Database connection closed")
            except Exception as e:
                print(f"Failed to close database connection: {e}")
    
    def _load_queries_from_file(self, file_path: str) -> List[str]:

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Failed to load query file: {e}")
            return []
    
    def _execute_materialization_query(self, db_path: str, query: str) -> Tuple[pd.DataFrame, int]:
        try:
            conn = duckdb.connect(db_path, read_only=True)
            t0 = time.perf_counter_ns()
            df = conn.execute(query).df()
            elapsed = time.perf_counter_ns() - t0
            conn.close()
            return df, elapsed
        except Exception as e:
            print(f"Materialization query failed: {e}")
            return None, 0
    
    def _create_temp_table_from_csv(self, csv_file: str, table_name: str) -> bool:
        try:
            # 使用DuckDB的CSV读取功能创建临时表
            create_sql = f"""
            CREATE TEMPORARY TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_file}')
            """
            self.connection.execute(create_sql)
            print(f"Temporary table created: {table_name}")
            return True
        except Exception as e:
            print(f"Failed to create temporary table {table_name}: {e}")
            return False
    
    def _extract_materialization_queries(self, queries: List[str]):
        mats = []
        main_query = None
        for q in queries:
            q_strip = q.strip()
            up = q_strip.upper()
            if up.startswith("CREATE TEMPORARY TABLE") and " AS " in up:
                parts = q_strip.split()
                try:
                    tbl_idx = parts.index("TABLE") + 1
                    table_name = parts[tbl_idx]
                except Exception:
                    table_name = f"tmp_{len(mats)}"
                as_pos = up.find(" AS ")
                select_part = q_strip[as_pos + 4:].strip()
                if select_part.startswith("(") and select_part.endswith(")"):
                    select_part = select_part[1:-1].strip()
                mats.append((table_name, select_part))
            else:
                main_query = q_strip
        return mats, main_query
    
    # _get_temp_table_names 不再需要，保留占位以兼容旧代码调用
    def _get_temp_table_names(self, _):
        return []
    
    def execute_cte_queries(self, queries: List[str]) -> Tuple[bool, int]:
        total_start_time = time.perf_counter_ns()
        mat_time_ns = 0
        temp_table_time_ns = 0
        main_time_ns = 0
        
        try:
            # 提取物化查询和主查询
            materialization_queries, main_query = self._extract_materialization_queries(queries)
            
            if not materialization_queries:
                print("No materialization queries found")
                return False, 0
            
            if not main_query:
                print("No main query found")
                return False, 0
            
            print(f"Found {len(materialization_queries)} materialization queries and 1 main query")
            # 并行查询 -> DataFrame
            db_path = self.config['database']['path']
            mats, _ = materialization_queries, None  # rename
            dfs = {}
            materialization_times = []
            with ThreadPoolExecutor(max_workers=min(len(mats), 16)) as pool:
                futures = {pool.submit(self._execute_materialization_query, db_path, sel): tbl for tbl, sel in mats}
                for fut in as_completed(futures):
                    table_name = futures[fut]
                    df, t_elapse = fut.result()
                    if df is None:
                        return False,0
                    dfs[table_name] = df
                    materialization_times.append(t_elapse)

            # Keep read-only connection; register DataFrame as in-memory relations
            register_start = time.perf_counter_ns()
            for table_name, df in dfs.items():
                self.connection.register(table_name, df)
                self.temp_tables.append(table_name)
            register_end = time.perf_counter_ns()
            temp_table_time_ns = register_end - register_start
            mat_time_ns = sum(materialization_times)
            
            # 在写模式下执行主查询（保持临时表可用）
            print("Running main query...")
            main_start_time = time.perf_counter_ns()
            result = self.connection.execute(main_query).fetchall()
            main_end_time = time.perf_counter_ns()
            main_time_ns = main_end_time - main_start_time
            
            print(f"Main query returned {len(result)} rows")
            
            # 计算总时间
            total_end_time = time.perf_counter_ns()
            total_execution_time = total_end_time - total_start_time
            
            # machine-readable output payload
            payload = {
                "materialization_time_ns": int(mat_time_ns),
                "temp_table_time_ns": int(temp_table_time_ns),
                "main_query_time_ns": int(main_time_ns),
                "total_time_ns": int(total_execution_time),
            }
            print("QSC_RESULT_JSON: " + json.dumps(payload, ensure_ascii=False))
            
            return True, total_execution_time
            
        except Exception as e:
            print(f"CTE evaluation failed: {e}")
            return False, 0
        finally:
            # 清理临时文件和临时表
            self._cleanup()
    
    def _cleanup(self):
        try:
            # 解除注册的临时表
            for table_name in self.temp_tables:
                try:
                    if self.connection:
                        self.connection.unregister(table_name)
                except Exception:
                    pass
            self.temp_tables.clear()
            
        except Exception as e:
            print(f"清理过程出错: {e}")
    
    def evaluate_cte_queries(self) -> int:
        try:
            # 连接数据库
            self._connect_database()
            
            # 加载查询
            query_file = self.config['queries']['cte_query_file']
            queries = self._load_queries_from_file(query_file)
            
            if not queries:
                print("No queries found")
                return 0
            
            # 执行CTE查询
            success, execution_time = self.execute_cte_queries(queries)
            
            if success:
                return execution_time
            else:
                return 0
                
        except Exception as e:
            print(f"CTE evaluation failed: {e}")
            return 0
        finally:
            # 确保连接正确关闭
            self.close_connection()

def main():
    if len(sys.argv) != 2:
        print("Usage: python query_evaluation_cte.py <config_file>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    # 创建评估器并执行
    evaluator = CTEQueryEvaluator(config_path)
    evaluator.evaluate_cte_queries()

if __name__ == "__main__":
    main() 