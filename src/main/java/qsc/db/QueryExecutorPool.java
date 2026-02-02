package qsc.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import qsc.util.Pair;

public class QueryExecutorPool {
    public ConnectionPool connectionPool;
    private ExecutorService executorService;
    private final AffinityThreadFactory threadFactory;
    private final long QUERYTIMEOUTLIMIT = 20L;

    public QueryExecutorPool(ConnectionPool connectionPool, int threadCount) {
        this.connectionPool = connectionPool;
        this.threadFactory = new AffinityThreadFactory(connectionPool);
        this.executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
    }

    // // 并行执行多个查询
    // public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler) throws Exception {
    //     List<T> results = new ArrayList<>();

        
    //     List<CompletableFuture<T>> futures = new ArrayList<>();

    //     // 提交所有查询任务到线程池
    //     for (String query : queries) {
    //         CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
    //             Connection conn = null;
    //             try {
    //                 // long start = System.currentTimeMillis();
    //                 conn = connectionPool.getConnection();
    //                 // long end = System.currentTimeMillis();
    //                 Statement stmt = conn.createStatement();
    //                 // long end2 = System.currentTimeMillis();
    //                 ResultSet rs = stmt.executeQuery(query);
    //                 // long end3 = System.currentTimeMillis();
    //                 T result = handler.handle(rs); // 使用 handler 处理结果
    //                 // long end4 = System.currentTimeMillis();
    //                 rs.close();
    //                 stmt.close();
    //                 // long end5 = System.currentTimeMillis();

    //                 // System.out.print("Connection: " + (end - start)+ " ");
    //                 // System.out.print(" Statement: " + (end2 - end)+ " ");
    //                 // System.out.print(" Execute: " + (end3 - end2)+ " ");
    //                 // System.out.print(" Handle: " + (end4 - end3)+ " ");
    //                 // System.out.print(" Close: " + (end5 - end4)+ " ");
    //                 // System.out.println("Total: " + (end5 - start));
    //                 return result;
    //             } catch (Exception e) {
    //                 e.printStackTrace(); // 记录错误
    //                 return null; // 返回 null 避免影响其他任务
    //             } finally {
    //                 if (conn != null) {
    //                     connectionPool.releaseConnection(conn); // 归还连接
    //                 }
    //             }
    //         }, executorService);
    //         futures.add(future);
    //     }

    //     // 等待所有查询任务完成并收集结果
    //     for (CompletableFuture<T> future : futures) {
    //         try {
    //             T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
    //             if (result != null) {
    //                 results.add(result); // 忽略 null 结果
    //             }
    //         } catch (Exception e) {
    //             e.printStackTrace(); // 捕获并记录单个任务的异常
    //         }
    //     } 
    //     return results; // 返回所有查询的结果列表
    // }



public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler, int maxConcurrent) throws Exception {  
    List<T> results = new ArrayList<>();  
    List<CompletableFuture<T>> futures = new ArrayList<>();  
    // 建立 Semaphore 以限制同时执行的查询数量  
    Semaphore semaphore = new Semaphore(maxConcurrent);  

    for (String query : queries) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            try {  
                // 获取许可，控制并发数量  
                semaphore.acquire();  
                Connection conn = null;  
                PreparedStatement ps = null;  
                ResultSet rs = null;  
                try {  
                    conn = connectionPool.getConnection();  

                    // 使用带有类型 & 并发模式的 PreparedStatement  
                    ps = conn.prepareStatement(query,  
                            ResultSet.TYPE_FORWARD_ONLY,  
                            ResultSet.CONCUR_READ_ONLY);  
                    ps.setFetchSize(10_000);  

                    rs = ps.executeQuery();  
                    // 交给外部的 handler 去处理 ResultSet  
                    return handler.handle(rs);  

                } catch (Exception e) {  
                    // e.printStackTrace(); // 记录错误  
                    return null; // 返回 null 避免影响其他任务  
                } finally {  
                    // 关闭 ResultSet 和 PreparedStatement  
                    if (rs != null) {  
                        try {  
                            rs.close();  
                        } catch (SQLException e) {  
                            e.printStackTrace();  
                        }  
                    }  
                    if (ps != null) {  
                        try {  
                            ps.close();  
                        } catch (SQLException e) {  
                            e.printStackTrace();  
                        }  
                    }  
                    // 归还连接到连接池  
                    if (conn != null) {  
                        connectionPool.releaseConnection(conn);  
                    }  
                }  
            } catch (InterruptedException e) {  
                Thread.currentThread().interrupt(); // 恢复中断状态  
                return null;  
            } finally {  
                // 释放许可  
                semaphore.release();  
            }  
        });  
        futures.add(future);  
    }  
    boolean failed = false;
    // 等待所有查询任务完成并收集结果  
    for (CompletableFuture<T> future : futures) {
        try {
            T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS);
            if (result != null) {
                results.add(result);
            }else{
                failed = true;
                break;
            }
        } catch (Exception e) {
            // 其他异常同样不终止循环
            System.out.println("Query failed: " + e.getMessage());
            failed = true;
            break;
        }
    }    
    if(failed){
        // close all future
        for (CompletableFuture<T> future : futures) {
            future.cancel(true);
        }
        return null;
    }
    // 返回所有查询的结果列表  
    return results;  
}
    public <T> List<T> executeQueriesConcurrently(String[] queries, QueryHandler<T> handler) throws Exception {
        List<T> results = new ArrayList<>();
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (String query : queries) {
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();

                    // 使用带有类型 & 并发模式的 PreparedStatement
                    ps = conn.prepareStatement(query, 
                            ResultSet.TYPE_FORWARD_ONLY, 
                            ResultSet.CONCUR_READ_ONLY);
                    ps.setFetchSize(10_000);

                    rs = ps.executeQuery();
                    // 交给外部的 handler 去处理 ResultSet
                    return handler.handle(rs);

                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    // 关闭 ResultSet 和 PreparedStatement
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    // 归还连接到连接池
                    if (conn != null) {
                        connectionPool.releaseConnection(conn);
                    }
                }
            }, executorService);
            futures.add(future);
        }
        boolean failed = false;
        // 等待所有查询任务完成并收集结果
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                // close all future
                System.out.println("Query failed: " + e.getMessage());
                failed = true;
                break;
            }
        }
        if(failed){
            // close all future
            for (CompletableFuture<T> future : futures) {
                future.cancel(true);
            }
            // // 归还所有连接
            // for (Connection conn : connectionPool.getConnections()) {
            //     connectionPool.releaseConnection(conn);
            // }
            return null;
        }
        // 返回所有查询的结果列表
        return results;
    }
    // 并行执行多个查询
    public <T> List<T> executeQueriesWithInfo2(String[] queries, int[] qi_1, int[] qi_2, QueryHandlerWithInfoInt<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, qi_1[k], qi_2[k]); // 使用 handler 处理结果
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        } 

        return results; // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> List<T> executeQueriesWithInfo(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for (CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        } 

        return results; // 返回所有查询的结果列表
    }
    public <T> List<T> executeQueriesWithInfo(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<T>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (QueryInfo qi: queries_info) {
            if(qi.activate == false) {
                continue;
            }
            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(qi.query);
                    T result = handler.handle(rs, qi); // 使用 handler 处理结果
                    return result;
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        for(CompletableFuture<T> future : futures) {
            try {
                T result = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return results; // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(HashSet<QueryInfo> queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (QueryInfo qi: queries_info) {
            if(qi.activate == false) {
                continue;
            }
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(qi.query);
                    T result = handler.handle(rs, qi); // 使用 handler 处理结果
                    return new Pair<T, Integer>(result, qi.id);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for(CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                T result = pair.getFirst();
                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for(int i = 0; i < queries_info.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(queries_info[k].query);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    // 确保资源按正确顺序关闭：ResultSet -> Statement -> Connection
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 并行执行多个查询
    public <T> Pair<List<T>, List<Integer>> executeQueriesWithInfoWithEmptyID(String[] queries, QueryInfo[] queries_info, QueryHandlerWithInfo<T> handler) throws InterruptedException, ExecutionException {
        List<CompletableFuture<Pair<T, Integer>>> futures = new ArrayList<>();

        // 提交所有查询任务到线程池
        for (int i = 0; i < queries.length; i++) {
            int k = i;
            CompletableFuture<Pair<T, Integer>> future = CompletableFuture.supplyAsync(() -> {
                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = connectionPool.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(queries[k]);
                    T result = handler.handle(rs, queries_info[k]); // 使用 handler 处理结果
                    return new Pair<T, Integer>(result, k);
                } catch (Exception e) {
                    e.printStackTrace(); // 记录错误
                    return null; // 返回 null 避免影响其他任务
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (conn != null) {
                        connectionPool.releaseConnection(conn); // 归还连接
                    }
                }
            }, executorService);
            futures.add(future);
        }
        // 等待所有查询任务完成并收集结果
        List<T> results = new ArrayList<>();
        List<Integer> IDs = new ArrayList<>();
        for (CompletableFuture<Pair<T, Integer>> future : futures) {
            try {
                Pair<T, Integer> pair = future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 获取任务结果
                T result = pair.getFirst();

                if (result != null) {
                    results.add(result); // 忽略 null 结果
                    IDs.add(pair.getSecond());
                }
            } catch (Exception e) {
                e.printStackTrace(); // 捕获并记录单个任务的异常
            }
        }
        return new Pair<List<T>, List<Integer>>(results, IDs); // 返回所有查询的结果列表
    }
    // 执行单个查询
    public boolean executeQuery(String query) throws Exception {
        return executorService.submit(() -> {
            Connection conn = null;
            try {
                conn = connectionPool.getConnection();
                Statement stmt = conn.createStatement();
                boolean result = stmt.execute(query);
                stmt.close();
                return result;
            } finally {
                if (conn != null) {
                    connectionPool.releaseConnection(conn); // 归还连接
                }
            }
        }).get(); // 等待线程完成并获取结果
    }

    // 优化NOT EXISTS查询的方法
    private String optimizeNotExistsQuery(String query) {
        // 简单的NOT EXISTS查询优化
        if (query.contains("NOT EXISTS") && query.contains("LIMIT 1")) {
            // 匹配模式: SELECT ... FROM table WHERE NOT EXISTS (SELECT * FROM t WHERE ...) LIMIT 1
            String pattern = "SELECT\\s+([^\\s]+)\\s*\\.\\s*([^\\s]+)\\s+AS\\s+([^\\s,]+),\\s*'([^']+)'\\s+AS\\s+([^\\s]+)\\s+FROM\\s+([^\\s]+)\\s+WHERE\\s+NOT\\s+EXISTS\\s*\\(\\s*SELECT\\s+\\*\\s+FROM\\s+([^\\s]+)\\s+WHERE\\s+([^)]+)\\)\\s+LIMIT\\s+1";
            
            if (query.matches(pattern)) {
                // 提取各个部分
                String[] parts = query.split("\\s+");
                String table1 = null, table2 = null, column1 = null, column2 = null;
                String value1 = null, value2 = null;
                
                // 解析查询结构
                for (int i = 0; i < parts.length; i++) {
                    if (parts[i].equals("FROM") && table1 == null) {
                        table1 = parts[i + 1].replace("\"", "");
                    } else if (parts[i].equals("FROM") && table1 != null) {
                        table2 = parts[i + 1].replace("\"", "");
                    } else if (parts[i].contains(".") && parts[i].contains("_1")) {
                        column1 = parts[i].split("\\.")[1].replace("\"", "");
                    } else if (parts[i].equals("'13'")) {
                        value1 = "13";
                    } else if (parts[i].equals("'37'")) {
                        value1 = "37";
                    }
                }
                
                if (table1 != null && table2 != null && column1 != null && value1 != null) {
                    // 构建优化的LEFT JOIN查询
                    String optimizedQuery = String.format(
                        "SELECT %s.%s AS t_1, '%s' AS t_2 " +
                        "FROM %s " +
                        "LEFT JOIN %s ON %s.%s = %s.%s AND %s.t_2 = '%s' " +
                        "WHERE %s.%s IS NULL " +
                        "LIMIT 1",
                        table1, column1, value1,
                        table1, table2, table1, column1, table2, "t_1", table2, value1,
                        table2, "t_1"
                    );
                    
                    // 优化查询已生成
                    return optimizedQuery;
                }
            }
        }
        return query; // 如果无法优化，返回原查询
    }

    public int executeQuerySingleThread1Line(String query) {
        // 尝试优化查询
        // String optimizedQuery = optimizeNotExistsQuery(query);
        
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs_1 = null;
        try {
            conn = connectionPool.getConnection();
            
            // stmt = conn.createStatement();
            ps = conn.prepareStatement(query);
            ps.setQueryTimeout(10); // 增加到30秒超时，与QUERYTIMEOUTLIMIT保持一致
            ps.setFetchSize(1);     // 每次从服务端取 1 行

            long startTime = System.currentTimeMillis();
            rs_1 = ps.executeQuery();
            
            if (rs_1.next()) {
                return 0;
            } else {
                return 1;
            }
        }  catch (Exception e) {
            System.out.println("Query timed out!");
            return -1;
        } finally {
            try {
                if (rs_1 != null) rs_1.close();
                if (ps != null) ps.close();
            } catch (SQLException e) {
                // 忽略关闭资源时的异常
            }
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }                      // 结束事务
    
    public <T> T executeQuerySingleThread(String query, QueryHandler<T> handler) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = connectionPool.getConnection();
            stmt = conn.createStatement();
            stmt.setQueryTimeout(15);
            
            rs = stmt.executeQuery(query);
            
            return handler.handle(rs);
        } catch (Exception e) {
            System.out.println("Query timed out!");
            return null;
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                // 忽略关闭资源时的异常
            }
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }
    
    public <T> T explainQuerySingleThread(String query, QueryHandler<T> handler) {  
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {  
            Connection conn = null;  
            Statement stmt = null;  
            ResultSet rs = null;  
            try {  
                conn = connectionPool.getConnection();  
                stmt = conn.createStatement();  
                rs = stmt.executeQuery(query);  
                return handler.handle(rs); // 使用 handler 处理结果  
            } catch (Exception e) {  
                e.printStackTrace(); // 记录错误  
                throw new RuntimeException("Query execution failed", e); // 抛出运行时异常  
            } finally {  
                // 确保资源被关闭  
                try {  
                    if (rs != null) rs.close();  
                    if (stmt != null) stmt.close();  
                } catch (SQLException e) {  
                    e.printStackTrace(); // 记录关闭资源时的异常  
                }  
                if (conn != null) {  
                    connectionPool.releaseConnection(conn); // 归还连接  
                }  
            }  
        }, executorService);  
    
        try {  
            return future.get(QUERYTIMEOUTLIMIT, TimeUnit.SECONDS); // 等待线程完成并获取结果  
        } catch (Exception e) {  
            System.out.println("Task timed out!");  
            future.cancel(true); // 强制取消任务  
            return null; // 或者抛出自定义异常  
        }
    }
    // 由外部负责管理线程池生命周期
    public void shutdown() {
        executorService.shutdown();
    }

}
