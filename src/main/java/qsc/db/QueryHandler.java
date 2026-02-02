package qsc.db;

import java.sql.ResultSet;

@FunctionalInterface
public interface QueryHandler<T> {
    T handle(ResultSet rs) throws Exception;
}