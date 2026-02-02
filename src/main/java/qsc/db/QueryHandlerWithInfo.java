package qsc.db;
import java.sql.ResultSet;
@FunctionalInterface
public interface QueryHandlerWithInfo<T> {
    T handle(ResultSet rs, QueryInfo queries_info) throws Exception;
}