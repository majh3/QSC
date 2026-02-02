package qsc.db;
import java.sql.ResultSet;
@FunctionalInterface
public interface QueryHandlerWithInfoInt<T> {
    T handle(ResultSet rs, int qi_1, int qi_2) throws Exception;
}