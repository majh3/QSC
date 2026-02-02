package qsc.db;

public class QueryInfo {
    public int headFucID;
    public int headArity;
    public int bodySize;
    public int[] bodyFucIDs;
    public int[] bodyArities;
    public String query;
    public String rule_query;
    public int id=-1;
    public boolean activate = true;
    public QueryInfo() {
    }
}