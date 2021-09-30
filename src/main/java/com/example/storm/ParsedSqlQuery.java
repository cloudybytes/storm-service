package com.example.storm;

public class ParsedSqlQuery {
    private String from_table;
    private String[] select_columns;
    private String[] where;
    private String[] join;

    public String getFrom_table() {
        return from_table;
    }

    public String[] getJoin() {
        return join;
    }

    public void setJoin(String[] join) {
        this.join = join;
    }

    public String[] getWhere() {
        return where;
    }

    public void setWhere(String[] where) {
        this.where = where;
    }

    public String[] getSelect_columns() {
        return select_columns;
    }

    public void setSelect_columns(String[] select_columns) {
        this.select_columns = select_columns;
    }
    
    public void setFrom_table(String from_table) {
        this.from_table = from_table;
    }
}
