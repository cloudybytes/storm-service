package com.example.storm;

public class ParsedSqlQuery {
    private String from_table;
    private String[] select_columns;
    private String[] where;
    private String[] join;
    private String group_by_column;
    private String[] having_condition;
    private String aggr_function;

    public String getFrom_table() {
        return from_table;
    }

    public String getAggr_function() {
        return aggr_function;
    }

    public void setAggr_function(String aggr_function) {
        this.aggr_function = aggr_function;
    }

    public String[] getHaving_condition() {
        return having_condition;
    }

    public void setHaving_condition(String[] having_condition) {
        this.having_condition = having_condition;
    }

    public String getGroup_by_column() {
        return group_by_column;
    }

    public void setGroup_by_column(String group_by_column) {
        this.group_by_column = group_by_column;
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
