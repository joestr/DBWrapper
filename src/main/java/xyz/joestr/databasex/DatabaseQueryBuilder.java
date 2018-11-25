/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xyz.joestr.databasex;

/**
 *
 * @author schueler
 */
public class DatabaseQueryBuilder {
    
    private String sqlString = "";
    
    public enum Action {
        CREATE_TABLE,
        INSERT_INTO,
        SELECT,
        UPDATE,
        DELETE_FROM
    }
    
    public DatabaseQueryBuilder doAn(Action g) {
        
        if(g == Action.CREATE_TABLE) this.sqlString += "CREATE TABLE $table ($columns);";
        if(g == Action.INSERT_INTO) this.sqlString += "INSERT INTO $table VALUES($columns);";
        if(g == Action.SELECT) this.sqlString += "SELECT $columns FROM $table WHERE $condition;";
        if(g == Action.UPDATE) this.sqlString += "UPDATE $table SET $columns WHERE $condition;";
        if(g == Action.DELETE_FROM) this.sqlString += "DELETE FROM $table WHERE $condition;";
        return this;
    }
    
    public DatabaseQueryBuilder setTable(String table) {
        
        this.sqlString = this.sqlString.replace("$table", table);
        return this;
    }
    
    public DatabaseQueryBuilder setColumns(String columns) {
        
        this.sqlString = this.sqlString.replace("$columns", columns);
        return this;
    }
    
    public DatabaseQueryBuilder setCondition(String condition) {
        
        this.sqlString = this.sqlString.replace("$condition", condition);
        return this;
    }
    
    public String build() {
        
        return this.sqlString;
    }
}
