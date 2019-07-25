package xyz.joestr.dbwrapper.special.mariadb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import xyz.joestr.dbwrapper.DatabaseConnectionHandler;

/**
 * Holds special functionality for MariaDB v10.0.38.
 *
 * @author Joel
 */
public class v10_0_38 {

    DatabaseConnectionHandler databaseConnectionHandler = null;

    public v10_0_38(DatabaseConnectionHandler databaseConnectionHandler) {
        this.databaseConnectionHandler = databaseConnectionHandler;
    }

    /**
     * Disables foreign key checks.
     *
     * @return {@code true} if successfull; {@code false} if not
     * @throws SQLException If something SQL relevant fails
     */
    public boolean disableForeignKeyChecks() throws SQLException {

        boolean result = true;

        // Connect
        this.databaseConnectionHandler.connect();

        // No possible SQL-Injection here, so we can use a statement
        Statement statement = this.databaseConnectionHandler.getConnection().createStatement();

        result = statement.execute("SET FOREIGN_KEY_CHECKS = 0;");

        // Disconnect
        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Enables foreign key checks.
     *
     * @return {@code true} if successfull; {@code false} if not
     * @throws SQLException If something SQL relevant fails
     */
    public boolean enableForeignKeyCheck() throws SQLException {

        boolean result = true;

        // Connect
        this.databaseConnectionHandler.connect();

        // No possible SQL-Injection here, so we can use a statement
        Statement statement = this.databaseConnectionHandler.getConnection().createStatement();

        result = statement.execute("SET FOREIGN_KEY_CHECKS = 1;");

        // Disconnect
        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Get all table names in the database.
     *
     * @return An string array of table names. In case of an error: {@code null}
     * @throws SQLException If something SQL relevant fails
     */
    public String[] getAllTableNames() throws SQLException {

        String[] result = null;

        // Connect
        this.databaseConnectionHandler.connect();

        // Possible SQL-Injection here, so we have to use a prepared statement
        PreparedStatement tableNamesStatement
            = this.databaseConnectionHandler.getConnection().prepareStatement(
                "SELECT `table_name` FROM `information_schema`.`tables` WHERE `table_schema` = '"
                + this.databaseConnectionHandler.getConnection().getCatalog()
                + "';"
            );

        // The table names
        ResultSet tableNames = tableNamesStatement.executeQuery();

        int rowcount = 0;

        if (tableNames.last()) {
            rowcount = tableNames.getRow();
            tableNames.beforeFirst(); // not #first() because the #next() below will move on, missing the first element
        }

        result = new String[rowcount];

        int count = 0;

        while (tableNames.next()) {

            result[count] = tableNames.getString(1);

            count++;
        }

        return result;
    }

    /**
     * Delete a table in the database.
     *
     * @param tableName The name of the table.
     * @return {@code true} if successfull; {@code false} if one or more tables
     * could not be deleted
     * @throws SQLException If something SQL relevant fails
     */
    public boolean deleteTable(String tableName) throws SQLException {

        boolean result = true;

        // Connect
        this.databaseConnectionHandler.connect();

        // Possible SQL-Injection here, so we have to use a prepared statement
        PreparedStatement dropTableStatement
            = this.databaseConnectionHandler.getConnection().prepareStatement(
                "DROP TABLE `" + tableName + "`;"
            );

        // The table names
        result = dropTableStatement.execute();

        // Disconnect
        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Delete all tables in the database.
     *
     * @return {@code true} if successfull; {@code false} if one or more tables
     * could not be deleted
     * @throws SQLException If something SQL relevant fails
     */
    public boolean deleteAllTables() throws SQLException {

        boolean result = true;

        for (String tableName : this.getAllTableNames()) {

            // If there was 'false' once, it stays false
            result = result && this.deleteTable(tableName);
        }

        return result;
    }
}
