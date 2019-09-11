package xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgreSQLDatabase extends AbstractDatabase {

    public PostgreSQLDatabase(String host, int port, String database, String username, String password)
            throws SQLException {
        super(host, port, database, username, password);
    }

    public PostgreSQLDatabase(String host, String database, String username, String password) throws SQLException {
        super(host, DatabaseType.PostgreSQL.getDriverPort(), database, username, password);
    }

    public DatabaseType getType() {
        return DatabaseType.PostgreSQL;
    }

    protected String getConnectionString() {
        return "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.database;
    }

    public boolean tableExist(String tableName) {
        try {
            Connection connection = this.getConnection();

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
            boolean result = resultSet.next();

            this.getHandler().closeConnection(connection, null, resultSet);
            return result;
        } catch (Exception exc) {
            exc.printStackTrace();
            return false;
        }
    }
}