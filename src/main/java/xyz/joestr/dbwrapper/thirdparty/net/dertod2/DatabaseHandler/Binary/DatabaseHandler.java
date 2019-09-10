package xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Binary;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.AbstractDatabase;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.DatabaseType;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.MySQLDatabase;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.PostGREDatabase;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.SQLiteDatabase;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Table.EntryHandler;


/**
 * Differences between Canary DataAccess and DerTod2's DatabaseHandler:<br />
 * + allows saving of Timestamp Objects<br />
 * + doesn't need an primary key (in canary the primary key is hardcoded)<br />
 * + sets the table name over an annotation and not over the constructor<br />
 * + automatic column type detection<br />
 * + when inserting rows with an primary key the key in the entry will be
 * updated automatically<br />
 * + access to private and protected variables, no publics needed<br />
 * + support for postgres and also mysql/sqlite<br />
 * + contains all needed drivers<br />
 * - currently no support for xml<br />
 * 
 * @author DerTod2
 *
 */
public class DatabaseHandler {

    private static DatabaseHandler databaseHandler;
    
    private static AbstractDatabase abstractDatabase;

    private DatabaseHandler() {
        
    }

    public static DatabaseHandler getInstance() {
        if(DatabaseHandler.databaseHandler == null) {
            DatabaseHandler.databaseHandler = new DatabaseHandler();
        }
        
        return DatabaseHandler.databaseHandler;
    }
    
    public void createMySQL(String host, int port, String database, String username, String password) throws SQLException {
        DatabaseHandler.abstractDatabase =
            new MySQLDatabase(
                host,
                port != -1 ? port : DatabaseType.MySQL.getDriverPort(),
                database,
                username,
                password
            );
    }
    
    public void createPostgreSQL(String host, int port, String database, String username, String password) throws SQLException {
        DatabaseHandler.abstractDatabase =
            new PostGREDatabase(
                host,
                port != -1 ? port : DatabaseType.PostGRE.getDriverPort(),
                database,
                username,
                password
            );
    }
    
    public void createSQLite(File folder, String database) throws SQLException {
        DatabaseHandler.abstractDatabase =
            new SQLiteDatabase(
                database,
                folder
            );
    }
    
    public AbstractDatabase getAbstractDatabase() {
        return DatabaseHandler.abstractDatabase;
    }

    public Connection getConnection() throws SQLException {
        if (DatabaseHandler.abstractDatabase == null)
            return null;
        return DatabaseHandler.abstractDatabase.getConnection();
    }
}