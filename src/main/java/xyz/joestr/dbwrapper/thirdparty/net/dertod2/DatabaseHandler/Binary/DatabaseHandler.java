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
    public static boolean debugMode;

    private static DatabaseHandler databaseHandler;
    private static AbstractDatabase abstractDatabase;
    private static EntryHandler entryHandler;


    public DatabaseHandler(boolean debugMode, String databaseTyper, int port, String host, String database, String username, String password, File folder) {

        DatabaseHandler.databaseHandler = this;

        DatabaseHandler.debugMode = debugMode;

        DatabaseType databaseType = DatabaseType.byName(databaseTyper);
        
        if (databaseType != null) {
            
            try {
                switch (databaseType) {
                case MySQL:
                    DatabaseHandler.abstractDatabase = new MySQLDatabase(host,
                            port != -1 ? port : DatabaseType.MySQL.getDriverPort(),
                            database, username,
                            password);
                    break;
                case PostGRE:
                    DatabaseHandler.abstractDatabase = new PostGREDatabase(host,
                            port != -1 ? port : DatabaseType.PostGRE.getDriverPort(),
                            database, username,
                            password);
                    break;
                case SQLite:
                    DatabaseHandler.abstractDatabase = new SQLiteDatabase(database, folder);
                    break;
                }

                if (DatabaseHandler.abstractDatabase != null)
                    DatabaseHandler.entryHandler = DatabaseHandler.abstractDatabase.getHandler();
            } catch (SQLException exc) {
                // Handling
            }
        } else {
            // Handling
            System.exit(-1);
        }

    }

    public void onDisable() {
        DatabaseHandler.abstractDatabase.shutdown();
        DatabaseHandler.abstractDatabase = null;
    }

    public static AbstractDatabase get() {
        return DatabaseHandler.abstractDatabase;
    }

    public static Connection getConnection() throws SQLException {
        if (DatabaseHandler.abstractDatabase == null)
            return null;
        return DatabaseHandler.abstractDatabase.getConnection();
    }

    public static DatabaseHandler getInstance() {
        return DatabaseHandler.databaseHandler;
    }

    /**
     * Creates an extra SQLite Database besides the integrated opened database<br />
     * Only use the entry handler over the internal funtion inside the
     * SQLiteDatabase Object!
     * 
     * @param file
     *            File Object
     * @return SQLiteDatabase Object
     * @throws SQLException
     */
    public static SQLiteDatabase getExtraMySQLDatabase(File file) throws SQLException {
        return new SQLiteDatabase(file);
    }

    /**
     * Returns the {@link EntryHandler} for working with the database
     * 
     * @return EntryHandler
     * @deprecated use the EntryHandler over the
     *             {@link AbstractDatabase#getHandler()} method
     */
    public static EntryHandler getHandler() {
        return DatabaseHandler.entryHandler;
    }
}