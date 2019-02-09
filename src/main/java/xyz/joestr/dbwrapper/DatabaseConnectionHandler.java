/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xyz.joestr.dbwrapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Handles the connection to a database;
 *
 * @author Joel Strasser (joestr)
 * @version ${project.version}
 */
public class DatabaseConnectionHandler {

    private final String connectionString;
    private Connection connection;
    private boolean persistentConnection = false;

    /**
     * Create an instance of
     * {@link xyz.joestr.dbwrapper.DatabaseConnectionHandler}.
     *
     * @param connectionString The connection string
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public DatabaseConnectionHandler(String connectionString) {

        this.connectionString = connectionString;
    }

    /**
     * Connects to a database using the {@code connectionString}.
     *
     * @throws SQLException If something SQL relevant fails
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public void connect() throws SQLException {

        if (this.persistentConnection) {

            if (!this.connection.isValid(0)) {

                this.connection = DriverManager.getConnection(this.connectionString);
            }

            return;
        }

        this.connection = DriverManager.getConnection(connectionString);
    }

    /**
     * Disconnects form a database.
     *
     * @throws SQLException If something SQL relevant fails
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public void disconnect() throws SQLException {

        if (this.persistentConnection) {

            return;
        }

        this.connection.close();
    }

    /**
     * Returns the {@code connectionString}.
     *
     * @return The {@link java.lang.String connectionString}
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public String getConnectionString() {
        return this.connectionString;
    }

    /**
     * Returns the {@code connection}.
     *
     * @return The {@link java.sql.Connection} to the database
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public Connection getConnection() {
        return this.connection;
    }

    /**
     * Returns {@code true} if the connection should not be closed after an
     * operation. Else {@code false}.
     *
     * @return A {@code boolean}
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public boolean isPersistentConnection() {
        return persistentConnection;
    }

    /**
     * Set the flag, if the connection should not be closed after an operation.
     *
     * @param persistentConnection If the connection should not be closed after
     * an operation
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public void setPersistentConnection(boolean persistentConnection) {
        this.persistentConnection = persistentConnection;
    }
}
