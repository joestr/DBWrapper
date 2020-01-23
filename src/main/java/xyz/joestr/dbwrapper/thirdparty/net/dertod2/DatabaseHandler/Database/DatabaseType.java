package xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database;

public enum DatabaseType {
    MySQL("com.mysql.jdbc.Driver", true, true, MySQLDatabase.class, 3306),
    PostgreSQL("org.postgresql.Driver", true, true, PostgreSQLDatabase.class, 5432),
    SQLite("org.sqlite.JDBC", true, false, SQLiteDatabase.class, null);

    private String driverPackage;
    private Class<? extends AbstractDatabase> driverClass;
    private Integer defaultPort;

    private boolean usesDatabaseDriver;
    private boolean supportConnectionPool;

    private DatabaseType(String driverPackage, boolean usesDatabaseDriver, boolean supportConnectionPool,
            Class<? extends AbstractDatabase> driverClass, Integer defaultPort) {
        this.driverPackage = driverPackage;
        this.driverClass = driverClass;
        this.defaultPort = defaultPort;

        this.usesDatabaseDriver = usesDatabaseDriver;
        this.supportConnectionPool = supportConnectionPool;
    }

    public boolean isUsingDatabaseDriver() {
        return this.usesDatabaseDriver;
    }

    public boolean isUsingConnectionPool() {
        return this.supportConnectionPool;
    }

    public String getDriverPackage() {
        return this.driverPackage;
    }

    public Class<? extends AbstractDatabase> getDriverClass() {
        return this.driverClass;
    }

    public Integer getDriverPort() {
        return this.defaultPort;
    }

    public static DatabaseType byName(String driverName) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.name().equalsIgnoreCase(driverName))
                return databaseType;
        }

        return null;
    }

    public static DatabaseType byPackage(String packageName) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.driverPackage.equals(packageName))
                return databaseType;
        }

        return null;
    }

    public static DatabaseType byClass(Class<? extends AbstractDatabase> driverClass) {
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.driverClass.equals(driverClass))
                return databaseType;
        }

        return null;
    }
}