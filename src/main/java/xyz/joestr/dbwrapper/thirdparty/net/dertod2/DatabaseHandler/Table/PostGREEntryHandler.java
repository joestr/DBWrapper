package xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Table;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import com.google.common.collect.ImmutableList;

import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Binary.DatabaseHandler;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Database.Pooler.PooledConnection;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Exceptions.EmptyFilterException;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Exceptions.NoTableColumnException;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Table.Column.ColumnType;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Table.Column.DataType;
import xyz.joestr.dbwrapper.thirdparty.net.dertod2.DatabaseHandler.Table.Column.EntryType;

// Differences: auto_increment -> dataType SERIAL or BIGSERIAL
// so do this: primary keys and auto_increment ONLY as BIGSERIAL and NOT as int
// also: postgres doesn't support the ` letter
public class PostGREEntryHandler extends EntryHandler {

    public void insert(List<TableEntry> entryList)
            throws IllegalArgumentException, IllegalAccessException, SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();

        if (!this.copyInsert(entryList, connection)) {
            for (TableEntry tableEntry : entryList)
                this.insert(tableEntry, connection);
        }

        this.closeConnection(connection, null, null);
    }

    public void insert(TableEntry tableEntry) throws IllegalArgumentException, IllegalAccessException, SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();
        this.insert(tableEntry, connection);
        this.closeConnection(connection, null, null);
    }

    private void insert(TableEntry tableEntry, Connection connection)
            throws IllegalArgumentException, IllegalAccessException, SQLException {
        if (this.copyInsert(ImmutableList.<TableEntry>of(tableEntry), connection))
            return;

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Map<Column, Object> dataList = tableEntry.getEntryColumns();
        Iterator<Column> iterator = dataList.keySet().iterator();

        StringBuilder columnList = new StringBuilder();
        StringBuilder valueList = new StringBuilder();

        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself

            columnList.append(column.columnName() + ", ");
            valueList.append("?, ");
        }

        // Remove the leading letter
        if (columnList.length() > 0)
            columnList.delete(columnList.length() - 2, columnList.length());
        if (valueList.length() > 0)
            valueList.delete(valueList.length() - 2, valueList.length());

        preparedStatement = connection.prepareStatement("INSERT INTO " + tableEntry.getTableName() + " ("
                + columnList.toString() + ") VALUES (" + valueList.toString() + ");",
                PreparedStatement.RETURN_GENERATED_KEYS);

        iterator = dataList.keySet().iterator(); // Need again... cause iterator cant start over again

        int index = 1;
        while (iterator.hasNext()) {
            Column column = iterator.next();
            if (column.autoIncrement() || column.columnType() == ColumnType.Primary)
                continue; // Skip auto Increment - the database sets the value itself
            Object columnValue = dataList.get(column);

            this.setStatement(index++, preparedStatement, columnValue, column);
        }

        preparedStatement.executeUpdate();

        boolean hasPrimaryKey = tableEntry.hasPrimaryKey();
        Column primaryKey = tableEntry.getPrimaryKey();

        if (hasPrimaryKey) {
            resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()) {
                tableEntry.setColumn(primaryKey, resultSet.getInt(primaryKey.columnName())); // Always ints
            }
        }

        tableEntry.isLoadedEntry = true;
        this.closeConnection(null, preparedStatement, resultSet);
    }

    private boolean copyInsert(List<TableEntry> entryList, Connection connection) {
        try {
            CopyManager copyManager = ((PGConnection) ((PooledConnection) connection).getRawConnection()).getCopyAPI();
            StringBuilder stringBuilder = new StringBuilder();
            final int batchSize = 75;

            // Prepare the Rows !warning! don't mix different table data here
            // The copyRows method only works with data WITHOUT an primary Key or "return
            // variables"

            Map<String, List<TableEntry>> tableEntryList = new HashMap<String, List<TableEntry>>();
            for (TableEntry tableEntry : entryList) {
                if (tableEntry.hasPrimaryKey())
                    return false;

                if (!tableEntryList.containsKey(tableEntry.getTableName()))
                    tableEntryList.put(tableEntry.getTableName(), new ArrayList<TableEntry>());
                tableEntryList.get(tableEntry.getTableName()).add(tableEntry);
            }

            for (String tableName : tableEntryList.keySet()) {
                PushbackReader pushBackReader = new PushbackReader(new StringReader(""), 10000);
                List<TableEntry> tableList = tableEntryList.get(tableName);

                for (int i = 0; i < tableList.size(); i++) {
                    TableEntry tableEntry = tableList.get(i);
                    List<Column> columnList = tableEntry.getPlainLayout();

                    for (Column column : columnList) {
                        Object data = tableEntry.getColumn(column);

                        switch (column.entryType()) {
                        case List:
                            stringBuilder.append("'" + setList((List<?>) data) + "',");
                            break;
                        case Map:
                            stringBuilder.append("'" + setMap((Map<?, ?>) data) + "',");
                            break;
                        case Normal:
                            if (column.dataType() == DataType.String) {
                                stringBuilder.append("'" + ((String) data) + "',");
                            } else {
                                stringBuilder.append(data + ",");
                            }

                            break;
                        }
                    }

                    stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
                    stringBuilder.append("\n");

                    if (i % batchSize == 0) {
                        pushBackReader.unread(stringBuilder.toString().toCharArray());
                        copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV", pushBackReader);
                        stringBuilder.delete(0, stringBuilder.length());
                    }

                    tableEntry.isLoadedEntry = true;
                }

                pushBackReader.unread(stringBuilder.toString().toCharArray());
                copyManager.copyIn("COPY " + tableName + " FROM STDIN WITH CSV", pushBackReader);
            }

            return true;
        } catch (SQLException | IOException | IllegalArgumentException | IllegalAccessException exc) {

        }

        return false;
    }

    public void remove(TableEntry tableEntry, Map<String, Object> filterList) throws SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = null;

        if (filterList != null && filterList.size() > 0) {
            StringBuilder stringBuilder = new StringBuilder();

            Iterator<String> iterator = filterList.keySet().iterator();
            while (iterator.hasNext()) {
                String columnName = iterator.next();
                if (filterList.get(columnName) == null)
                    continue; // Can't check NULL variables
                if (tableEntry.getColumn(columnName) == null)
                    throw new NoTableColumnException(columnName, tableEntry.getClass().getName());

                if (stringBuilder.length() > 0) {
                    stringBuilder.append(" AND " + columnName);
                } else {
                    stringBuilder.append(columnName);
                }

                stringBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement(
                    "DELETE FROM " + tableEntry.getTableName() + " WHERE " + stringBuilder.toString() + ";");

            int index = 1;
            iterator = filterList.keySet().iterator();
            while (iterator.hasNext()) {
                String columnName = iterator.next();
                Object columnValue = filterList.get(columnName);
                if (columnValue == null)
                    continue; // Can't check NULL variables

                Column column = tableEntry.getColumn(columnName);
                this.setStatement(index++, preparedStatement, columnValue, column);
            }

            preparedStatement.executeUpdate();
        } else {
            preparedStatement = connection.prepareStatement("TRUNCATE TABLE " + tableEntry.getTableName() + ";");
            preparedStatement.executeUpdate();
        }

        this.closeConnection(connection, preparedStatement, null);
    }

    public boolean update(TableEntry tableEntry, Map<String, Object> filterList, List<String> specificRows)
            throws SQLException, IllegalArgumentException, IllegalAccessException {
        if (!tableEntry.isLoadedEntry)
            return false;

        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = null;
        boolean returnResult;

        StringBuilder setBuilder = new StringBuilder();
        Map<Column, Object> columnList = tableEntry.getEntryColumns();

        Iterator<Column> setIterator = columnList.keySet().iterator();
        while (setIterator.hasNext()) {
            Column column = setIterator.next();
            if (column.columnType() == ColumnType.Primary)
                continue;
            if (specificRows != null && !specificRows.isEmpty() && !specificRows.contains(column.columnName()))
                continue;

            if (setBuilder.length() > 0) {
                setBuilder.append(",  " + column.columnName() + " = ?");
            } else {
                setBuilder.append(column.columnName() + " = ?");
            }
        }

        int index = 1;

        if (filterList != null && filterList.size() > 0) {
            StringBuilder whereBuilder = new StringBuilder();

            Iterator<String> whereIterator = filterList.keySet().iterator();
            while (whereIterator.hasNext()) {
                String columnName = whereIterator.next();

                Column column = tableEntry.getColumn(columnName);
                if (column == null)
                    throw new NoTableColumnException(columnName, tableEntry.getClass().getName());

                if (whereBuilder.length() > 0) {
                    whereBuilder.append(" AND " + columnName);
                } else {
                    whereBuilder.append(columnName);
                }

                whereBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement("UPDATE " + tableEntry.getTableName() + " SET "
                    + setBuilder.toString() + " WHERE " + whereBuilder.toString() + ";");

            setIterator = columnList.keySet().iterator();
            while (setIterator.hasNext()) {
                Column column = setIterator.next();
                if (column.columnType() == ColumnType.Primary)
                    continue;
                Object columnValue = columnList.get(column);

                this.setStatement(index++, preparedStatement, columnValue, column);
            }

            whereIterator = filterList.keySet().iterator();
            while (whereIterator.hasNext()) {
                String columnName = whereIterator.next();

                Column column = tableEntry.getColumn(columnName);
                Object columnValue = tableEntry.getColumn(column);

                this.setStatement(index++, preparedStatement, columnValue, column);
            }
        } else {
            preparedStatement = connection
                    .prepareStatement("UPDATE " + tableEntry.getTableName() + " SET " + setBuilder.toString() + ";");

            setIterator = columnList.keySet().iterator();
            while (setIterator.hasNext()) {
                Column column = setIterator.next();
                if (column.columnType() == ColumnType.Primary)
                    continue;
                Object columnValue = columnList.get(column);

                this.setStatement(index++, preparedStatement, columnValue, column);
            }
        }

        returnResult = preparedStatement.executeUpdate() > 0;
        this.closeConnection(connection, preparedStatement, null);

        return returnResult;
    }

    public boolean update(TableEntry tableEntry, Map<String, Object> filterList, Map<String, Object> specificRows)
            throws Exception {
        if (specificRows == null || specificRows.isEmpty())
            throw new Exception("The specificRows argument can't be null");

        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = null;
        boolean returnResult;

        StringBuilder setBuilder = new StringBuilder();

        Iterator<String> setIterator = specificRows.keySet().iterator();
        while (setIterator.hasNext()) {
            String columnName = setIterator.next();
            Column column = tableEntry.getColumn(columnName);
            if (column == null)
                throw new NoTableColumnException(columnName, tableEntry.getClass().getName());

            if (column.columnType() == ColumnType.Primary)
                continue;

            if (setBuilder.length() > 0) {
                setBuilder.append(",  " + column.columnName() + " = ?");
            } else {
                setBuilder.append(column.columnName() + " = ?");
            }
        }

        int index = 1;

        if (filterList != null && filterList.size() > 0) {
            StringBuilder whereBuilder = new StringBuilder();

            Iterator<String> whereIterator = filterList.keySet().iterator();
            while (whereIterator.hasNext()) {
                String columnName = whereIterator.next();

                Column column = tableEntry.getColumn(columnName);
                if (column == null)
                    throw new NoTableColumnException(columnName, tableEntry.getClass().getName());

                if (whereBuilder.length() > 0) {
                    whereBuilder.append(" AND " + columnName);
                } else {
                    whereBuilder.append(columnName);
                }

                whereBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement("UPDATE " + tableEntry.getTableName() + " SET "
                    + setBuilder.toString() + " WHERE " + whereBuilder.toString() + ";");

            setIterator = specificRows.keySet().iterator();
            while (setIterator.hasNext()) {
                String columnName = setIterator.next();
                Column column = tableEntry.getColumn(columnName);

                if (column.columnType() == ColumnType.Primary)
                    continue;
                this.setStatement(index++, preparedStatement, specificRows.get(columnName), column);
            }

            whereIterator = filterList.keySet().iterator();
            while (whereIterator.hasNext()) {
                String columnName = whereIterator.next();

                Column column = tableEntry.getColumn(columnName);
                Object columnValue = tableEntry.getColumn(column);

                this.setStatement(index++, preparedStatement, columnValue, column);
            }
        } else {
            preparedStatement = connection
                    .prepareStatement("UPDATE " + tableEntry.getTableName() + " SET " + setBuilder.toString() + ";");

            setIterator = specificRows.keySet().iterator();
            while (setIterator.hasNext()) {
                String columnName = setIterator.next();
                Column column = tableEntry.getColumn(columnName);

                if (column.columnType() == ColumnType.Primary)
                    continue;
                this.setStatement(index++, preparedStatement, specificRows.get(columnName), column);
            }
        }

        returnResult = preparedStatement.executeUpdate() > 0;
        this.closeConnection(connection, preparedStatement, null);

        return returnResult;
    }

    public boolean load(TableEntry tableEntry, Map<String, Object> filterList, LoadHelper loadHelper)
            throws SQLException, IllegalArgumentException, IllegalAccessException {
        if (tableEntry.isLoadedEntry)
            return false;
        if (filterList == null)
            throw new NullPointerException("The FilterList is NULL");
        if (filterList.size() <= 0)
            throw new EmptyFilterException("filterList");

        // Alter LoadHelper
        if (loadHelper == null)
            loadHelper = new LoadHelper();
        loadHelper.limit(1).offset(0);

        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = this.prepareSelect(connection, tableEntry, filterList, loadHelper);
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet == null)
            return false;
        resultSet.next(); // Now getAbstractDatabase the ONLY ONE result

        Map<String, Column> tableLayout = tableEntry.getTableLayout();
        Iterator<Column> columnIterator = tableLayout.values().iterator();

        while (columnIterator.hasNext()) {
            Column column = columnIterator.next();
            if (column.entryType() != EntryType.Normal) {
                if (column.entryType() == EntryType.Map) {
                    tableEntry.setColumn(column, this.getMap(column.dataType(), column.entryDataType(),
                            resultSet.getString(column.columnName())));
                } else {
                    tableEntry.setColumn(column,
                            this.getList(column.dataType(), resultSet.getString(column.columnName())));
                }
            } else {
                if (column.autoIncrement() || column.columnType() == ColumnType.Primary) {
                    tableEntry.setColumn(column, resultSet.getInt(column.columnName()));
                } else {
                    tableEntry.setColumn(column, this.getResult(resultSet, column));
                }
            }
        }

        tableEntry.isLoadedEntry = true;
        tableEntry.inform();

        this.closeConnection(connection, preparedStatement, resultSet);
        return true;
    }

    public void load(TableEntry tableEntry, List<TableEntry> resultList, Map<String, Object> filterList,
            LoadHelper loadHelper) throws IllegalArgumentException, IllegalAccessException, SQLException {
        if (resultList == null)
            throw new NullPointerException("The resultList is NULL");
        resultList.clear(); // Clear the List - when not empty... not my problem

        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = this.prepareSelect(connection, tableEntry, filterList, loadHelper);
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet == null)
            return;

        List<Column> tableLayout = new ArrayList<Column>(tableEntry.getTableLayout().values());
        while (resultSet.next()) {
            TableEntry newInstance = tableEntry.getInstance();

            for (Column column : tableLayout) {
                if (column.autoIncrement() || column.columnType() == ColumnType.Primary) {
                    newInstance.setColumn(column, resultSet.getInt(column.columnName()));
                } else {
                    newInstance.setColumn(column, this.getResult(resultSet, column));
                }
            }

            newInstance.isLoadedEntry = true;
            newInstance.inform();

            resultList.add(newInstance);
        }

        this.closeConnection(connection, preparedStatement, resultSet);
    }

    protected PreparedStatement prepareSelect(Connection connection, TableEntry tableEntry,
            Map<String, Object> filterList, LoadHelper loadHelper) throws SQLException {
        StringBuilder getBuilder = new StringBuilder();
        StringBuilder whereBuilder = new StringBuilder();

        Map<String, Column> tableLayout = tableEntry.getTableLayout();

        Iterator<String> layoutIterator = tableLayout.keySet().iterator();
        while (layoutIterator.hasNext()) {
            if (getBuilder.length() > 0) {
                getBuilder.append(", " + layoutIterator.next());
            } else {
                getBuilder.append(layoutIterator.next());
            }
        }

        if (filterList != null && filterList.size() > 0) {
            Iterator<String> filterIterator = filterList.keySet().iterator();
            while (filterIterator.hasNext()) {
                if (whereBuilder.length() > 0) {
                    whereBuilder.append(" AND " + filterIterator.next());
                } else {
                    whereBuilder.append(filterIterator.next());
                }

                whereBuilder.append(" = ?");
            }
        }

        String helper = "";
        if (loadHelper != null) {
            if (loadHelper.columnSorter.size() > 0) {
                helper = " ORDER BY ";

                StringBuilder stringBuilder = new StringBuilder();
                for (String field : loadHelper.columnSorter.keySet()) {
                    stringBuilder.append(field + " " + loadHelper.columnSorter.get(field) + ", ");
                }

                stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());

                helper = helper + stringBuilder.toString();
            }

            if (loadHelper.limit > 0)
                helper = helper + " LIMIT " + loadHelper.limit;
            if (loadHelper.offset > 0)
                helper = helper + " OFFSET " + loadHelper.offset;
        }

        PreparedStatement preparedStatement = connection
                .prepareStatement("SELECT " + getBuilder.toString() + " FROM " + tableEntry.getTableName()
                        + (whereBuilder.length() > 0 ? " WHERE " + whereBuilder.toString() : "") + helper + ";");

        if (filterList != null && filterList.size() > 0) {
            int index = 1;

            Iterator<String> filterIterator = filterList.keySet().iterator();
            while (filterIterator.hasNext()) {
                String columnName = filterIterator.next();

                Column column = tableEntry.getColumn(columnName);
                if (column == null)
                    throw new NoTableColumnException(columnName, tableEntry.getClass().getName());
                Object columnValue = filterList.get(columnName);

                this.setStatement(index++, preparedStatement, columnValue, column);
            }
        }

        // Execute in the correct method not here
        return preparedStatement;
    }

    public boolean exist(TableEntry tableEntry) throws SQLException, IllegalArgumentException, IllegalAccessException {
        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        boolean returnResult;

        if (tableEntry.hasPrimaryKey()) { // Simple and faster :)
            Column column = tableEntry.getPrimaryKey();
            Object primaryKey = tableEntry.getColumn(column);

            preparedStatement = connection.prepareStatement(
                    "SELECT * FROM " + tableEntry.getTableName() + " WHERE " + column.columnName() + " = ?;");
            this.setStatement(1, preparedStatement, primaryKey, column);

            resultSet = preparedStatement.executeQuery();
            returnResult = resultSet.next();
        } else { // Needs to check more - so a little bit slower
            Map<Column, Object> columnList = tableEntry.getEntryColumns();
            Iterator<Column> iterator = columnList.keySet().iterator();

            StringBuilder stringBuilder = new StringBuilder();

            while (iterator.hasNext()) {
                Column column = iterator.next();
                if (columnList.get(column) == null)
                    continue; // Can't check NULL variables

                if (stringBuilder.length() > 0) {
                    stringBuilder.append(" AND " + column.columnName());
                } else {
                    stringBuilder.append(column.columnName());
                }

                stringBuilder.append(" = ?");
            }

            preparedStatement = connection.prepareStatement(
                    "SELECT * FROM " + tableEntry.getTableName() + " WHERE " + stringBuilder.toString() + ";");
            iterator = columnList.keySet().iterator(); // New iterator cause we cant start over again

            int index = 1;
            while (iterator.hasNext()) {
                Column column = iterator.next();
                Object value = columnList.get(column);
                if (value == null)
                    continue;

                this.setStatement(index++, preparedStatement, value, column);
            }

            resultSet = preparedStatement.executeQuery();
            returnResult = resultSet.next();
        }

        this.closeConnection(connection, preparedStatement, resultSet);
        return returnResult;
    }

    public void updateLayout(TableEntry tableEntry) throws SQLException {
        if (!DatabaseHandler.getInstance().getAbstractDatabase().tableExist(tableEntry.getTableName())) {
            this.addTable(tableEntry);
            return;
        }

        String tableName = tableEntry.getTableName();

        List<String> removeList = new ArrayList<String>();
        Map<String, Column> addList = tableEntry.getTableLayout();
        List<String> existingList = this.getTableColumns(tableEntry.getTableName());

        for (String columnName : existingList) {
            if (!addList.containsKey(columnName)) {
                removeList.add(columnName);
            } else {
                addList.remove(columnName);
            }
        }

        List<Column> correctAddList = new ArrayList<Column>(addList.values());
        correctAddList.sort(new TableEntry.ColumnClassSorter());

        // Add and remove the columns
        for (String columnName : removeList)
            this.delColumn(tableName, columnName);
        for (Column addColumn : correctAddList)
            this.addColumn(tableEntry, addColumn);
    }

    protected void addTable(TableEntry tableEntry) throws SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();
        PreparedStatement preparedStatement = null;

        StringBuilder columnBuilder = new StringBuilder();
        List<Column> columnList = tableEntry.getPlainLayout();
        Iterator<Column> iterator = columnList.iterator();

        while (iterator.hasNext()) {
            Column column = iterator.next();

            columnBuilder.append(column.columnName() + " ");
            columnBuilder.append(column.autoIncrement() || column.columnType() == ColumnType.Primary ? "BIGSERIAL"
                    : this.toDatabaseType(
                            column.entryType() != EntryType.Normal ? DataType.String : column.dataType()));
            if (column.columnType() != ColumnType.Normal)
                columnBuilder.append(column.columnType() == ColumnType.Primary ? " PRIMARY KEY" : " UNIQUE");

            if (iterator.hasNext())
                columnBuilder.append(", ");
        }

        preparedStatement = connection
                .prepareStatement("CREATE TABLE " + tableEntry.getTableName() + " (" + columnBuilder.toString() + ");");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    protected List<String> getTableColumns(String tableName) throws SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();

        ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null);
        List<String> columnList = new ArrayList<String>();

        while (resultSet.next())
            columnList.add(resultSet.getString("COLUMN_NAME"));

        this.closeConnection(connection, null, resultSet);
        return columnList;
    }

    protected void addColumn(TableEntry tableEntry, Column column) throws SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();
        DataType dataType = column.entryType() != EntryType.Normal ? DataType.String : column.dataType();

        // String before = column.order() != -1 ? " BEFORE " +
        // tableEntry.getColumn(column.order() - 1).columnName() : "";
        PreparedStatement preparedStatement = connection.prepareStatement("ALTER TABLE " + tableEntry.getTableName()
                + " ADD " + column.columnName() + " " + this.toDatabaseType(dataType)
                + (column.columnType() == ColumnType.Unique ? " UNIQUE" : "") + ";");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    protected void delColumn(String tableName, String columnName) throws SQLException {
        Connection connection = DatabaseHandler.getInstance().getConnection();

        PreparedStatement preparedStatement = connection
                .prepareStatement("ALTER TABLE " + tableName + " DROP " + columnName + ";");
        preparedStatement.execute();

        this.closeConnection(connection, preparedStatement, null);
    }

    protected String toDatabaseType(DataType dataType) {
        switch (dataType) {
        case Boolean:
            return "BOOLEAN";
        case Byte:
            return "INTEGER";
        case Double:
            return "DOUBLE PRECISION";
        case Float:
            return "DOUBLE PRECISION";
        case Integer:
            return "INTEGER";
        case Long:
            return "BIGINT";
        case Short:
            return "INTEGER";
        case String:
            return "TEXT";
        case Timestamp:
            return "TIMESTAMP WITHOUT TIME ZONE"; // timestamp without time zone
        case Binary:
            return "BYTEA";
        case UniqueId:
            return "TEXT";
        }

        return null;
    }
}