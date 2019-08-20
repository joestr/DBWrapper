package xyz.joestr.dbwrapper;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import xyz.joestr.dbwrapper.annotations.WrappedField;
import xyz.joestr.dbwrapper.annotations.WrappedTable;

/**
 * Wraps around a given class, which implements
 * {@link xyz.joestr.dbwrapper.DatabaseWrapable}.
 *
 * @param <T> Class which is annotated by
 * {@link xyz.joestr.dbwrapper.annotations.WrappedTable} and at least one field
 * is annotated by {@link xyz.joestr.dbwrapper.annotations.WrappedField}.
 * @author Joel Strasser (joestr)
 * @version ${project.version}
 */
public class DatabaseAnnotationWrapper<T> {

    // The connection to the database
    private final DatabaseConnectionHandler databaseConnectionHandler;
    private final Class<T> clazz;
    private String tableName;
    private final Collection<String> columnNames;
    private final Collection<String> fieldNames;

    /**
     * Creates a new instance of the
     * {@link xyz.joestr.dbwrapper.DatabaseWrapper}.
     *
     * @param databaseConnectionHandler The
     * {@link xyz.joestr.dbwrapper.DatabaseConnectionHandler}
     * @param clazz The {@link java.lang.Class}
     * @throws InstantiationException If the instantiation fails
     * @throws IllegalAccessException If the access is prohibited
     */
    public DatabaseAnnotationWrapper(Class<T> clazz, DatabaseConnectionHandler databaseConnectionHandler) throws InstantiationException, IllegalAccessException {

        this.databaseConnectionHandler = databaseConnectionHandler;
        this.clazz = clazz;
        this.tableName = "";
        this.columnNames = new ArrayList<>();
        this.fieldNames = new ArrayList<>();
        this.resolveDatabaseTableName();
        this.resolveDatabaseColumnNamesAndClassFieldNames();
    }

    /**
     * Selects all entries in the table and bundles them in a
     * {@link java.util.Collection}.
     *
     * @return A {@link java.util.Collection} of {@link java.lang.reflect.Type}
     * @throws SQLException If something SQL relevant fails
     * @throws InstantiationException If the instantiation is prohibited
     * @throws IllegalAccessException If the access is prohibited
     * @throws NoSuchFieldException If the field does not exist
     */
    public Collection<T> select() throws SQLException, InstantiationException, IllegalAccessException, NoSuchFieldException {

        Collection<T> result = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("SELECT ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining(", ")));
        stringBuilder.append(" FROM ");
        stringBuilder.append(this.tableName);

        this.databaseConnectionHandler.connect();

        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        ResultSet resultSet;
        resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {

            T resultObject = this.clazz.newInstance();

            for (int i = 0; i < this.columnNames.size(); i++) {

                Field field_ = this.clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

                field_.setAccessible(true);

                field_.set(resultObject, resultSet.getObject(i + 1, field_.getType()));
            }

            result.add(resultObject);
        }

        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Selects all entries in the table witch match a given condition and
     * bundles them in a {@link java.util.Collection}. This method does not
     * prevent SQL injections!
     *
     * @param condition The condition as a {@link java.lang.String}
     * @return A {@link java.util.Collection} of {@link java.lang.reflect.Type}
     * @throws NullPointerException If condition is null
     * @throws SQLException If something SQL relevant fails
     * @throws InstantiationException If the instantiation is prohibited
     * @throws IllegalAccessException If the access is prohibited
     * @throws NoSuchFieldException If the field does not exist
     */
    public Collection<T> select(String condition) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, NoSuchFieldException {

        if (condition == null) {
            throw new NullPointerException("condition can not be null!");
        }

        Collection<T> result = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("SELECT ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining(", ")));
        stringBuilder.append(" FROM ");
        stringBuilder.append(this.tableName);
        stringBuilder.append(" WHERE ");
        stringBuilder.append(condition);

        this.databaseConnectionHandler.connect();

        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        ResultSet resultSet;
        resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {

            T resultObject = this.clazz.newInstance();

            for (int i = 0; i < this.columnNames.size(); i++) {

                Field field_ = this.clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

                field_.setAccessible(true);

                field_.set(resultObject, resultSet.getObject(i + 1, field_.getType()));
            }

            result.add(resultObject);
        }

        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Inserts given object in the table.
     *
     * @param object Object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     */
    public int insert(T object) throws SQLException, NoSuchFieldException, IllegalAccessException {

        int result = -1;

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("INSERT INTO ");
        stringBuilder.append(tableName);
        stringBuilder.append("(");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining(", ")));
        stringBuilder.append(") ");
        stringBuilder.append("VALUES(");

        for (int i = 0; i < this.columnNames.size(); i++) {

            if (i < this.columnNames.size() - 1) {

                stringBuilder.append("?, ");
            } else {

                stringBuilder.append("?");
            }
        }

        stringBuilder.append(")");

        this.databaseConnectionHandler.connect();

        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        for (int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i + 1, field_.getType().cast(field_.get(object)));
        }

        result = preparedStatement.executeUpdate();

        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Updates an {@code oldObject} with an {@code newObject}.
     *
     * @param oldObject Old object of {@link java.lang.reflect.Type}
     * @param newObject New object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     */
    public int update(T oldObject, T newObject) throws SQLException, NoSuchFieldException, IllegalAccessException {

        int result = -1;

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("UPDATE ");
        stringBuilder.append(tableName);
        stringBuilder.append(" SET ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=?, ", "", "=?")));
        stringBuilder.append(" WHERE ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=? AND ", "", "=?")));

        this.databaseConnectionHandler.connect();

        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        for (int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i + 1, field_.getType().cast(field_.get(newObject)));
        }

        for (int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i + 1 + this.columnNames.size(), field_.getType().cast(field_.get(oldObject)));
        }

        result = preparedStatement.executeUpdate();

        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Deletes an {@code object} from the table.
     *
     * @param object Object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     */
    public int delete(T object) throws SQLException, NoSuchFieldException, IllegalAccessException {

        int result = -1;

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("DELETE FROM ");
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=? AND ", "", "=?")));

        this.databaseConnectionHandler.connect();

        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        for (int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>) this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i + 1, field_.getType().cast(field_.get(object)));
        }

        result = preparedStatement.executeUpdate();

        this.databaseConnectionHandler.disconnect();

        return result;
    }

    /**
     * Resolves the annotated class for the table name.
     */
    private void resolveDatabaseTableName() {

        if (this.clazz.isAnnotationPresent(WrappedTable.class)) {
            WrappedTable databaseTable
                = this.clazz.getAnnotation(WrappedTable.class);

            this.tableName = databaseTable.name();
        }
    }

    /**
     * Resolves the annotated field(s) for the column names and the field names.
     */
    private void resolveDatabaseColumnNamesAndClassFieldNames() {

        for (Field f : this.clazz.getFields()) {

            f.setAccessible(true);
            if (f.isAnnotationPresent(WrappedField.class)) {
                WrappedField databaseField
                    = f.getAnnotation(WrappedField.class);

                this.columnNames.add(
                    databaseField.name()
                );

                this.fieldNames.add(
                    f.getName()
                );
            }
        }
    }
}
