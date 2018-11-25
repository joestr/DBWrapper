package xyz.joestr.databasex;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Wraps around a given class, which implements {@link xyz.joestr.databasex.DatabaseWrapable}.
 * @author Joel Strasser (joestr)
 * @version 0.1.0
 * @since 0.1.0
 * @param <T> Class which implements {@link xyz.joestr.databasex.DatabaseWrapable}
 */
public class DatabaseWrapper<T extends DatabaseWrapable> {
    
    // The connection to the database
    private final DatabaseConnectionHandler databaseConnectionHandler;
    private final Class<T> clazz;
    private final String tableName;
    private final Collection<String> columnNames;
    private final Collection<String> fieldNames;
    private final Collection<Class> fieldClasses;
    
    /**
     * Creates a new instance of the {@link xyz.joestr.databasex.DatabaseWrapper}.
     * @param databaseConnectionHandler The {@link xyz.joestr.databasex.DatabaseConnectionHandler}
     * @param clazz The {@link java.lang.Class}
     * @throws InstantiationException If the instantiation fails
     * @throws IllegalAccessException If the access is prohibited
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public DatabaseWrapper(Class<T> clazz, DatabaseConnectionHandler databaseConnectionHandler) throws InstantiationException, IllegalAccessException {
        
        this.databaseConnectionHandler = databaseConnectionHandler;
        this.clazz = clazz;
        this.tableName = this.clazz.newInstance().databaseTableName();
        this.columnNames = this.clazz.newInstance().databaseColumnNames();
        this.fieldNames = this.clazz.newInstance().classFieldNames();
        this.fieldClasses = this.clazz.newInstance().classFieldClasses();
    }
    
    /**
     * Selects all entries in the table and bundles them in a {@link java.util.Collection}.
     * @return A {@link java.util.Collection} of {@link java.lang.reflect.Type}
     * @throws SQLException If something SQL relevant fails
     * @throws InstantiationException If the instantiation is prohibited
     * @throws IllegalAccessException If the access is prohibited
     * @throws NoSuchFieldException If the field does not exist
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
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
        
        while(resultSet.next()) {
            
            T resultObject = this.clazz.newInstance();

            for(int i = 0; i < this.columnNames.size(); i++) {

                Field field_ = this.clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

                field_.setAccessible(true);

                field_.set(resultObject, resultSet.getObject(i+1, ((ArrayList<Class>)this.fieldClasses).get(i)));
            }
            
            result.add(resultObject);
        }
        
        this.databaseConnectionHandler.disconnect();
        
        return result;
    }
    
    /**
     * Selects all entries in the table witch match a given condition and bundles them in a {@link java.util.Collection}.
     * This method does not prevent SQL injections!
     * @param condition The condition as a {@link java.lang.String}
     * @return A {@link java.util.Collection} of {@link java.lang.reflect.Type}
     * @throws NullPointerException If condition is null
     * @throws SQLException If something SQL relevant fails
     * @throws InstantiationException If the instantiation is prohibited
     * @throws IllegalAccessException If the access is prohibited
     * @throws NoSuchFieldException If the field does not exist
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.1
     */
    public Collection<T> select(String condition) throws NullPointerException, SQLException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        
        if(condition == null) throw new NullPointerException("condition can not be null!");
        
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
        
        while(resultSet.next()) {
            
            T resultObject = this.clazz.newInstance();

            for(int i = 0; i < this.columnNames.size(); i++) {

                Field field_ = this.clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

                field_.setAccessible(true);

                field_.set(resultObject, resultSet.getObject(i+1, ((ArrayList<Class>)this.fieldClasses).get(i)));
            }
            
            result.add(resultObject);
        }
        
        this.databaseConnectionHandler.disconnect();
        
        return result;
    }
    
    /**
     * Inserts given object in the table.
     * @param object Object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
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
        
        for(int i = 0; i < this.columnNames.size(); i++) {
            
            if(i < this.columnNames.size() - 1) {
                
                stringBuilder.append("?, ");
            } else {
                
                stringBuilder.append("?");
            }
        }
        
        stringBuilder.append(")");
        
        this.databaseConnectionHandler.connect();
        
        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        for(int i = 0; i < this.columnNames.size(); i++) {
            
            Field field_ = clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i+1, ((ArrayList<Class>)this.fieldClasses).get(i).cast(field_.get(object)));
        }
        
        result = preparedStatement.executeUpdate();
        
        this.databaseConnectionHandler.disconnect();
        
        return result;
    }
    
    /**
     * Updates an {@code oldObject} with an {@code newObject}.
     * @param oldObject Old object of {@link java.lang.reflect.Type}
     * @param newObject New object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public int update(T oldObject, T newObject) throws SQLException, NoSuchFieldException, IllegalAccessException {
        
        int result = -1;
        
        StringBuilder stringBuilder = new StringBuilder();
        
        stringBuilder.append("UPDATE ");
        stringBuilder.append(tableName);
        stringBuilder.append(" SET ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=?, ","","=?")));
        stringBuilder.append(" WHERE ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=? AND ","","=?")));
        
        this.databaseConnectionHandler.connect();
        
        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());

        for(int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i+1, ((ArrayList<Class>)this.fieldClasses).get(i).cast(field_.get(newObject)));
        }
        
        for(int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i+1+this.columnNames.size(), ((ArrayList<Class>)this.fieldClasses).get(i).cast(field_.get(oldObject)));
        }
        
        result = preparedStatement.executeUpdate();
        
        this.databaseConnectionHandler.disconnect();
        
        return result;
    }
    
    /**
     * Deletes an {@code object} from the table.
     * @param object Object of {@link java.lang.reflect.Type}
     * @return An {@link java.lang.Integer}
     * @throws SQLException If something SQL relevant fails
     * @throws NoSuchFieldException If the field does not exist
     * @throws IllegalAccessException If the access is prohibited
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public int delete(T object) throws SQLException, NoSuchFieldException, IllegalAccessException {
        
        int result = -1;
        
        StringBuilder stringBuilder = new StringBuilder();
        
        stringBuilder.append("DELETE FROM ");
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");
        stringBuilder.append(this.columnNames.stream().collect(Collectors.joining("=? AND ","","=?")));
        
        this.databaseConnectionHandler.connect();
        
        PreparedStatement preparedStatement;
        preparedStatement = this.databaseConnectionHandler.getConnection().prepareStatement(stringBuilder.toString());
        
        for(int i = 0; i < this.columnNames.size(); i++) {

            Field field_ = this.clazz.getDeclaredField(((ArrayList<String>)this.fieldNames).get(i));

            field_.setAccessible(true);

            preparedStatement.setObject(i+1, field_.getType().cast(field_.get(object)));
        }
        
        result = preparedStatement.executeUpdate();
        
        this.databaseConnectionHandler.disconnect();
        
        return result;
    }
}
