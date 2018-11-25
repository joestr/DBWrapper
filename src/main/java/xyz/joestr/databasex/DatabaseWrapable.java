/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xyz.joestr.databasex;

import java.util.Collection;

/**
 * Classes which implement this interface can be used by the
 * {@link xyz.joestr.databasex.DatabaseWrapper}.
 * @author Joel Strasser (joestr)
 * @version 0.1.0
 * @since 0.1.0
 */
public interface DatabaseWrapable {
    
    /**
     * Defines the name of the table in the database.
     * @return {@link java.lang.String} Name of the table
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public String databaseTableName();
    
    /**
     * Defines the name of the columns in the database.
     * @return {@link java.util.Collection}&lt;{@link java.lang.String}&gt; Name of the columns in the database
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public Collection<String> databaseColumnNames();
    
    /**
     * Defines the name of the fields in the implementing class.
     * @return {@link java.util.Collection}&lt;{@link java.lang.String}&gt; Name of the fields in the implementing class
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public Collection<String> classFieldNames();
    
    /**
     * Defines the class of the fields in the implementing class.
     * @return {@link java.util.Collection}&lt;{@link java.lang.Class}&gt; Class of the fields in the implementing class
     * @author Joel Strasser (joestr)
     * @version 0.1.0
     * @since 0.1.0
     */
    public Collection<Class> classFieldClasses();
}
