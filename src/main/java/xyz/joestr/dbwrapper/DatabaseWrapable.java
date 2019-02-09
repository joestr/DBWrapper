/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xyz.joestr.dbwrapper;

import java.util.Collection;

/**
 * Classes which implement this interface can be used by the
 * {@link xyz.joestr.dbwrapper.DatabaseWrapper}.
 *
 * @author Joel Strasser (joestr)
 * @version ${project.version}
 */
public interface DatabaseWrapable {

    /**
     * Defines the name of the table in the database.
     *
     * @return {@link java.lang.String} Name of the table
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public String databaseTableName();

    /**
     * Defines the name of the columns in the database.
     *
     * @return {@link java.util.Collection}&lt;{@link java.lang.String}&gt; Name
     * of the columns in the database
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public Collection<String> databaseColumnNames();

    /**
     * Defines the name of the fields in the implementing class.
     *
     * @return {@link java.util.Collection}&lt;{@link java.lang.String}&gt; Name
     * of the fields in the implementing class
     * @author Joel Strasser (joestr)
     * @version ${project.version}
     */
    public Collection<String> classFieldNames();
}
