/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package xyz.joestr.dbwrapper.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import xyz.joestr.dbwrapper.DatabaseConnectionHandler;

/**
 *
 * @author Joel
 */
public class TestPersistence {

    @Test
    public void notAPersitentConnectionByDefault() {
        DatabaseConnectionHandler dCH = new DatabaseConnectionHandler("");

        assertEquals(false, dCH.isPersistentConnection());
    }
}
