package com.protean.tecs.jdbc;


import com.mysql.cj.jdbc.NonRegisteringDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ServiceLoader;

public class ProteanTecsJdbcDriver extends ProteanTecsNonRegisteringJdbcDriver {

    public ProteanTecsJdbcDriver() throws SQLException {
        // Required for Class.forName().newInstance()
    }

    static {
        try {
            DriverManager.registerDriver(new ProteanTecsJdbcDriver());
            //DriverManager.registerDriver(new NonRegisteringDriver());
            //ServiceLoader.load(com.mysql.cj.jdbc.Driver.class);
            //ServiceLoader.load(com.vertica.jdbc.Driver.class);
            //ClassLoader.getSystemClassLoader().loadClass("com.vertica.jdbc.Driver");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
