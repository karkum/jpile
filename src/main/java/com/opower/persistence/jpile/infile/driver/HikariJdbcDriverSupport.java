package com.opower.persistence.jpile.infile.driver;

import com.opower.persistence.jpile.infile.InfileStatementCallback;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Support for HikariCP's statement proxy.
 *
 * @author aldenquimby@gmail.com
 * @see <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a>
 */
public class HikariJdbcDriverSupport implements InfileStatementCallback.JdbcDriverSupport {
    private static Class<?> targetInterface;

    static {
        try {
            targetInterface = Class.forName("com.zaxxer.hikari.proxy.StatementProxy");
        }
        catch (ClassNotFoundException e) {
            targetInterface = null;
        }
    }

    @Override
    public boolean accept(Statement statement) {
        return targetInterface != null && targetInterface.isInstance(statement);
    }

    @Override
    public void doWithStatement(Statement statement, InputStream inputStream) throws SQLException {
        statement.unwrap(com.mysql.jdbc.Statement.class).setLocalInfileInputStream(inputStream);
    }
}
