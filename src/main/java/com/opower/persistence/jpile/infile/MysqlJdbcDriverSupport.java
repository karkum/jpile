package com.opower.persistence.jpile.infile;

import java.io.InputStream;
import java.sql.Statement;

/**
 * To be used when connections are of type {@code com.mysql.jdbc.Statement}
 *
 * @author amir.raminfar
 */
public class MysqlJdbcDriverSupport implements InfileStatementCallback.JdbcDriverSupport {

    @Override
    public boolean accept(Statement statement) {
        // Want to use instanceof because only MySQL supports the infile right now and if MySQL jar file is not in classpath
        // ClassNotFoundException should be thrown
        return statement instanceof com.mysql.jdbc.Statement;
    }

    @Override
    public void doWithStatement(Statement statement, InputStream inputStream) {
        com.mysql.jdbc.Statement mysqlStatement = (com.mysql.jdbc.Statement) statement;
        mysqlStatement.setLocalInfileInputStream(inputStream);
    }
}
