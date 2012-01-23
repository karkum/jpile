package com.opower.persistence.jpile.infile;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import com.google.common.base.Throwables;
import com.mchange.v2.c3p0.C3P0ProxyStatement;

/**
 * Support for C3P0 driver
 *
 * @author amir.raminfar
 * @see C3P0ProxyStatement#rawStatementOperation(java.lang.reflect.Method, Object, Object[])
 */
public class C3P0JdbcDriverSupport implements InfileStatementCallback.JdbcDriverSupport {
    // Infile method name for reflection lookup.
    // This is the MySQL driver dependency. We don't load the class, but this is the name
    // of the method on the MySQL statement that we invoke via reflection.
    private static final String INFILE_MUTATOR_METHOD = "setLocalInfileInputStream";

    private static Class targetInterface;

    static {
        try {
            // Use Class.forName because we might not have this driver in classpath
            targetInterface = Class.forName("com.mchange.v2.c3p0.C3P0ProxyStatement");
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
        try {
            Method m = com.mysql.jdbc.Statement.class.getMethod(INFILE_MUTATOR_METHOD, new Class[]{InputStream.class});
            C3P0ProxyStatement proxyStatement = (C3P0ProxyStatement) statement;
            proxyStatement.rawStatementOperation(m, C3P0ProxyStatement.RAW_STATEMENT, new Object[]{inputStream});
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
        catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        catch (InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }
}
