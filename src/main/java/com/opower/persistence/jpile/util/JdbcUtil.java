package com.opower.persistence.jpile.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import com.google.common.base.Throwables;

/**
 * A jdbc helper util class. This class was created so that Spring JDBC is no longer a dependency.
 *
 * @author amir.raminfar
 */
public final class JdbcUtil {
    private JdbcUtil() {
    }

    /**
     * Creates a new statement from connection and closes the statement properly after
     *
     * @param connection        the connection to use
     * @param statementCallback the callback
     * @param <E>               the return type
     * @return the return value from callback
     */
    public static <E> E execute(Connection connection, StatementCallback<E> statementCallback) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            return statementCallback.doInStatement(statement);
        }
        catch(SQLException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if(statement != null) {
                try {
                    statement.close();
                }
                catch(SQLException e) {
                    // Do nothing
                }
            }
        }
    }

    /**
     * A helper callback method for this util class
     *
     * @param <E>
     */
    public interface StatementCallback<E> {
        /**
         * @param statement the statement to use
         * @return return value for this callback
         * @throws SQLException if a sql error happens
         */
        E doInStatement(Statement statement) throws SQLException;
    }
}
