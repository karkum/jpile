package com.opower.persistence.jpile.infile.driver;

import com.mysql.jdbc.Statement;
import com.zaxxer.hikari.proxy.StatementProxy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

/**
 * @author alden@mark43.com
 * @since 12/22/14
 */
@RunWith(MockitoJUnitRunner.class)
public class HikariJdbcDriverSupportTest {
    private HikariJdbcDriverSupport hikariSupport = new HikariJdbcDriverSupport();

    @Mock
    private Statement mysqlStatement;
    @Mock
    private InputStream inputStream;

    @Test
    public void testAccept_hikariStatement_returnsTrue() throws Exception {
        assertTrue(hikariSupport.accept(new StubHikariStatement(null)));
    }

    @Test
    public void testAccept_mysqlStatement_returnsFalse() throws Exception {
        assertFalse(hikariSupport.accept(mysqlStatement));
    }

    @Test
    public void testDoWithStatement_unwrapsStatement_setsLocalInfileInputStream() throws Exception {
        hikariSupport.doWithStatement(new StubHikariStatement(mysqlStatement), inputStream);

        verify(mysqlStatement).setLocalInfileInputStream(inputStream);
    }

    /**
     * Need a stub here b/c {@link com.zaxxer.hikari.proxy.StatementProxy#delegate} is annoyingly final.
     */
    private class StubHikariStatement extends StatementProxy {

        protected StubHikariStatement(java.sql.Statement statement) {
            super(null, statement);
        }

        @Override
        public int getMaxFieldSize() throws SQLException {
            return 0;
        }

        @Override
        public void setMaxFieldSize(int max) throws SQLException {

        }

        @Override
        public int getMaxRows() throws SQLException {
            return 0;
        }

        @Override
        public void setMaxRows(int max) throws SQLException {

        }

        @Override
        public void setEscapeProcessing(boolean enable) throws SQLException {

        }

        @Override
        public int getQueryTimeout() throws SQLException {
            return 0;
        }

        @Override
        public void setQueryTimeout(int seconds) throws SQLException {

        }

        @Override
        public void cancel() throws SQLException {

        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public void setCursorName(String name) throws SQLException {

        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public int getUpdateCount() throws SQLException {
            return 0;
        }

        @Override
        public boolean getMoreResults() throws SQLException {
            return false;
        }

        @Override
        public void setFetchDirection(int direction) throws SQLException {

        }

        @Override
        public int getFetchDirection() throws SQLException {
            return 0;
        }

        @Override
        public void setFetchSize(int rows) throws SQLException {

        }

        @Override
        public int getFetchSize() throws SQLException {
            return 0;
        }

        @Override
        public int getResultSetConcurrency() throws SQLException {
            return 0;
        }

        @Override
        public int getResultSetType() throws SQLException {
            return 0;
        }

        @Override
        public void addBatch(String sql) throws SQLException {

        }

        @Override
        public void clearBatch() throws SQLException {

        }

        @Override
        public boolean getMoreResults(int current) throws SQLException {
            return false;
        }

        @Override
        public ResultSet getGeneratedKeys() throws SQLException {
            return null;
        }

        @Override
        public int getResultSetHoldability() throws SQLException {
            return 0;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public void setPoolable(boolean poolable) throws SQLException {

        }

        @Override
        public boolean isPoolable() throws SQLException {
            return false;
        }

        @Override
        public void closeOnCompletion() throws SQLException {

        }

        @Override
        public boolean isCloseOnCompletion() throws SQLException {
            return false;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}
