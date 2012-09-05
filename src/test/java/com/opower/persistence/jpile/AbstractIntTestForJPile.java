package com.opower.persistence.jpile;

import java.sql.Connection;
import java.sql.DriverManager;
import com.google.common.base.Throwables;
import com.opower.persistence.jpile.loader.HierarchicalInfileObjectLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Abstract test case for all int tests. Loads MySQL drivers and creates a new MySQL {@link Connection}
 *
 * @author amir.raminfar
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public abstract class AbstractIntTestForJPile {
    private static final String JDBC_URL = "jdbc:mysql://localhost/jpile?useUnicode=true&characterEncoding=utf-8";
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    protected Connection connection;
    protected HierarchicalInfileObjectLoader hierarchicalInfileObjectLoader = new HierarchicalInfileObjectLoader();
    protected SimpleJdbcTemplate simpleJdbcTemplate;

    @Before
    public void setUp() throws Exception {
        connection = DriverManager.getConnection(JDBC_URL, "root", "");
        hierarchicalInfileObjectLoader.setConnection(connection);
        simpleJdbcTemplate = new SimpleJdbcTemplate(new SingleConnectionDataSource(connection, true));
    }

    @After
    public void tearDown() throws Exception {
        hierarchicalInfileObjectLoader.close();
        simpleJdbcTemplate.update("truncate customer");
        simpleJdbcTemplate.update("truncate product");
        simpleJdbcTemplate.update("truncate contact");
        simpleJdbcTemplate.update("truncate contact_phone");
        simpleJdbcTemplate.update("truncate binary_data");
        connection.close();
    }
}
