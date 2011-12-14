package com.opower.persistence.jpile.loader;

import java.io.InputStreamReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.opower.persistence.jpile.infile.InfileDataBuffer;
import com.opower.persistence.jpile.infile.InfileStatementCallback;
import com.opower.persistence.jpile.reflection.CacheablePersistenceAnnotationInspector;
import com.opower.persistence.jpile.sample.Customer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;


/**
 * Tests the builder for Single Infile Object Loader
 *
 * @author amir.raminfar
 */
@RunWith(MockitoJUnitRunner.class)
public class SingleInfileObjectLoaderBuilderTest {
    @Mock
    JdbcTemplate jdbcTemplate;
    SingleInfileObjectLoader<Customer> objectLoader;

    @Before
    public void setUp() throws Exception {
        objectLoader = new SingleInfileObjectLoaderBuilder<Customer>(Customer.class)
                .withDefaultTableName()
                .withJdbcTemplate(jdbcTemplate)
                .usingHibernateBeanUtils(new CacheablePersistenceAnnotationInspector())
                .withBuffer(new InfileDataBuffer())
                .build();

    }

    @Test
    public void testBuildingCustomer() throws Exception {
        assertEquals(ImmutableSet.of("id", "last_seen_on"), objectLoader.getMappings().keySet());
        assertEquals(ImmutableMap.of(), objectLoader.getEmbeds());
        assertEquals(ImmutableList.of(), objectLoader.getWarnings());
        assertTrue(objectLoader.isAutoGenerateId());
    }

    @Test
    public void testAddingCustomer() throws Exception {
        Customer customer = new Customer();
        objectLoader.add(customer);
        assertNotNull(customer.getId());
        assertEquals("1\t\\N", CharStreams.toString(new InputStreamReader(objectLoader.getInfileDataBuffer().asInputStream())
        ));
    }

    @Test
    public void testFlush() throws Exception {
        Customer customer = new Customer();
        objectLoader.add(customer);
        objectLoader.flush();
        verify(jdbcTemplate).execute(any(InfileStatementCallback.class));
    }
}
