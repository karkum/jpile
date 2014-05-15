package com.opower.persistence.jpile.loader;

import com.google.common.collect.ImmutableSet;
import com.opower.persistence.jpile.AbstractIntTestForJPile;
import com.opower.persistence.jpile.sample.Contact;
import com.opower.persistence.jpile.sample.Customer;
import com.opower.persistence.jpile.sample.Data;
import com.opower.persistence.jpile.sample.ObjectFactory;
import com.opower.persistence.jpile.sample.Product;
import org.junit.Test;
import org.springframework.jdbc.core.RowMapper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests object loader for correctness
 *
 * @author amir.raminfar
 */
public class IntHierarchicalInfileObjectLoaderTest extends AbstractIntTestForJPile {
    @Test
    public void testSingleCustomer() throws Exception {
        // Note, this SimpleDateFormat does NOT match the DATE_TIME_FORMATTER in the InfileDataBuffer.  The database
        // being used with this test does not support milliseconds so we cannot assert with that granularity.  Others
        // using jPile with a later version of MySQL should be able to assert with the granularity of milliseconds.
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Customer expected = ObjectFactory.newCustomer();

        hierarchicalInfileObjectLoader.persist(expected);
        hierarchicalInfileObjectLoader.flush();
        Map<String, Object> customer = jdbcTemplate.queryForMap("select * from customer");
        Map<String, Object> contact = jdbcTemplate.queryForMap("select * from contact");
        Map<String, Object> phone = jdbcTemplate.queryForMap("select * from contact_phone");
        List<Map<String, Object>> products = jdbcTemplate.queryForList("select * from product");
        Map<String, Object> supplier = jdbcTemplate.queryForMap("select * from supplier");

        assertEquals(simpleDateFormat.format(expected.getLastSeenOn()), simpleDateFormat.format(customer.get("last_seen_on")));
        assertEquals(expected.getType().ordinal(), customer.get("type"));
        assertEquals(expected.getId(), customer.get("id"));
        assertEquals(expected.getId(), contact.get("customer_id"));
        assertEquals(expected.getContact().getFirstName(), contact.get("first_name"));
        assertEquals(expected.getContact().getLastName(), contact.get("last_name"));
        assertEquals(expected.getId(), phone.get("customer_id"));
        assertEquals(expected.getContact().getPhone(), phone.get("phone"));
        assertEquals(expected.getContact().getType().name(), contact.get("type"));
        assertEquals(expected.getContact().getAddress().getStreetNumber(), contact.get("street_number"));
        assertEquals(expected.getContact().getAddress().getStreet(), contact.get("street"));
        assertEquals(expected.getContact().getAddress().getCity(), contact.get("city"));
        assertEquals(expected.getContact().getAddress().getState(), contact.get("state"));
        assertEquals(expected.getContact().getAddress().getZipCode(), contact.get("zip_code"));
        assertEquals(expected.getProducts().size(), products.size());

        for (int i = 0, productsSize = expected.getProducts().size(); i < productsSize; i++) {
            Product expectedProduct = expected.getProducts().get(i);
            Map<String, Object> actualMap = products.get(i);
            assertEquals(expectedProduct.getId(), actualMap.get("id"));
            assertEquals(expected.getId().intValue(), actualMap.get("customer_id"));
            assertEquals(expectedProduct.getSupplier().getId().intValue(), actualMap.get("supplier_id"));
            assertEquals(expectedProduct.getTitle(), actualMap.get("title"));
            assertEquals(expectedProduct.getDescription(), actualMap.get("description"));
            assertEquals(expectedProduct.getPrice().doubleValue(), actualMap.get("price"));
            assertEquals(simpleDateFormat.format(expectedProduct.getPurchasedOn()),
                         simpleDateFormat.format(actualMap.get("purchased_on")));
            assertEquals(expectedProduct.getPackaging().ordinal(), actualMap.get("packaging"));
            assertEquals(expectedProduct.getSupplier().getAddress().getStreetNumber(), supplier.get("street_number"));
            assertEquals(expectedProduct.getSupplier().getAddress().getStreet(), supplier.get("street"));
            assertEquals(expectedProduct.getSupplier().getAddress().getCity(), supplier.get("city"));
            assertEquals(expectedProduct.getSupplier().getAddress().getState(), supplier.get("state"));
            assertEquals(expectedProduct.getSupplier().getAddress().getZipCode(), supplier.get("zip_code"));
        }
    }

    /**
     * Test that updating rows works. Persists two rows with the same primary key. Asserts that the second persist wins.
     * @throws Exception
     */
    @Test
    public void testMultipleCustomers() throws Exception {
        Customer customer1 = ObjectFactory.newCustomer();
        customer1.setId(1L);
        customer1.setType(Customer.Type.RESIDENTIAL);

        Customer customer2 = ObjectFactory.newCustomer();
        customer2.setId(1L);
        customer2.setType(Customer.Type.SMALL_BUSINESS);

        this.hierarchicalInfileObjectLoader.setUseReplace(true);
        this.hierarchicalInfileObjectLoader.persist(customer1);
        this.hierarchicalInfileObjectLoader.persist(customer2);

        this.hierarchicalInfileObjectLoader.flush();

        Map<String, Object> customers = this.jdbcTemplate.queryForMap("select * from customer");
        assertEquals(Customer.Type.SMALL_BUSINESS.ordinal(), customers.get("type"));
    }

    @Test
    public void testHundredCustomers() {
        for (int i = 0; i < 100; i++) {
            hierarchicalInfileObjectLoader.persist(ObjectFactory.newCustomer());
        }
    }

    @Test
    public void testBinaryDataToHex() throws NoSuchAlgorithmException {
        String string = "Data to be inserted";
        byte[] md5 = toMd5(string);
        Data data = new Data();
        data.setName(string);
        data.setMd5(md5);

        hierarchicalInfileObjectLoader.persist(data);
        hierarchicalInfileObjectLoader.flush();

        Data actual = jdbcTemplate.queryForObject("select * from binary_data", new RowMapper<Data>() {
            @Override
            public Data mapRow(ResultSet rs, int rowNum) throws SQLException {
                Data data = new Data();
                data.setId(rs.getLong("id"));
                data.setName(rs.getString("name"));
                data.setMd5(rs.getBytes("md5"));
                return data;
            }
        });

        assertTrue(Arrays.equals(md5, actual.getMd5()));
    }

    @Test
    public void testIgnore() {
        hierarchicalInfileObjectLoader.setClassesToIgnore(ImmutableSet.<Class>of(Customer.class));

        Customer customer = ObjectFactory.newCustomer();
        hierarchicalInfileObjectLoader.persist(customer);

        assertNull(customer.getId());
    }

    @Test
    public void testEventCallback() {
        HierarchicalInfileObjectLoader.CallBack callBack = mock(HierarchicalInfileObjectLoader.CallBack.class);
        hierarchicalInfileObjectLoader.setEventCallback(callBack);

        Customer customer = ObjectFactory.newCustomer();
        hierarchicalInfileObjectLoader.persist(customer);

        verify(callBack, times(1)).onBeforeSave(customer);
        verify(callBack, times(1)).onAfterSave(customer);
    }

    @Test
    public void testUtf8() {
        Contact expected = ObjectFactory.newContact();
        expected.setFirstName("\u304C\u3126");
        expected.setLastName("ががががㄦ");

        hierarchicalInfileObjectLoader.persist(expected);
        hierarchicalInfileObjectLoader.flush();

        Map<String, Object> actual = jdbcTemplate.queryForMap("select * from contact");

        assertEquals("がㄦ", actual.get("first_name"));
        assertEquals("ががががㄦ", actual.get("last_name"));
    }

    @Test
    public void testStuff() {
        Contact expected = ObjectFactory.newContact();
        expected.setFirstName("hello\t\t\tsomethingelse");
        expected.setLastName("last\t\t\tname");
        expected.setPhone("702\t\t\t122");

        hierarchicalInfileObjectLoader.setUseReplace(true);
        hierarchicalInfileObjectLoader.persist(expected);
        hierarchicalInfileObjectLoader.flush();

        Map<String, Object> actual = jdbcTemplate.queryForMap("select * from contact");

        assertEquals("がㄦ", actual.get("first_name"));
        assertEquals("ががががㄦ", actual.get("last_name"));
    }

    private byte[] toMd5(String s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.getBytes());
        return md.digest();
    }
}
