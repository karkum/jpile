package com.opower.persistence.jpile.loader;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableSet;
import com.opower.persistence.jpile.AbstractIntTestForJPile;
import com.opower.persistence.jpile.sample.Customer;
import com.opower.persistence.jpile.sample.Data;
import com.opower.persistence.jpile.sample.ObjectFactory;
import com.opower.persistence.jpile.sample.Product;
import org.junit.Test;
import org.springframework.jdbc.core.RowMapper;

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
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Customer expected = ObjectFactory.newCustomer();
        hierarchicalInfileObjectLoader.persist(expected);
        hierarchicalInfileObjectLoader.flush();
        Map<String, Object> customer = simpleJdbcTemplate.queryForMap("select * from customer");
        Map<String, Object> contact = simpleJdbcTemplate.queryForMap("select * from contact");
        Map<String, Object> phone = simpleJdbcTemplate.queryForMap("select * from contact_phone");
        List<Map<String, Object>> products = simpleJdbcTemplate.queryForList("select * from product");

        assertEquals(simpleDateFormat.format(expected.getLastSeenOn()), simpleDateFormat.format(customer.get("last_seen_on")));
        assertEquals(expected.getId(), customer.get("id"));
        assertEquals(expected.getId(), contact.get("customer_id"));
        assertEquals(expected.getContact().getFirstName(), contact.get("first_name"));
        assertEquals(expected.getContact().getLastName(), contact.get("last_name"));
        assertEquals(expected.getId(), phone.get("customer_id"));
        assertEquals(expected.getContact().getPhone(), phone.get("phone"));
        assertEquals(expected.getProducts().size(), products.size());


        for (int i = 0, productsSize = expected.getProducts().size(); i < productsSize; i++) {
            Product expectedProduct = expected.getProducts().get(i);
            Map<String, Object> actualMap = products.get(i);
            assertEquals(expectedProduct.getId(), actualMap.get("id"));
            assertEquals(expected.getId().intValue(), actualMap.get("customer_id"));
            assertEquals(expectedProduct.getTitle(), actualMap.get("title"));
            assertEquals(expectedProduct.getDescription(), actualMap.get("description"));
            assertEquals(expectedProduct.getPrice().doubleValue(), actualMap.get("price"));
            assertEquals(simpleDateFormat.format(expectedProduct.getPurchasedOn()),
                         simpleDateFormat.format(actualMap.get("purchased_on")));
        }

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

        Data actual = simpleJdbcTemplate.queryForObject("select * from binary_data", new RowMapper<Data>() {
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

        Customer customer = new Customer();
        hierarchicalInfileObjectLoader.persist(customer);

        assertNull(customer.getId());
    }

    @Test
    public void testEventCallback() {
        HierarchicalInfileObjectLoader.CallBack callBack = mock(HierarchicalInfileObjectLoader.CallBack.class);
        hierarchicalInfileObjectLoader.setEventCallback(callBack);

        Customer customer = new Customer();
        hierarchicalInfileObjectLoader.persist(customer);

        verify(callBack, times(1)).onBeforeSave(customer);
        verify(callBack, times(1)).onAfterSave(customer);
    }

    private byte[] toMd5(String s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.getBytes());
        return md.digest();
    }
}
