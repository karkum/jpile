package com.opower.persistence.jpile.sample;

import java.math.BigDecimal;
import java.util.Date;
import com.google.common.collect.ImmutableList;

/**
 * Creates objects for testing
 *
 * @author amir.raminfar
 */
public final class ObjectFactory {

    private ObjectFactory() {
    }

    public static Customer newCustomer() {
        Customer customer = new Customer();
        customer.setContact(newContact());
        customer.setLastSeenOn(new Date());
        customer.setType(Customer.Type.RESIDENTIAL);
        Supplier supplier = newSupplier();
        customer.setProducts(ImmutableList.of(newProduct(customer, supplier), newProduct(customer, supplier),
                newProduct(customer, supplier), newProduct(customer, supplier)));

        return customer;
    }

    public static Contact newContact() {
        Contact contact = new Contact();
        contact.setFirstName("John");
        contact.setLastName("Smith");
        contact.setPhone("1234445566");
        contact.setType(Contact.Type.PRIMARY);
        contact.setAddress(newAddress());

        return contact;
    }

    public static Product newProduct(Customer customer, Supplier supplier) {
        Product product = new Product();
        product.setCustomer(customer);
        product.setDescription("This is a short description about this product");
        product.setPrice(BigDecimal.valueOf(1.23));
        product.setPurchasedOn(new Date());
        product.setTitle("Title of an awesome product");
        product.setPackaging(Product.Packaging.MEDIUM);
        product.setSupplier(supplier);

        return product;
    }

    public static Supplier newSupplier() {
        Supplier supplier = new Supplier();
        supplier.setName("Company Co");
        supplier.setAddress(newAddress());

        return supplier;
    }

    public static Address newAddress() {
        Address address = new Address();
        address.setStreetNumber("1515");
        address.setStreet("N Courthouse Rd");
        address.setCity("Arlington");
        address.setState("VA");
        address.setZipCode("22201");

        return address;
    }
}
