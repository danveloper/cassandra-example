package com;

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CustomerRepositoryTest extends AbstractRepositoryTest {
    private CustomersRepository customerRepository;

    @Before
    public void setUp(){
        super.setUp();
        customerRepository = new CustomersRepository(cluster,keyspace);
    }

    @Test
    public void foo(){
        customerRepository.save("foo");
        customerRepository.save("bar");
    }

    @Test
    public void getCustomers_WhenEmpty(){
        Set<String> result = customerRepository.getCustomers();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void getCustomers_WhenNotEmpty(){
        customerRepository.save("foo");
        customerRepository.save("bar");

        Set<String> result = customerRepository.getCustomers();
        assertNotNull(result);
        assertEquals(2,result.size());
        assertTrue(result.contains("foo"));
        assertTrue(result.contains("bar"));
    }
}
