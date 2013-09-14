package com.hazelcast.webmonitor.cassandra.repositories;

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompanyRepositoryTest extends AbstractRepositoryTest {
    private CompanyRepository companyRepository;

    @Before
    public void setUp(){
        super.setUp();
        companyRepository = new CompanyRepository(cluster,keyspace);
    }

    @Test
    public void foo(){
        companyRepository.save("foo");
        companyRepository.save("bar");
    }

    @Test
    public void getCompanyNames_empty(){
        Set<String> result = companyRepository.getCompanyNames();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void getCompanyNames_notEmpty(){
        companyRepository.save("foo");
        companyRepository.save("bar");

        Set<String> result = companyRepository.getCompanyNames();
        assertNotNull(result);
        assertEquals(2,result.size());
        assertTrue(result.contains("foo"));
        assertTrue(result.contains("bar"));
    }
}
