package com.repositories;

import com.User;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class UserRepositoryTest extends AbstractRepositoryTest {
    private UserRepository userRepository;

    @Before
    public void setUp() {
        super.setUp();
        userRepository = new UserRepository(cluster, keyspace);
    }

    @Test
    public void load_notFound() {
        User found = userRepository.load("foo");
        assertNull(found);
    }

    // ======================= insert ==========================

    @Test
    public void insert_nonExisting() {
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setLoginName("peter");
        user.setPassword("password");

        userRepository.insert(user);
        User found = userRepository.load(user.getLoginName());
        assertEquals(user, found);
    }

    @Test
    public void insert_duplicate() {
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setLoginName("peter");
        user.setPassword("password");

        userRepository.insert(user);

        try {
            userRepository.insert(user);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // ======================= exist ==========================

    @Test
    public void exist_notFound() {
        boolean found = userRepository.exists("foo");
        assertFalse(found);
    }

    @Test
    public void exists_found(){
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setLoginName("peter");
        user.setPassword("password");

        userRepository.insert(user);
        boolean found = userRepository.exists(user.getLoginName());
        assertTrue(found);
    }
}

