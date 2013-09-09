package com.hazelcast.webmonitor.repositories;

import com.hazelcast.webmonitor.model.User;
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
        user.setUsername("peter");
        user.setPassword("password");

        userRepository.insert(user);
        User found = userRepository.load(user.getUsername());
        assertEquals(user, found);
    }

    @Test
    public void insert_duplicate() {
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setUsername("peter");
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
        user.setUsername("peter");
        user.setPassword("password");

        userRepository.insert(user);
        boolean found = userRepository.exists(user.getUsername());
        assertTrue(found);
    }

    // ====================== delete ===========================


    @Test
   public void delete_nonExisting(){
        userRepository.delete("non existing");
        boolean found = userRepository.exists("non existing");
        assertFalse(found);
    }

    @Test
    public void delete_existing(){
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setUsername("peter");
        user.setPassword("password");

        userRepository.insert(user);
        userRepository.delete(user.getUsername());
        boolean found = userRepository.exists(user.getUsername());
        assertFalse(found);
    }

    // ======================= exist ==========================

    @Test
    public void login_notFound() {
        User found = userRepository.login("peter", "password");
        assertNull(found);
    }

    @Test
    public void login_badPassword(){
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setUsername("peter");
        user.setPassword("password");

        userRepository.insert(user);
        User found = userRepository.login(user.getUsername(),"bad password");
        assertNull(found);
    }

    @Test
    public void login_success(){
        User user = new User();
        user.setCompany("hazelcast");
        user.setEmail("peter@hazelcast.com");
        user.setUsername("peter");
        user.setPassword("password");

        userRepository.insert(user);
        User found = userRepository.login(user.getUsername(),user.getPassword());
        assertEquals(user, found);
    }

}

