package com.hazelcast.webmonitor.repositories;

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ActiveMembersRepositoryTest extends AbstractRepositoryTest {

    private ActiveMembersRepository activeMembersRepository;

    @Before
    public void setUp(){
        super.setUp();
        activeMembersRepository = new ActiveMembersRepository(cluster,keyspace);
    }

    @Test
    public void getActiveMembers_noMembers(){
        Set<String> members = activeMembersRepository.getActiveMembers("hazelcast","dev",0,100);
        assertTrue(members.isEmpty());
    }

    @Test
    public void getActiveMembers(){
        activeMembersRepository.insert("hazelcast","dev","192.168.1.1:5701",100);

        Set<String> members = activeMembersRepository.getActiveMembers("hazelcast","dev",0,200);
        assertEquals(1,members.size());
        assertTrue(members.contains("192.168.1.1:5701"));
    }

    @Test
    public void getActiveClusters(){
        activeMembersRepository.insert("hazelcast","dev","192.168.1.1:5701",100);

        Set<String> members = activeMembersRepository.getActiveClusters("hazelcast",0,200);
        assertEquals(1,members.size());
        assertTrue(members.contains("dev"));
    }

    @Test
    public void getActiveMembers_inActiveMember(){
        activeMembersRepository.insert("hazelcast","dev","192.168.1.1:5701",100);

        Set<String> members = activeMembersRepository.getActiveMembers("hazelcast","dev",200,300);
        assertTrue(members.isEmpty());
    }
}
