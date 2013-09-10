package com.hazelcast.webmonitor.repositories;

import com.hazelcast.webmonitor.model.WarningMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WarningMessageRepositoryTest extends AbstractRepositoryTest {

    private WarningMessageRepository warningMessageRepository;

    @Before
    public void setUp() {
        super.setUp();
        warningMessageRepository = new WarningMessageRepository(cluster, keyspace);
    }

    @Test
    public void get_nonExisting() {
        WarningMessage warningMessage = warningMessageRepository.get("hazelcast","non existing","foo");
        assertNull(warningMessage);
    }

    @Test
    public void save(){
        WarningMessage warningMessage = new WarningMessage("hazelcast","date","somemessage","dev");

        warningMessageRepository.save(warningMessage);
        WarningMessage found = warningMessageRepository.get(warningMessage.getCompany(),warningMessage.getClusterName(), warningMessage.getDate());
        assertEquals(warningMessage,found);
    }
}
