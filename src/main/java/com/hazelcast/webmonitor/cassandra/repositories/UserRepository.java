package com.hazelcast.webmonitor.cassandra.repositories;

import com.hazelcast.webmonitor.cassandra.model.User;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;

public class UserRepository extends AbstractRepository {

    private final ColumnFamilyDefinition userColumnFamily;

    public UserRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        userColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "Users", ComparatorType.UTF8TYPE);

        add(userColumnFamily);
    }

    public void insert(User user){
        //todo: there is a data race here.
        if(exists(user.getUsername())){
            throw new IllegalArgumentException("A user with id:"+user.getUsername()+" already exists");
        }

        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());

        String rowId = user.getUsername();
        mutator.addInsertion(rowId, userColumnFamily.getName(), HFactory.createStringColumn("username", user.getUsername()))
                .addInsertion(rowId, userColumnFamily.getName(), HFactory.createStringColumn("password", user.getPassword()))
                .addInsertion(rowId, userColumnFamily.getName(), HFactory.createStringColumn("email", user.getEmail()))
                .addInsertion(rowId, userColumnFamily.getName(), HFactory.createStringColumn("company", user.getCompany()));
        mutator.execute();
    }

    public void delete(String username){
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(username, userColumnFamily.getName());
        mutator.execute();
    }

    public boolean exists(String username){
        return load(username)!=null;
    }

    public User login(String username, String password){
        User user = load(username);
        if(user == null){
            return null;
        }

        if(!user.getPassword().equals(password)){
            return null;
        }

        return user;
    }

    public User load(String username){
        SliceQuery<String, String,String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        q.setColumnFamily(userColumnFamily.getName())
                .setKey(username)
                .setColumnNames("username", "password", "email","company");
        QueryResult<ColumnSlice<String, String>> result = q.execute();
        ColumnSlice<String, String> columnSlice = result.get();
        if(columnSlice.getColumns().isEmpty()){
            return null;
        }

        User user = new User();
        user.setUsername(columnSlice.getColumnByName("username").getValue());
        user.setPassword(columnSlice.getColumnByName("password").getValue());
        user.setEmail(columnSlice.getColumnByName("email").getValue());
        user.setCompany(columnSlice.getColumnByName("company").getValue());
        return user;
    }
}
