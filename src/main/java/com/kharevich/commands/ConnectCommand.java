package com.kharevich.commands;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * Created by zeremit on 1/6/16.
 */
@Component
public class ConnectCommand implements CommandMarker {

    private Cluster cluster;

    private Session session;

    @CliCommand(value = "connect", help = "Connects to Cassandra database")
    public String connect(@CliOption(key = {"node"}, mandatory = true, help = "Cassandra's cluster IP address") String node,
                          @CliOption(key = {"keyspace"}, mandatory = false, help = "Cassandra's keyspace") String keyspace,
                          @CliOption(key = {"user"}, mandatory = false, help = "Cassandra's user name") String user,
                          @CliOption(key = {"password"}, mandatory = false, help = "Cassandra's user password") String password) {
        Cluster.Builder clusterBuild = Cluster.builder().addContactPoint(node);
        if (user != null && password != null) {
            clusterBuild.withCredentials(user, password);
        }
        cluster = clusterBuild.build();
        Metadata metadata = cluster.getMetadata();
        session = cluster.connect();
        if (keyspace != null) {
            session.execute("USE " + keyspace + ";");
        }
        return "Connected to cluster: " + metadata.getClusterName() + "\n"
                + "Keyspace: " + session.getLoggedKeyspace();
    }
}
