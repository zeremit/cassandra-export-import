package com.kharevich.commands;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.kharevich.formatter.*;
import com.kharevich.formatter.Formatter;
import org.apache.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;

/**
 * Created by zeremit on 1/6/16.
 */
@Component
public class ConnectCommand implements CommandMarker {

    private static final Logger log = Logger.getLogger(ConnectCommand.class);

    private Cluster cluster;

    private Session session;

    private char separator = ';';

    private String directory = ".";

    @CliCommand(value = "set", help = "Sets custom properties")
    public String set(
            @CliOption(key = {"separator"}, mandatory = false, help = "CSV separator, default: ';'") String separator,
            @CliOption(key = {"directory"}, mandatory = false, help = "Output directory, default: './'") String directory
    ) {

        if (separator != null) {
            this.separator = separator.charAt(0);
        }

        if (directory != null) {
            this.directory = directory;
        }

        return showVariables();
    }

    private String showVariables() {
        return "separator = " + separator + "\n" +
                "directory = " + directory;
    }

    @CliCommand(value = "connect", help = "Connects to Cassandra database")
    public String connect(@CliOption(key = {"node"}, mandatory = true, help = "Cassandra's cluster IP address") String node,
                          @CliOption(key = {"keyspace"}, mandatory = false, help = "Cassandra's keyspace") String keyspace,
                          @CliOption(key = {"user"}, mandatory = false, help = "Cassandra's user name") String user,
                          @CliOption(key = {"password"}, mandatory = false, help = "Cassandra's user password") String password) {
        Cluster.Builder clusterBuild = Cluster.builder().addContactPoint(node);
        if (user != null && password != null) {
            clusterBuild.withCredentials(user, password);
        }
        clusterBuild.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));
        cluster = clusterBuild.build();
        session = cluster.connect();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(60000);
        Metadata metadata = cluster.getMetadata();
        for (Host host : metadata.getAllHosts()) {
            log.info(String.format("Datatacenter: %s; Host: %s; Rack: %s",
                    host.getDatacenter(), host.getAddress(), host.getRack()));
        }
        if (keyspace != null) {
            session.execute("USE " + keyspace + ";");
        }
        return "Connected to cluster: " + metadata.getClusterName() + "\n"
                + "Keyspace: " + session.getLoggedKeyspace();
    }

    @CliCommand(value = "export", help = "Queries database and saves output to CSV file")
    public String exportToCSV(
            @CliOption(key = {"stmt"}, mandatory = true, help = "CQL query statement") String q,
            @CliOption(key = {"filename"}, mandatory = false, help = "Specify output filename") String filename
    ) {
        String outputPath = directory + File.separator;

        if (filename != null) {
            outputPath += filename;
        } else {
            outputPath += "result_" + new Date().getTime() + ".csv";
        }
        Writer fw;
        Long savedCounter = 0l;
        try {
            fw = new FileWriter(outputPath);
            log.info("exporting records");
            Statement stmt = new SimpleStatement(q);
            ResultSet rs = session.execute(stmt);
            Iterator<Row> iter = rs.iterator();
            int c = 0;
            List<ColumnDefinitions.Definition> columnDefinition = rs.getColumnDefinitions().asList();
            Formatter formatter = new JSONStreamFormatter(fw, columnDefinition);
            while (iter.hasNext()) {
                savedCounter++;
                Row row = iter.next();
                formatter.append(row);
                if ((c % 1000) == 0) {
                    log.info(" ... exported " + c + " rows");
                }
                c++;
            }
            log.info(" -- exported " + c + " records");
            formatter.close();
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            return errors.toString();
        }
        return "Success, " + savedCounter + " rows saved to " + outputPath;
    }

    @CliCommand(value = "import", help = "Queries database and saves output to CSV file")
    public String importFromCSV(
            @CliOption(key = {"stmt"}, mandatory = true, help = "CQL query statement") String q,
            @CliOption(key = {"filename"}, mandatory = true, help = "Specify output filename") String filename
    ) {
        String outputPath = directory + File.separator;
        outputPath += filename;
        FileReader fr;
        Long savedCounter = 0l;
        try {
            fr = new FileReader(outputPath);
            CSVReader reader = new CSVReader(fr, separator);
            log.info("importing records");
            long c = 0l;
            while (true) {
                String[] arrayValues = reader.readNext();
                if (arrayValues == null) {
                    break;
                }
                c++;
            }
            log.info(" -- imported " + c + " records");
            reader.close();
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            return errors.toString();
        }
        return "Success, " + savedCounter + " rows imported from " + outputPath;
    }
}
