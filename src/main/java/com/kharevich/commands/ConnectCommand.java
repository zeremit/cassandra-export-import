package com.kharevich.commands;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.kharevich.parser.JSONStreamParser;
import com.kharevich.formatter.*;
import com.kharevich.formatter.Formatter;
import org.apache.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.ParseException;
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

    private int fetchSize = 10000;

    private int batchSize = 100;

    @CliCommand(value = "set", help = "Sets custom properties")
    public String set(
            @CliOption(key = {"separator"}, mandatory = false, help = "CSV separator, default: ';'") String separator,
            @CliOption(key = {"directory"}, mandatory = false, help = "Output directory, default: './'") String directory,
            @CliOption(key = {"batch"}, mandatory = false, help = "batch size for insert operation") Integer batch
    ) {

        if (separator != null) {
            this.separator = separator.charAt(0);
        }

        if (directory != null) {
            this.directory = directory;
        }

        if(batch !=null){
            this.batchSize = batch;
        }

        return showVariables();
    }

    private String showVariables() {
        return "separator = " + separator + "\n" +
                "batch = " + batchSize + "\n" +
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
            @CliOption(key = {"keyspace"}, mandatory = true, help = "Keyspace name") String keyspace,
            @CliOption(key = {"table"}, mandatory = true, help = "Table name") String table,
            @CliOption(key = {"filename"}, mandatory = false, help = "Specify output filename") String filename,
            @CliOption(key = {"limit"}, mandatory = false, help = "limit of export rows") Integer limit
    ) {
        String outputPath = directory + File.separator;

        if (filename != null) {
            outputPath += filename;
        } else {
            outputPath += "result_" + new Date().getTime() + ".json";
        }
        Metadata metadata = session.getCluster().getMetadata();
        String initTable = metadata.getKeyspace(keyspace).getTable(table).asCQLQuery();
        System.out.println(initTable);
        Writer fw;
        Long savedCounter = 0l;
        Select selectStatement = QueryBuilder.select().all().from(keyspace, table);
        if(limit != null){
            selectStatement.limit(limit);
        }
        try {
            fw = new FileWriter(outputPath);
            log.info("exporting records");
            selectStatement.setFetchSize(fetchSize);
            ResultSet rs = session.execute(selectStatement);
            Iterator<Row> iterator = rs.iterator();
            int c = 0;
            List<ColumnDefinitions.Definition> columnDefinition = rs.getColumnDefinitions().asList();
            Formatter formatter = new JSONStreamFormatter(fw, columnDefinition);
            while (iterator.hasNext()) {
                savedCounter++;
                Row row = iterator.next();
                formatter.append(row);
                if ((c % fetchSize) == 0) {
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
            @CliOption(key = {"keyspace"}, mandatory = false, help = "keyspace") String keyspace,
            @CliOption(key = {"table"}, mandatory = false, help = "keyspace") String table,
            @CliOption(key = {"filename"}, mandatory = true, help = "Specify output filename") String filename
    ) throws ParseException {
        String outputPath = directory + File.separator;
        outputPath += filename;
        FileReader fr;
        Long savedCounter = 0l;
        try {
            fr = new FileReader(outputPath);
            JSONStreamParser streamParser = new JSONStreamParser(fr);
            PreparedStatement prepare = session.prepare(streamParser.getCQLInsert(keyspace, table));
            streamParser.jumpToData();
            BatchStatement statement = new BatchStatement();
            while(streamParser.hasNextValues()){
                Object[] array = streamParser.getNextValues();
                BoundStatement bprep = new BoundStatement(prepare);
                bprep.bind(array);
                statement.add(bprep);
                savedCounter++;
                if(savedCounter%batchSize==0){
                    session.execute(statement);
                    statement = new BatchStatement();
                    System.out.println(" -- imported " + savedCounter + " records");
                }

            }
            session.execute(statement);
//            }
            log.info(" -- imported " + savedCounter + " records");
//            reader.close();
        } catch (IOException e) {
            StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            return errors.toString();
        }
        return "Success, " + savedCounter + " rows imported from " + outputPath;
    }
}
