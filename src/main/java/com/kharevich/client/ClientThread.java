package com.kharevich.client;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.kharevich.parser.Parser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by khorevich on 1/20/16.
 */
public class ClientThread extends Thread{

    private final PreparedStatement prepare;
    private String cqlQuery;

    Parser parser;

    Session session;

    long savedCounter = 0;

    int batchSize = 100;

    public ClientThread(Parser parser, Session session, String keyspace, String table) throws IOException {

        prepare = session.prepare(parser.getCQLInsert(keyspace, table));
        this.parser = parser;
        this.session = session;

    }

    @Override
    public void run() {
        BatchStatement statement = new BatchStatement();
        try {
            Object[] array = parser.getNextValues();
            while(array!=null) {
                BoundStatement bprep = new BoundStatement(prepare);
                bprep.bind(array);
                statement.add(bprep);
                savedCounter++;
                if (savedCounter % batchSize == 0) {
//                    session.execute(statement);
                    statement = new BatchStatement();
                    if (savedCounter % 10000 == 0) {
                        System.out.println(" -- imported " + savedCounter + " records");
                    }
                }
                array = parser.getNextValues();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
