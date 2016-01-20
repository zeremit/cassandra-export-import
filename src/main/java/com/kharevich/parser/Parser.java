package com.kharevich.parser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by zeremit on 15.1.16.
 */
public interface Parser {

    String getCQLInsert(String keyspace, String table) throws IOException;

    void jumpToData() throws IOException;

    Object[] getNextValues() throws IOException, ParseException;

}
