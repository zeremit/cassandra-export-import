package com.kharevich.commands.parser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by zeremit on 15.1.16.
 */
public interface Parser {

    String getCQLInsert() throws IOException;

    void jumpToData() throws IOException;

    boolean hasNextValues() throws IOException;

    Object[] getNextValues() throws IOException, ParseException;

}
