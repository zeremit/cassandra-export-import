package com.kharevich.formatter;


import com.datastax.driver.core.Row;

import java.io.IOException;

public interface Formatter {

    void append(Row row) throws IOException;

    void close() throws IOException;
}
