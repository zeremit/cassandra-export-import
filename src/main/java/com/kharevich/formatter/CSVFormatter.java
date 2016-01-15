package com.kharevich.formatter;

import au.com.bytecode.opencsv.CSVWriter;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Created by zeremit on 1/11/16.
 */
public class CSVFormatter implements Formatter {

    private CSVWriter csvWriter;

    private List<ColumnDefinitions.Definition> columnDefinition;

    private final static char separator = ';';

    public CSVFormatter(Writer writer, List<ColumnDefinitions.Definition> columnDefinition){
        csvWriter = new CSVWriter(writer, separator);
        this.columnDefinition = columnDefinition;
    }

    public void append(Row row) {
        String[] valuesArray = new String[columnDefinition.size()];
        int i = 0;
        for (ColumnDefinitions.Definition key : columnDefinition) {
            Object o = row.getObject(key.getName());
            String value;
            if (o == null) {
                value = null;
            } else if (key.getType() == DataType.timeuuid()) {
                UUID uuid = (UUID) o;
                value = uuid.toString();
            } else if (key.getType() == DataType.uuid()) {
                UUID uuid = (UUID) o;
                value = uuid.toString();
            } else if (key.getType() == DataType.blob()) {
                ByteBuffer buffer = (ByteBuffer) o;
                byte[] blobData = buffer.array();
                value = Base64.encodeBase64String(blobData);
            } else if (key.getType() == DataType.timestamp()) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date t = row.getTimestamp(key.getName());
                value = fmt.format(t);
            } else {
                value = o.toString();
            }
            valuesArray[i] = value;
            i++;
        }
        csvWriter.writeNext(valuesArray);
    }

    public void close() throws IOException {
        csvWriter.close();
    }
}
