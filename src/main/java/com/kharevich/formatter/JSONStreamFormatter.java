package com.kharevich.formatter;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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
 * Created by zeremit on 1/13/16.
 */
public class JSONStreamFormatter implements Formatter{

    private final JsonGenerator generator;
    private final Writer writer;
    private final List<ColumnDefinitions.Definition> columnDefinition;

    public JSONStreamFormatter(Writer writer, List<ColumnDefinitions.Definition> columnDefinition) throws IOException {
        this.writer = writer;
        this.columnDefinition = columnDefinition;
        JsonFactory jsonFactory = new JsonFactory();
        generator = jsonFactory.createGenerator(writer);
        generator.writeStartObject();
        generator.writeArrayFieldStart("columns");
        for(ColumnDefinitions.Definition key : columnDefinition){
            generator.writeStartObject();
            generator.writeStringField("name", key.getName());
            generator.writeStringField("type", key.getType().toString());
            generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeArrayFieldStart("data");
    }


    public void append(Row row) throws IOException {
        generator.writeStartObject();
        for (ColumnDefinitions.Definition key : columnDefinition) {
            Object o = row.getObject(key.getName());
            String keyName = key.getName();
            if(o == null) {
                generator.writeNullField(keyName);
            } else if(key.getType() == DataType.timeuuid() || key.getType() == DataType.uuid()) {
                UUID uuid = (UUID) o;
                generator.writeStringField(keyName, uuid.toString());
            } else if(key.getType() == DataType.blob()) {
                ByteBuffer buffer = (ByteBuffer) o;
                byte[] blobData = buffer.array();
                generator.writeBinaryField(keyName, blobData);
            } else if(key.getType() == DataType.timestamp()) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date t = row.getTimestamp(keyName);
                generator.writeStringField(keyName, fmt.format(t));
            } else {
                generator.writeStringField(keyName, o.toString());
            }
        }
        generator.writeEndObject();
    }

    public void close() throws IOException {
        generator.writeEndArray();
        generator.writeEndObject();
        generator.close();
        writer.close();
    }
}
