package com.kharevich.formatter;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Created by zeremit on 1/12/16.
 */
public class JSONFormatter implements Formatter {

    Writer writer;

    public JSONFormatter(Writer writer){
        this.writer = writer;
    }



    public void append(Row row) {
        JSONObject rowObject = new JSONObject();
        JSONObject rowData = new JSONObject();
        for (ColumnDefinitions.Definition key : row.getColumnDefinitions().asList()) {
            Object o = row.getObject(key.getName());
            if(o == null) {
                rowData.put(key.getName(), null);
            } else if(key.getType() == DataType.timeuuid()) {
                UUID uuid = (UUID) o;
                String txt = uuid.toString();
                rowData.put(key.getName(), txt);
            } else if(key.getType() == DataType.uuid()) {
                UUID uuid = (UUID) o;
                String txt = uuid.toString();
                rowData.put(key.getName(), txt);
            } else if(key.getType() == DataType.blob()) {
                ByteBuffer buffer = (ByteBuffer) o;
                byte[] blobData = buffer.array();
                String blobBase64 = Base64.encodeBase64String(blobData);
                rowData.put(key.getName(), blobBase64);
            } else if(key.getType() == DataType.timestamp()) {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date t = row.getTimestamp(key.getName());
                String text = fmt.format(t);
                rowData.put(key.getName(), text);
            } else {
                rowData.put(key.getName(), o.toString());
            }
        }
        rowObject.put("data", rowData);
        try {
            writer.write(rowObject.toJSONString());
            writer.write(",\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() throws IOException {
        writer.close();
    }
}
