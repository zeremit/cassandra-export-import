package com.kharevich.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by zeremit on 15.1.16.
 */
public class JSONStreamParser implements Parser {


    private final JsonParser jsonParser;
    private Map<String, String> fields;

    public JSONStreamParser(Reader reader) throws IOException {
        jsonParser = new JsonFactory().createParser(reader);
        if (jsonParser.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected data to start with an Object");
        }
        fields = Maps.newLinkedHashMap();
        getTableFields();
        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jsonParser.getCurrentName();
            if ("data".equals(fieldName)) {
                jsonParser.nextToken();
                break;
            }
        }
    }

    private void getTableFields() throws IOException {
        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jsonParser.getCurrentName();
                if ("columns".equals(fieldName)) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        String name = null, type = null;
                        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                            fieldName = jsonParser.getCurrentName();
                            if ("name".equals(fieldName)) {
                                name = jsonParser.getValueAsString();
                            }
                            if ("type".equals(fieldName)) {
                                type = jsonParser.getValueAsString();
                            }
                        }
                        fields.put(name, type);
                    }
                    break;
                }

        }
    }


    public String getCQLInsert(String keyspace, String table) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(keyspace).append(".").append(table);
        sb.append(" (");
        int i = 0;
        for (Iterator it = fields.keySet().iterator(); it.hasNext(); ) {
            String colName = (String) it.next();
            if (i != 0) {
                sb.append(',');
            }
            sb.append(colName);
            i++;
        }
        int columnCount = fields.size();
        sb.append(") VALUES (");
        for (i = 0; i < columnCount; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append('?');
        }
        sb.append(");");
        return sb.toString();
    }

    public void jumpToData() throws IOException {

    }

    public boolean hasNextValues() throws IOException {
        return jsonParser.nextToken() != JsonToken.END_ARRAY;
    }

    public synchronized Object[] getNextValues() throws IOException, ParseException {
        if(!hasNextValues())
            return null;
        Object[] array = new Object[fields.size()];
                int i = 0;
                while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                        String fieldValue = jsonParser.getCurrentName();
                        String type = fields.get(fieldValue);
                        jsonParser.nextToken();
                        String dataValue = jsonParser.getValueAsString();
                        switch (type) {
                            case "timestamp":
                                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                                Date d = fmt.parse(dataValue);
                                array[i] = d;
                                break;
                            case "blob":
                                byte[] blobData = Base64.decodeBase64(dataValue);
                                ByteBuffer buffer = ByteBuffer.wrap(blobData);
                                array[i] = buffer;
                                break;
                            case "double":
                                Double dv = Double.parseDouble(dataValue);
                                array[i] = dv;
                                break;
                            case "bigint":
                                Long ln = Long.parseLong(dataValue);
                                array[i] = ln;
                                break;
                            case "boolean":
                                Boolean bv = Boolean.parseBoolean(dataValue);
                                array[i] = bv;
                                break;
                            case "int":
                                Integer n = Integer.parseInt(dataValue);
                                array[i] = n;
                                break;
                            case "timeuuid":
                            case "uuid":
                                array[i] = UUID.fromString(dataValue);
                                break;
                            default:
                                array[i] = dataValue;
                                break;
                        }
                        i++;
                    }

                }
//            }
//            break;
//        }

        return array;
    }
}
