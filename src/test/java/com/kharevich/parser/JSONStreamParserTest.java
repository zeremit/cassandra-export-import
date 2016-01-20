package com.kharevich.parser;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;

/**
 * Created by zeremit on 18.1.16.
 */
public class JSONStreamParserTest extends Assert{

    @DataProvider(name = "jsonFile")
    public static Object[][] jsonFile(){
        return new Object[][]{{"{\"columns\":[{\"name\":\"uuid\",\"type\":\"uuid\"},{\"name\":\"surname\",\"type\":\"varchar\"},{\"name\":\"name\",\"type\":\"varchar\"},{\"name\":\"email\",\"type\":\"varchar\"},{\"name\":\"birthday\",\"type\":\"timestamp\"}],\"data\":[{\"uuid\":\"0eaab2a1-a6df-4632-a2d0-acdc3834c21f\",\"surname\":\"Goff\",\"name\":\"Katie Hansen\",\"email\":\"klevy@everyma1l.co.uk\",\"birthday\":\"1974-07-10 00:00:00.000\"},{\"uuid\":\"63977328-56a2-4b83-8d1d-c2fbd479867f\",\"surname\":\"Molina\",\"name\":\"Kathy Dawson\",\"email\":\"theylist@gma1l.org\",\"birthday\":\"1975-11-20 00:00:00.000\"},{\"uuid\":\"2402ca26-b98e-4dda-98c6-0581dbefbe4b\",\"surname\":\"Kent\",\"name\":\"Jeremy Bradley\",\"email\":\"leaderthrough@hotma1l.com\",\"birthday\":\"1982-03-20 00:00:00.000\"},{\"uuid\":\"a1f4fce3-e0e3-4432-a1f0-05c055f1de15\",\"surname\":\"Griffin\",\"name\":\"Martin Christensen\",\"email\":\"abouton@somema1l.com\",\"birthday\":\"1967-04-08 00:00:00.000\"},{\"uuid\":\"1e03ace1-124a-43fc-b9bb-61e30796e1dc\",\"surname\":\"Arnold\",\"name\":\"Wanda Armstrong\",\"email\":\"daysbuild@yah00.org\",\"birthday\":\"1975-03-24 00:00:00.000\"},{\"uuid\":\"f787e893-577e-4472-8d69-0dc596db1b0b\",\"surname\":\"Reyes\",\"name\":\"Katelyn Noel\",\"email\":\"holdman@yah00.com\",\"birthday\":\"1975-02-27 00:00:00.000\"},{\"uuid\":\"b3f785fc-1111-4d5a-8d00-dd52e3224ae4\",\"surname\":\"Manning\",\"name\":\"Johnny Roach\",\"email\":\"enterroom@everyma1l.co.uk\",\"birthday\":\"1971-07-21 00:00:00.000\"},{\"uuid\":\"40e730fd-d0a8-4be8-91ca-8f39796b83d8\",\"surname\":\"Miller\",\"name\":\"David Weaver\",\"email\":\"tooksuspense@ma1l2u.co.uk\",\"birthday\":\"1961-01-23 00:00:00.000\"}]}", 8},
                {"{\"columns\":[{\"name\":\"uuid\",\"type\":\"uuid\"},{\"name\":\"surname\",\"type\":\"varchar\"},{\"name\":\"name\",\"type\":\"varchar\"},{\"name\":\"email\",\"type\":\"varchar\"},{\"name\":\"birthday\",\"type\":\"timestamp\"}],\"data\":[]}", 0}};
    }

    @Test(dataProvider = "jsonFile")
    public void parseJson(String jsonFile, int expected) throws IOException, ParseException {
        Reader reader = new StringReader(jsonFile);
        Parser parser = new JSONStreamParser(reader);
        parser.jumpToData();
        int i = 0;
        while (parser.hasNextValues()){
            i++;
            parser.getNextValues();
        }
        assertEquals(i, expected);
    }
}
