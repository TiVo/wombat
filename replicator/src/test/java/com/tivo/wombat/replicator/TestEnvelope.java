package com.tivo.wombat.replicator;

/**
 * Copyright 2015 TiVo Inc. All rights reserved.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestEnvelope
{
    @Test
    public void testMarshalNoBodyNoHeaders() throws Envelope.ParserException
    {
        Envelope envelope = new Envelope("Test", "");
        String mrpcExpected = "Test 2 0\r\n\r\n";
        checkMarshaling(envelope, mrpcExpected);
    }

    @Test
    public void testMarshalNoBody() throws Envelope.ParserException
    {
        Envelope envelope = new Envelope("Test", "");
        envelope.addHeader("header1", "1");
        checkMarshaling(envelope,
                        "Test 14 0\r\n" + 
                        "header1: 1\r\n\r\n");
    }

    @Test
    public void testMarshal() throws Envelope.ParserException
    {
        Envelope envelope = new Envelope("Test", "{}");
        envelope.addHeader("header1", "1");
        checkMarshaling(envelope,
                        "Test 14 2\r\n" + 
                        "header1: 1\r\n" + 
                        "\r\n" + 
                        "{}");
    }
 
    // XXX um, should this be allowed?
    public void testUnMarshalNoBlankLineAfterHeaders() throws Envelope.ParserException
    {
        Envelope unmarshaledObject = Envelope.parse( "Test 12 2\r\n" + 
                                                             "header1: 1\r\n" + 
                                                             "{}");
        assertEquals("{}", unmarshaledObject.getPayload());
        assertEquals("1", unmarshaledObject.getHeader("header1"));
    }

    @Test
    public void testBadFirstLine()
    {
        unmarshalAndFail("Test\r\n", 
                         "First line format is wrong. Expecting tag, header size, body size");

        unmarshalAndFail("Test 2\r\n", 
                         "First line format is wrong. Expecting tag, header size, body size");
    }

    @Test
    public void testWrongSizes()
    {
        unmarshalAndFail("Test 0 0\r\n", 
                         "Header length minimum is 2 (CRLF)");

        unmarshalAndFail("Test 2 2\r\n", 
                         "The sum of declared header and body sizes don't match the envelope size");

        unmarshalAndFail("Test 2 2\r\n", 
                         "The sum of declared header and body sizes don't match the envelope size");
    
        unmarshalAndFail(
            "Test 2 0\r\n" + 
            "header1: 1\r\n",
            "The sum of declared header and body sizes don't match the envelope size");

        unmarshalAndFail(
            "Test 12 10\r\n" + 
            "header1: 1\r\n",
            "The sum of declared header and body sizes don't match the envelope size");

        unmarshalAndFail(
            "Test 14 20\r\n" + 
            "header1: 1\r\n" + 
            "\r\n" + 
            "{}",
            "The sum of declared header and body sizes don't match the envelope size");
    }

    @Test
    public void testInvalidHeaderFormat()
    {
        unmarshalAndFail(
            "Test 9 0\r\n" + 
            "header1\r\n",
            "Invalid header format: 'header1'. Expecting key:value");

        unmarshalAndFail(
            "Test 10 0\r\n" + 
            "header1:\r\n",
            "Invalid header format: 'header1:'. Header value not found");

        unmarshalAndFail(
            "Test 11 0\r\n" + 
            "header1: \r\n",
            "Invalid header format: 'header1: '. Header value not found");
    }


    private void checkMarshaling(Envelope envelope, String marshaledExpected) throws Envelope.ParserException
    {
        String marshaledActual = envelope.marshal();
        System.out.println(marshaledActual);
        assertEquals(marshaledExpected, marshaledActual);
        Envelope unmarshaledObject = Envelope.parse(marshaledActual);
        assertEquals(envelope, unmarshaledObject);
    }    

    private void unmarshalAndFail(String mrpc, String expectedError)
    {
        try {
            Envelope.parse(mrpc);
            fail(String.format("Umarshaling '%s' should have failed with error %s", mrpc, expectedError));
        }
        catch (Envelope.ParserException ex) {
            System.out.println(ex.getMessage());
            assertEquals(expectedError, ex.getMessage());
        }
    }
}
