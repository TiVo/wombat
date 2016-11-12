/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Marshaling/unmarshaling of a Envelope message. It allows an arbitrary
 * namespace in the first line, arbitrary headers and an empty message body.
 * 
 * <tag><headers length><body length>\r\n
 * header: value\r\n
 * header: value\r\n
 * \r\n
 * body
 *
 * Where tag identifies a protocol. A protocol is simply a namespace. One use of
 * this namespace is to let you trigger off and identify what are required set
 * of headers for a namespace.
 * 
 * Header and body lengths are in chars. CR/LF between headers and body is
 * included into header length (2 chars). Spaces before ':' in the header line
 * are OK. They won't be trimmed. Spaces after ':' in the header line will be
 * trimmed.
 * 
 */
public class Envelope
{
    @SuppressWarnings("serial")
    public static class ParserException extends Exception 
    {
        ParserException(String message) 
        {
            super(message);
        }
    }

    /**
     * Protocol name/version
     */
    protected final String protocolTag;

    /**
     * Headers. From the point of view of this class they are all optional.
     */
    protected final Map<String, String> headersMap;
    
    /**
     * Body of the message
     */
    protected final String payload;

    protected Envelope(String tag, String payload)
    {
        headersMap = new HashMap<String, String>();
        protocolTag = tag;
        this.payload = payload;
    }

    protected Envelope(String tag, Map<String, String> headersMap, String payload)
    {
        protocolTag = tag;
        this.headersMap = headersMap;
        this.payload = payload;
    }

    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Envelope)) {
            return false;
        }
        Envelope other = (Envelope)obj;
        
        return (payload.equals(other.payload) && 
                protocolTag.equals(other.protocolTag) && 
                headersMap.equals(other.headersMap));
    }

    public String getPayload() 
    {
        return payload;
    }

    public String getHeader(String header) 
    {
        return headersMap.get(header);
    }

    public void addHeader(String header, String value) 
    {
        headersMap.put(header, value);
    }

    public String marshal() 
    {
        checkNotNull(protocolTag);
        StringWriter headers = new StringWriter();
        Iterator<Entry<String, String>> it = headersMap.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> pair = it.next();
            headers.write(String.format("%s: %s\r\n", pair.getKey(), pair.getValue()));
        }
        headers.write("\r\n");
        String headerStr = headers.toString();
        if (payload != null) {
            return String.format("%s %d %d\r\n%s%s", protocolTag, headerStr.length(), payload.length(), headerStr, payload);
        }
        return String.format("%s %d 0\r\n%s", protocolTag, headerStr.length(), headerStr);
    }

    public static Envelope parse(String message) throws ParserException
    {
        checkNotNull(message);
        if (message.isEmpty()) {
            throw new ParserException("Envelope can't be empty");
        }
            
        int firstLineEnd = message.indexOf("\r\n");   
        if (firstLineEnd == -1) {
            throw new ParserException("First line of Envelope must end with CR/LF");
        }
        String firstLine = message.substring(0, firstLineEnd);
        String[] tagAndSizes = firstLine.split(" ");
        if (tagAndSizes.length != 3) {
            throw new ParserException("First line format is wrong. Expecting tag, header size, body size");
        }

        int headersLength = 0;
        int bodyLength = 0;
        try {
            headersLength = Integer.parseInt(tagAndSizes[1]);
            bodyLength = Integer.parseInt(tagAndSizes[2]);
        }
        catch (NumberFormatException ex) {
            throw new ParserException("First line format is wrong. Header and body sizes must be integers");
        }
        if (headersLength < 2) {
            throw new ParserException("Header length minimum is 2 (CRLF)");
        }
        if (headersLength + bodyLength != message.length() - firstLine.length() - 2) {
            throw new ParserException("The sum of declared header and body sizes don't match the envelope size");
        }
        Map<String, String> headersMap = new HashMap<String, String>();
        if (headersLength > 0) {  
            String headers = null;
            try {
                headers = message.substring(firstLine.length() + 2, firstLine.length() + 2 + headersLength);
            }
            catch (IndexOutOfBoundsException ex) {
                //should not happen
                throw new ParserException("Declared headers length too big");                    
            }
            String[] headerLines = headers.split("\r\n");   
            for (int i = 0; i < headerLines.length; i++) {
                if (headerLines[i].isEmpty()) {
                    //allow any number of blank lines
                    continue;
                }
                // not going to require a blank line after the headers
                // since we have header and body lengths.
                int colon = headerLines[i].indexOf(":");
                if (colon == -1) {
                    throw new ParserException(String.format("Invalid header format: '%s'. Expecting key:value", 
                                                            headerLines[i]));
                }
                // Unlike http, we allow spaces before ':' and don't trim them.
                // Spaces become part of the key.
                String key = headerLines[i].substring(0, colon);
                String value = headerLines[i].substring(colon+1);
                if (value == null || value.isEmpty()) {
                    throw new ParserException(String.format("Invalid header format: '%s'. Header value not found", 
                                                            headerLines[i]));
                }
                // Spaces are trimmed from the values
                value = value.trim();
                if (value.isEmpty()) {
                    throw new ParserException(String.format("Invalid header format: '%s'. Header value not found", 
                                                            headerLines[i]));
                }
                headersMap.put(key, value);
            }
        }
        String payload = "";
        if (bodyLength > 0) {
            try {
                payload = message.substring(firstLine.length() + 2 + headersLength);
            }
            catch (IndexOutOfBoundsException ex) {
                //should not happen
                throw new ParserException("First line plus declared headers length exceeded message size");                    
            }            
        }     
        return new Envelope(tagAndSizes[0], headersMap, payload);           
    }
    
   private static void checkNotNull(Object o) {
       if (o == null) {
           throw new NullPointerException();
       }
   }
}
