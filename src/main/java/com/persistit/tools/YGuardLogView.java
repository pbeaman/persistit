/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 17, 2004
 */
package com.persistit.tools;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.TreeMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.persistit.Persistit;

/**
 *
 * @version 1.0
 */
public class YGuardLogView
extends DefaultHandler
{
    private final static String[] TYPES = {"package", "class", "method", "field"};
    boolean _inMapElement = false;
    TreeMap _map = new TreeMap();
    PrintWriter _writer;
    
    public static void main(String[] args)
    throws Exception
    {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();
        String fileName = args.length == 0 ? "C:/temp/persistit/dist/buildinfo.xml" : args[0];
        
        FileInputStream fis = new FileInputStream(fileName);

        PrintWriter writer = new PrintWriter(new FileWriter(fileName + ".map.txt"));
        YGuardLogView ylv = new YGuardLogView(writer);
        try
        {
            parser.parse(fis, ylv);
            fis.close();
        }
        finally
        {
            writer.close();
        }
    }
    
    YGuardLogView(PrintWriter writer)
    {
        _writer = writer;
    }
    
    public void startElement(
        String uri, 
        String localName, 
        String qName, 
        Attributes attributes)
    throws SAXException
    {
        if ("map".equals(qName)) _inMapElement = true;
        else if (!_inMapElement) return;
        else
        {
            for (int i = 0; i < TYPES.length; i++)
            {
                if (TYPES[i].equals(qName)) map(TYPES[i], attributes);
            }
        }
    }
    
    public void endDocument()
    {
        Iterator iter = _map.keySet().iterator();
        while (iter.hasNext())
        {
            String s = (String)iter.next();
            _writer.println(s);
        }
    }
    
    private void map(String type, Attributes attrs)
    {
        StringBuffer sb = new StringBuffer(type);
        sb.append("  ");
        sb.append(attrs.getValue("map"));
        if (type == TYPES[2] || type == TYPES[3])
        {
            sb.append("  ");
            sb.append(attrs.getValue("class"));
        }
        sb.append("  ");
        sb.append(attrs.getValue("name"));
        _map.put(sb.toString(), "");
    }
    
}
