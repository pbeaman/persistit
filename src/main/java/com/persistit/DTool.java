/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
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
 */
package com.persistit;

import java.awt.Color;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;

import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.PlainDocument;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.*;

public class DTool
{
    public static PrintStream _out = System.out;
    
    public static void buffer(Volume volume, long page)
    throws Exception
    {
        Buffer buffer = volume.getPool().getBufferCopy(volume, page);
        buffer(buffer);
    }

    public static void buffer(Buffer buffer)
    {
        l();
        wl(buffer.summarize());
        wl("Verify: " + buffer.verify(null));
        l();
        Management.RecordInfo[] records = buffer.getRecords();
        wl(bufferDump(records));
        l();
    }
    
    static String bufferDump(Management.RecordInfo[] records)
    {
        StringBuffer sb = new StringBuffer();
        Key key = new Key((Persistit)null);
        for (int i = 0; i < records.length; i++)
        {
            Management.RecordInfo r = records[i];
            r._key.copyTo(key);
            sb.append(i + ": db=" + r._db + " ebc=" + r._ebc + " ptr=" + r._pointerValue + " key=" + key + "\r\n");
        }
        return sb.toString();
    }
    
    static void w(String s)
    {
        _out.print(s);
        _out.flush();
    }
    
    static void wl(String s)
    {
        _out.println(s);
        _out.flush();
    }
    
    static void l()
    {
        _out.println();
        _out.flush();
    }
    
    public static void makeGUI()
    {
        PlainDocument doc = new PlainDocument();
        JFrame frame = new JFrame();
        frame.getContentPane().add(new JScrollPane(new JTextArea(doc)));
        frame.setSize(800, 600);
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.addWindowListener(new WindowAdapter()
        {
            public void windowClosed(WindowEvent we)
            {
                SwingUtilities.invokeLater(new Runnable()
                {
                    public void run()
                    {
                        _out = System.out;
                    }
                });
            }
        });
        _out = new PrintStream(new ConsoleOutputStream(doc, false));
        frame.setVisible(true);
    }
    
    static class ConsoleOutputStream
    extends OutputStream
    {
        private PlainDocument _consoleDocument;
        private StringBuffer _sb = new StringBuffer(100);
        private SimpleAttributeSet _as = new SimpleAttributeSet();
        
        public ConsoleOutputStream(PlainDocument doc, boolean errorStream)
        {
            _consoleDocument = doc;
            _as.addAttribute(PlainDocument.lineLimitAttribute, new Integer(120));
            _as.addAttribute(PlainDocument.tabSizeAttribute, new Integer(8));
            StyleConstants.setFontFamily(_as, "Monospaced");
            StyleConstants.setFontSize(_as, 12);
            StyleConstants.setForeground(_as, errorStream ? Color.red : Color.black);
            StyleConstants.setBackground(_as, Color.white);
        }
        
        public Document getDocument()
        {
            return _consoleDocument;
        }
        
        public void write(int c)
        {
            _sb.append((char)c);
        }
        
        public void write(byte[] bytes, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++)
            {
                _sb.append((char)bytes[i]);
            }
        }
       
        public synchronized void flush()
        {
            if (_sb.length() > 0)
            {
                String s = _sb.toString();
                _sb.setLength(0);
                try
                {
                    int offset = _consoleDocument.getLength();
                    _consoleDocument.insertString(offset, s, _as);
                }
                catch (BadLocationException ble)
                {
                    ble.printStackTrace();
                }
            }
        }
    }

}
