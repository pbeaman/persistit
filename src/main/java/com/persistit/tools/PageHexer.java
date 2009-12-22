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
 * Created on Mar 10, 2005
 */
package com.persistit.tools;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import com.persistit.Util;
/**
 * @author Peter Beaman
 * @version 1.0
 */
public class PageHexer
{
    final static int BUFFER_SIZE = 8192;
    final static String CRLF = "\r\n";
    
    public static void main(String[] args)
    throws Exception
    {
        boolean fromVolume = false;
        boolean fromFile = false;
        boolean toFile = false;
        boolean toJava = false;
        
        long page = -1;
        String fileName = null;
        
        for (int i = 0; i < args.length; i++)
        {
            String s = args[i];
            if ("-f".equals(s)) fromFile = true;
            else if ("-v".equals(s)) fromVolume = true;
            else if ("-j".equals(s)) toJava = true;
            else if (fromVolume && page == -1)
            {
                try
                {
                    page = Long.parseLong(s);
                }
                catch (NumberFormatException nfe)
                {
                    if (fileName == null) fileName = s;
                }
            }
            else
            {
                if (fromFile || fromVolume) fileName = s; 
            }
        }
        byte[] buffer;
        
        if (fromVolume) buffer = fromRandomAccessFile(page, new RandomAccessFile(fileName, "r"));
        else
        {
            DataInputStream is = null;
            if (fromFile && fileName != null)
            {
                is = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
            }
            else
            {
                System.out.println("Paste the decimal list form of the buffer here:");
                is = new DataInputStream(new BufferedInputStream(System.in));
            }
            buffer = fromTextInputStream(is);
        }
        
        if (toJava)
        {
            toJavaConstant(buffer, System.out);
        }
        else
        {
            System.out.println(Util.dump(buffer, 0, buffer.length));
        }
    }
    
    static byte[] fromRandomAccessFile(long page, RandomAccessFile raf1)
    throws Exception
    {
        raf1.seek(page * BUFFER_SIZE);
        byte[] buffer = new byte[BUFFER_SIZE];
        raf1.readFully(buffer);
        raf1.close();
        return buffer;
    }
    
    static byte[] fromTextInputStream(DataInputStream is)
    throws IOException
    {
        ArrayList list = new ArrayList();
        String line = null;
        for(;;)
        {
            line = is.readLine();
            if (line == null) break;
            for (int p = 0; p < line.length(); p++)
            {
                char c = line.charAt(p);
                if (c == '-' || Character.isDigit(c))
                {
                    int q = p+1;
                    for (; q < line.length() && Character.isDigit(line.charAt(q)); q++)
                    {
                    }
                    String s = line.substring(p, q);
                    p = q - 1;
                    list.add(new Integer(Integer.parseInt(s)));
                }
            }
        }
        byte[] buffer = new byte[list.size()];
        for (int i = 0; i < list.size(); i++)
        {
            buffer[i] = (byte)((Integer)list.get(i)).intValue();
        }
        return buffer;
    }

    static void toJavaConstant(byte[] buffer, PrintStream ps)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("    final static byte[] PAGE_CONTENTS =" + CRLF);
        sb.append("    {" + CRLF);
        for (int index = 0; index < 8192; index++)
        {
            if ((index % 8) == 0) sb.append(CRLF + "        ");
            sb.append("(byte)0x");
            Util.hex(sb, buffer[index], 2);
            sb.append(", ");
        }
        sb.append(CRLF + "    }" + CRLF);
        ps.print(sb.toString());
        ps.close();
        System.out.println(sb);
          
    }
    

}
