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
import java.io.RandomAccessFile;
/**
 * @author Peter Beaman
 * @version 1.0
 */
public class FileCutter
{

    public static void main(String[] args)
    throws Exception
    {
        long from = Long.parseLong(args[2]);
        long size = Long.parseLong(args[3]);
        RandomAccessFile raf1 = new RandomAccessFile(args[0], "r");
        RandomAccessFile raf2 = new RandomAccessFile(args[1], "rw");
        raf1.seek(from);
        raf2.setLength(0);
        byte[] buffer = new byte[1024 * 1024];
        long end = raf1.length();
        if (end > from + size) end = from + size;
        for (long addr = from; addr < end; addr += buffer.length)
        {
            int count = end - addr > buffer.length ? buffer.length : (int)(end - addr);
            raf1.readFully(buffer, 0, count);
            raf2.write(buffer, 0, count);
            System.out.print(".");
        }
        raf1.close();
        raf2.close();
          
    }
}
