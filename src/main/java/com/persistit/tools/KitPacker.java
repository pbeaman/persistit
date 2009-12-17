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
 * Created on Apr 18, 2005
 */
package com.persistit.tools;
import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class KitPacker
{

    public static void main(String[] args)
    throws Exception
    {
        Properties properties = new Properties();
        properties.load(new FileInputStream("license_rewriter.properties"));
        MakeLicense.rewriteKit(
            new FileInputStream("c:/source/persistit/persistit_jsa110_138.20050415.zip"), 
            new FileOutputStream("c:/temp/kitpacker_test.zip"), 
            properties,
            "20050415013800");
    }
    
    static void emitKit(
        String from, 
        String to, 
        Properties licenseProperties,
        long modificationTime)
    throws IOException
    {
        OutputStream toStream = new FileOutputStream(to);
        ZipFile inputFile = new ZipFile(from);
        byte[] buffer = new byte[32768];
        ZipOutputStream zos = new ZipOutputStream(toStream);
        
        for (Enumeration files = inputFile.entries();
             files.hasMoreElements(); )
        {
            ZipEntry entry = (ZipEntry)files.nextElement();
            String name = entry.getName();
            int method = entry.getMethod();
            long size = entry.getSize();
            long compressed = entry.getCompressedSize();
            long time = entry.getTime();
            System.out.print(
                name + "  method=" + method + "  size=" + size + 
                "  compressed=" + compressed + "  time=" + time);
            if (modificationTime >= 0) time = modificationTime;
            InputStream is = inputFile.getInputStream(entry);
            
            if ("lib/persistit_jsa110.jar".equals(name) ||
                "lib/persistit_jst110.jar".equals(name))
            {
                ZipInputStream libis = new ZipInputStream(is);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipOutputStream libos = new ZipOutputStream(baos);
                libos.setMethod(8);
                emitLibrary(libis, libos, buffer, modificationTime);
                baos.close();
                ZipEntry entry2 = new ZipEntry(name);
                entry2.setTime(time);
                zos.putNextEntry(entry2);
                zos.write(baos.toByteArray(), 0, baos.size());
                zos.flush();
            }
            else if (!entry.isDirectory())
            {
                //zos.putNextEntry(entry);
                ZipEntry entry2 = new ZipEntry(name);
                zos.putNextEntry(entry2);
                int length = 0;
                int offset;
                for (offset = 0; ; offset += length)
                {
                    length = is.read(buffer);
                    if (length >= 0)
                    {
                        zos.write(buffer, 0, length);
                    }
                    else break;
                }
                System.out.println(
                    "  length=" + offset + 
                    "  recompressed=" + entry.getCompressedSize());
            }
            is.close();
        }
        zos.close();
        inputFile.close();
    }
    
    static void emitLibrary(
        ZipInputStream libis, 
        ZipOutputStream libos, 
        byte[] buffer, 
        long modificationTime)
    throws IOException
    {
        for (;;)
        {
            ZipEntry entry = (ZipEntry)libis.getNextEntry();
            if (entry == null) break;
            if (!entry.isDirectory())
            {
                System.out.println();
                System.out.print(
                    "  " + entry.getName() +
                    "  size=" + entry.getSize() + 
                    "  compressed=" + entry.getCompressedSize());
                ZipEntry entry2 = new ZipEntry(entry.getName());
                if (modificationTime >= 0)
                {
                    entry2.setTime(modificationTime);
                }
                else
                {
                    entry2.setTime(entry.getTime());
                }
                libos.putNextEntry(new ZipEntry(entry.getName()));
                int length = 0;
                int offset;
                for (offset = 0; ; offset += length)
                {
                    length = libis.read(buffer);
                    if (length >= 0)
                    {
                        libos.write(buffer, 0, length);
                    }
                    else break;
                }
                System.out.print(
                    "  resized=" + entry.getSize() + 
                    "  recompressed=" + entry.getCompressedSize() + 
                    "  length=" + offset);
            }
            libos.flush();
            libis.closeEntry();
        }
        System.out.println();
//        libos.close();
//        libis.close();
    }
}
