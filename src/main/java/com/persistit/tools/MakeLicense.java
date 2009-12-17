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
 * Created on Aug 9, 2004
 */
package com.persistit.tools;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.persistit.Util;

/**
 *
 * @version 1.0
 */
public class MakeLicense
{
    public final static String PROP_VERSION = "version";
    public final static String PROP_START = "effective";
    public final static String PROP_END = "expires";
    public final static String PROP_RESTRICTIONS = "restrictions";
    public final static String PROP_MESSAGE = "message";
    public final static String PROP_LICENSEE = "licensee";
    public final static String PROP_CUSTOM1 = "custom1";
    public final static String PROP_CUSTOM2 = "custom2";
    public final static String PROP_CUSTOM3 = "custom3";
    public final static String PROP_CUSTOM4 = "custom4";
    public final static String PROP_SIGNATURE = "signature";

    private final static long WEEK = 7 * 24 * 3600 * 1000;
    
    private final static String[] PROPERTIES =
    {
        PROP_LICENSEE,
        PROP_VERSION,
        PROP_START,
        PROP_END,
        PROP_MESSAGE,
        PROP_RESTRICTIONS,
        PROP_CUSTOM1,
        PROP_CUSTOM2,
        PROP_CUSTOM3,
        PROP_CUSTOM4,
        PROP_SIGNATURE,
    };
    
    private final static SimpleDateFormat SDF = 
        new SimpleDateFormat("yyyy-MMM-ddZ");
    
    private final static SimpleDateFormat SDF1 = 
        new SimpleDateFormat("yyyy-MMM-dd");
    
    private final static SimpleDateFormat SDF2 = 
        new SimpleDateFormat("yyyyMMddHHmmss");
    
    private final static String JKS_FILE_NAME = "MakeLicense.jks";
    
    private static boolean _fromMain = false;
    private static boolean _verbose = false;

    private String _version;
    private long _start = Long.MIN_VALUE;
    private long _end = Long.MAX_VALUE;
    private String _restrictions;
    private String _message;
    private String _licensee;
    private String _custom1;
    private String _custom2;
    private String _custom3;
    private String _custom4;
    private boolean _valid;
    
    
    void createLicense(Properties properties)
    throws Exception
    {
        String startDateString = properties.getProperty(PROP_START);
        if (startDateString != null)
        {
            _start = SDF.parse(startDateString + "-0000").getTime();
        }
        
        String endDateString = properties.getProperty(PROP_END);
        if (endDateString != null)
        {
            _end = SDF.parse(endDateString + "-0000").getTime();
        }

        _version = properties.getProperty(PROP_VERSION);
        _licensee = properties.getProperty(PROP_LICENSEE);
        _restrictions = properties.getProperty(PROP_RESTRICTIONS);
        _message = properties.getProperty(PROP_MESSAGE);
        _custom1 = properties.getProperty(PROP_CUSTOM1);
        _custom2 = properties.getProperty(PROP_CUSTOM2);
        _custom3 = properties.getProperty(PROP_CUSTOM3);
        _custom4 = properties.getProperty(PROP_CUSTOM4);
        
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(
            _fromMain 
            ? new FileInputStream(JKS_FILE_NAME)
            : this.getClass().getClassLoader().getResourceAsStream(JKS_FILE_NAME),
            null);
        PrivateKey privateKey =
            (PrivateKey)keyStore.getKey("persistit-license", "dylian".toCharArray());
        
        Signature sig = Signature.getInstance("MD5withRSA");
        sig.initSign(privateKey);

        sig.update(Long.toString(_start).getBytes());
        sig.update(Long.toString(_end).getBytes());
        if (_version != null) sig.update(_version.getBytes());
        if (_licensee != null) sig.update(_licensee.getBytes());
        if (_restrictions != null) sig.update(_restrictions.getBytes());
        if (_message != null) sig.update(_message.getBytes());
        if (_custom1 != null) sig.update(_custom1.getBytes());
        if (_custom2 != null) sig.update(_custom2.getBytes());
        
        byte[] signature = sig.sign();
        
        properties.setProperty(PROP_SIGNATURE, Util.bytesToHex(signature));
    }
    
    public void save(Properties properties, String fileName)
    throws IOException
    {
        
        save(properties, new FileOutputStream(fileName, false));
    }
    
    public void save(Properties properties, OutputStream outputStream)
    {
        PrintWriter writer = new PrintWriter(outputStream);
        for (int i = 0; i < PROPERTIES.length; i++)
        {
            String value = properties.getProperty(PROPERTIES[i]);
            if (value != null)
            {
                writer.println(PROPERTIES[i] + " = " + value);
            }
        }
        writer.flush();
    }
    
    
    public static void rewriteKit(
        InputStream fromStream, 
        OutputStream toStream, 
        Properties licenseProperties,
        String modTime)
    throws Exception
    {
        byte[] buffer = new byte[32768];
        ZipInputStream zis = new ZipInputStream(fromStream);
        ZipOutputStream zos = new ZipOutputStream(toStream);
        long time = -1;
        if (modTime != null && modTime.length() > 0)
        {
            time = SDF2.parse(modTime).getTime();
            String myDate = new Date(time).toString();
        }
        rewriteKit(zis, zos, buffer, licenseProperties, time, 0);
        zis.close();
        zos.close();
    }
    
    static void rewriteKit(
        ZipInputStream zis,
        ZipOutputStream zos,
        byte[] buffer,
        Properties licenseProperties,
        long modificationTime,
        int level)
    throws Exception
    {
        for (;;)
        {
            ZipEntry entry = zis.getNextEntry();
            if (entry == null) break;
            String name = entry.getName();
            int method = entry.getMethod();
            long size = entry.getSize();
            long compressed = entry.getCompressedSize();
            long time = entry.getTime();
            if (_verbose)
            {
                for (int count = level; --count >= 0;)
                {
                    System.out.print("  ");
                }
                System.out.print(
                    name + "  method=" + method + "  size=" + size + 
                    "  compressed=" + compressed + "  time=" + time);
            }
            ZipEntry entry2 = new ZipEntry(name);
            entry2.setTime(modificationTime >= 0 ? modificationTime : time);
            zos.putNextEntry(entry2);

            if (name.indexOf("lib/persistit") >= 0 && name.endsWith(".jar"))
            {
                ZipInputStream libis = new ZipInputStream(zis);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ZipOutputStream libos = new ZipOutputStream(baos);
                libos.setMethod(8);
                if (_verbose)
                {
                    System.out.println();
                }
                rewriteKit(libis, libos, buffer, licenseProperties, modificationTime, level + 1);
                baos.close();
                zos.write(baos.toByteArray(), 0, baos.size());
                zos.flush();
            }
            else if ("license.properties".equals(name))
            {
                MakeLicense ml = new MakeLicense();
                ml.createLicense(licenseProperties);
                ml.save(licenseProperties, zos);
                if (_verbose)
                {
                    System.out.print(" - rewrote license.properties");
                }
            }
            else if (!entry.isDirectory())
            {
                int length = 0;
                int offset;
                for (offset = 0; ; offset += length)
                {
                    length = zis.read(buffer);
                    if (length >= 0)
                    {
                        zos.write(buffer, 0, length);
                    }
                    else break;
                }
                zos.flush();
                if (_verbose)
                {
                    System.out.print(
                        "  length=" + offset + 
                        "  recompressed=" + entry.getCompressedSize());
                }
            }
            else
            {
                if (_verbose)
                {
                    System.out.print(" - skipped directory entry");
                }
            }
            if (_verbose)
            {
                System.out.println();
            }
        }
        
        zis.closeEntry();
        zos.close();
    }
    
    
    public static String licenseDate(long days)
    {
        long delta = days * 86400 * 1000;
        Date date = new Date(System.currentTimeMillis() + delta);
        return SDF1.format(date);
    }

    
    public static void main(String[] args)
    throws Exception
    {
        _fromMain = true;
        String from = args.length > 0 ? args[0] : "license.properties";
        String to = args.length > 1 ? args[1] : "build/classes/license.properties";
        String modTime = args.length > 2 ? args[2] : null;
        
        MakeLicense ml = new MakeLicense();
        Properties properties = new Properties();
        System.out.println("MakeLicense loading properties from " + from);
        properties.load(new FileInputStream(from));
        
        if (to.endsWith(".zip") || to.endsWith(".jar"))
        {
            FileInputStream fis = new FileInputStream(to);
            FileOutputStream fos = new FileOutputStream(to + "_licensed");
            System.out.println("MakeLicense writing kit: " + to + " to: " + to + "_licensed");
            rewriteKit(fis, fos, properties, modTime);
        }
        else
        {
            ml.createLicense(properties);
            System.out.println("MakeLicense saving properties to: " + to);
            ml.save(properties, to);
        }
    }
}
