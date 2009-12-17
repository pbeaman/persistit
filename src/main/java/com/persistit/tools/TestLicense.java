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

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;

import com.persistit.Util;


class TestLicense
{
    private final static String CONTROL_BUNDLE = "license";

    private final static String PROP_VERSION = "version";
    private final static String PROP_START = "effective";
    private final static String PROP_END = "expires";
    private final static String PROP_RESTRICTIONS = "restrictions";
    private final static String PROP_MESSAGE = "message";
    private final static String PROP_LICENSEE = "licensee";
    private final static String PROP_CUSTOM1 = "custom1";
    private final static String PROP_CUSTOM2 = "custom2";
    private final static String PROP_CUSTOM3 = "custom3";
    private final static String PROP_CUSTOM4 = "custom4";
    private final static String PROP_SIGNATURE = "signature";
    private final static long HOUR = 3600 * 1000;
    private final static long WEEK = 7 * 24 * HOUR;
    
    private String _algorithm = "MD5withRSA";
    private String _fileName = "License.properties";
    
    private final static byte[] ENCODED_PUBLIC_KEY =
    {
        (byte)0x30, (byte)0x5C, (byte)0x30, (byte)0x0D, (byte)0x06,
        (byte)0x09, (byte)0x2A, (byte)0x86, (byte)0x48, (byte)0x86,
        (byte)0xF7, (byte)0x0D, (byte)0x01, (byte)0x01, (byte)0x01,
        (byte)0x05, (byte)0x00, (byte)0x03, (byte)0x4B, (byte)0x00,
        (byte)0x30, (byte)0x48, (byte)0x02, (byte)0x41, (byte)0x00,
        (byte)0xDD, (byte)0x4E, (byte)0x21, (byte)0xF9, (byte)0x49,
        (byte)0x87, (byte)0x6F, (byte)0x8E, (byte)0xA4, (byte)0xEC,
        (byte)0x06, (byte)0x45, (byte)0xD6, (byte)0xC2, (byte)0x39,
        (byte)0xE6, (byte)0x21, (byte)0x50, (byte)0x62, (byte)0x5A,
        (byte)0xFC, (byte)0x3B, (byte)0xCF, (byte)0x11, (byte)0xA9,
        (byte)0xB7, (byte)0xDE, (byte)0x89, (byte)0x03, (byte)0x98,
        (byte)0xAB, (byte)0x6C, (byte)0x3C, (byte)0xDA, (byte)0xE4,
        (byte)0x19, (byte)0x1F, (byte)0x2F, (byte)0x39, (byte)0x88,
        (byte)0x0C, (byte)0x8E, (byte)0xC2, (byte)0xBF, (byte)0x8E,
        (byte)0x51, (byte)0xF5, (byte)0x60, (byte)0x8B, (byte)0x36,
        (byte)0x3D, (byte)0x38, (byte)0xFA, (byte)0xBE, (byte)0xB5,
        (byte)0x2D, (byte)0xB8, (byte)0x5F, (byte)0x16, (byte)0xB4,
        (byte)0xC0, (byte)0x00, (byte)0xBA, (byte)0xEB, (byte)0x02,
        (byte)0x03, (byte)0x01, (byte)0x00, (byte)0x01,
    };
    
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
    private byte[] _signature;
    private boolean _valid;
    private long _verifyTime;
    
    void initialize()
    {
        try
        {
            Properties properties = new Properties();
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");
            
            String startDateString = properties.getProperty(PROP_START);
            if (startDateString != null)
            {
                _start = sdf.parse(startDateString).getTime();
            }
            
            String endDateString = properties.getProperty(PROP_END);
            if (endDateString != null)
            {
                _end = sdf.parse(endDateString).getTime();
            }
        
            _version = properties.getProperty(PROP_VERSION);
            _licensee = properties.getProperty(PROP_LICENSEE);
            _restrictions = properties.getProperty(PROP_RESTRICTIONS);
            _message = properties.getProperty(PROP_MESSAGE);
            _custom1 = properties.getProperty(PROP_CUSTOM1);
            _custom2 = properties.getProperty(PROP_CUSTOM2);
            _custom3 = properties.getProperty(PROP_CUSTOM3);
            _custom4 = properties.getProperty(PROP_CUSTOM4);
            String signature = properties.getProperty(PROP_SIGNATURE, "");
            _signature = Util.hexToBytes(signature);
            _valid = isValid();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    boolean verifySignature()
    {
        try
        {
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(ENCODED_PUBLIC_KEY);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
    
            Signature sig = Signature.getInstance(_algorithm);
            sig.initVerify(pubKey);
            
            sig.update(Long.toString(_start).getBytes());
            sig.update(Long.toString(_end).getBytes());
            if (_version != null) sig.update(_version.getBytes());
            if (_licensee != null) sig.update(_licensee.getBytes());
            if (_restrictions != null) sig.update(_restrictions.getBytes());
            if (_message != null) sig.update(_message.getBytes());
            if (_custom1 != null) sig.update(_custom1.getBytes());
            if (_custom2 != null) sig.update(_custom2.getBytes());
            return sig.verify(_signature);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return false;
        }
    }
    
    String getProperty(ResourceBundle bundle, String propertyName)
    {
        try
        {
            return bundle.getString(propertyName);
        }
        catch (MissingResourceException mre)
        {
            return null;
        }
    }
    
    String getVersion()
    {
        return _version;
    }
    
    String getMessage()
    {
        return _message;
    }

    String getRestrictions()
    {
        return _restrictions;
    }

    long getStart()
    {
        return _start;
    }

    long getEnd()
    {
        return _end;
    }
    
    String[] getCustom()
    {
        return new String[] {_custom1, _custom2, _custom3, _custom4};
    }

    String getLicensee()
    {
        return _licensee;
    }
    
    boolean isValid()
    {
        long now = System.currentTimeMillis();
        if (now > _verifyTime + HOUR)
        {
            _valid = verifySignature();
            _verifyTime = now;
            if (now + WEEK < _start || now - WEEK > _end)
            {
                _valid = false;
            }
        }
        return _valid;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");
        if (_licensee != null)
        {
            sb.append("licensee="); 
            sb.append(_licensee);
        }
        if (_start != 0 && _start != Long.MIN_VALUE)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append("effective="); 
            sb.append(sdf.format(new Date(_start)));
        }
        if (_end != Long.MAX_VALUE)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append("expiration=");
            sb.append(sdf.format(new Date(_end)));
        }
        if (_restrictions != null)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append("restrictions="); 
            sb.append(_restrictions);
        }
        if (_custom1 != null)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append(_custom1);
        }
        if (_custom2 != null)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append(_custom2);
        }
        if (_custom3 != null)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append(_custom3);
        }
        if (_custom4 != null)
        {
            if (sb.length() > 0) sb.append(", ");
            sb.append(_custom4);
        }
        
        return sb.toString();
    }
    
    public static void main(String[] args)
    throws Exception
    {
        TestLicense tl = new TestLicense();
        if (args.length > 0) tl._fileName = args[0];
        if (args.length > 1) tl._algorithm = args[1];
        tl.initialize();
        System.out.println(tl);
        System.out.println("valid=" + tl.isValid());
    }
}
