/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.stress.unit;

import java.security.DigestException;
import java.security.MessageDigest;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Volume;
import com.persistit.stress.AbstractStressTest;
import com.persistit.util.Util;

public class MD5Sum extends AbstractStressTest {
    /**
     * Computes and prints the MD5 sum of all keys and values. This value can be
     * used to test whether the database state is consistent.
     */

    int _size;
    String _opflags;

    private MessageDigest md;

    public MD5Sum(String argsString) {
        super(argsString);
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    @Override
    public void executeTest() throws Exception {
        final Volume volume = getPersistit().getVolume("persistit");
        final String[] treeNames = volume.getTreeNames();
        long pairs = 0;
        for (int index = 0; index < treeNames.length; index++) {
            final Exchange exchange = getPersistit().getExchange(volume, treeNames[index], false);
            exchange.clear().append(Key.BEFORE);
            while (exchange.next(true)) {
                update(md, exchange.getKey().getEncodedBytes(), exchange.getKey().getEncodedSize());
                update(md, exchange.getValue().getEncodedBytes(), exchange.getValue().getEncodedSize());
                pairs++;
            }
        }
    }

    private void update(final MessageDigest md, final byte[] bytes, final int length) throws DigestException {
        md.update(bytes, 0, length);
    }


}
