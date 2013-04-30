/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.stress.unit;

import java.security.DigestException;
import java.security.MessageDigest;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Volume;
import com.persistit.stress.AbstractStressTest;

public class MD5Sum extends AbstractStressTest {
    /**
     * Computes and prints the MD5 sum of all keys and values. This value can be
     * used to test whether the database state is consistent.
     */

    int _size;
    String _opflags;

    private MessageDigest md;

    public MD5Sum(final String argsString) {
        super(argsString);
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (final Exception e) {
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
