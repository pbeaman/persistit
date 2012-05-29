/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.stress;

import java.security.DigestException;
import java.security.MessageDigest;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Volume;
import com.persistit.test.AbstractTestRunnerItem;
import com.persistit.util.Util;

public class MD5Sum extends AbstractTestRunnerItem {

    private final static String SHORT_DESCRIPTION = "Computes and prints the MD5 sum of all keys and values";

    private final static String LONG_DESCRIPTION = "   Computes and prints the MD5 sum of all keys and values.\n"
            + "This value can be used to test whether the database state is consistent.";

    int _size;
    int _splay;
    String _opflags;

    private MessageDigest md;

    private String progress = "Not started";

    public MD5Sum() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            printStackTrace(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

    }

    @Override
    public void executeTest() throws Exception {
        final Volume volume = _persistit.getVolume("persistit");
        final String[] treeNames = volume.getTreeNames();
        long pairs = 0;
        for (int index = 0; index < treeNames.length; index++) {
            final Exchange exchange = _persistit.getExchange(volume, treeNames[index], false);
            exchange.clear().append(Key.BEFORE);
            while (exchange.next(true)) {
                update(md, exchange.getKey().getEncodedBytes(), exchange.getKey().getEncodedSize());
                update(md, exchange.getValue().getEncodedBytes(), exchange.getValue().getEncodedSize());
                pairs++;
                if (pairs % 10000 == 0) {
                    progress = String.format("%,10d pairs scanned.  Tree %3d of %4d: %s", pairs, index,
                            treeNames.length, treeNames[index]);
                }
            }
        }
        final byte[] digest = md.digest();
        progress = Util.bytesToHex(digest);
    }

    private void update(final MessageDigest md, final byte[] bytes, final int length) throws DigestException {
        md.update(bytes, 0, length);
    }

    @Override
    protected String getProgressString() {
        return progress;
    }

    @Override
    protected double getProgress() {
        // TODO Auto-generated method stub
        return 0;
    }
}
