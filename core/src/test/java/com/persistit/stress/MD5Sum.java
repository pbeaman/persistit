/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
