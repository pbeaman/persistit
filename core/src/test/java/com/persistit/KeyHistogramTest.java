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

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class KeyHistogramTest extends PersistitUnitTestCase {

    private String _volumeName = "persistit";

    @Test
    public void testCull() throws Exception {
        final int total = 1000;
        final int samples = 10;
        final KeyHistogram histogram = new KeyHistogram(null, null, null, samples, 1, 1);
        final Key key = new Key((Persistit) null);
        for (int i = 0; i < total; i++) {
            key.to(i);
            histogram.addKeyCopy(key);
        }
        histogram.cull();
        assertEquals(total, histogram.getKeyCount());
        assertEquals(samples, histogram.getSampleSize());
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
    }

    @Test
    public void testGather() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestGather", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            exchange.to(i).store();
        }
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, null, 1);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(samples, histogram.getSampleSize());
    }

    @Test
    public void testUnequalDistribution() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestUnequalDistribution", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            final String which = i % 100 < 10 ? "A" : i % 100 < 90 ? "B" : "C";
            exchange.clear().append(which).append(i).store();
        }
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, null, 0);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(3, histogram.getSampleSize());
    }

    @Test
    public void testFiltered() throws Exception {
        final Exchange exchange = _persistit.getExchange(_volumeName, "HistogramTestTestFiltered", true);
        final int total = 100000;
        final int samples = 10;
        exchange.removeAll();
        for (int i = 0; i < total; i++) {
            exchange.getValue().put(String.format("Record %08d", i));
            exchange.clear().append(i);
            if ((i % 100) == 0) {
                exchange.store();
            }
            exchange.append(i).store();
        }
        final KeyFilter keyFilter = new KeyFilter(new KeyFilter.Term[] { KeyFilter.ALL }, 0, 1);
        final KeyHistogram histogram = exchange.computeHistogram(null, null, samples, 1, keyFilter, 0);
        // for (final KeyCount sample : histogram.getSamples()) {
        // System.out.println(sample);
        // }
        assertEquals(samples, histogram.getSampleSize());
    }

    @Override
    public void runAllTests() throws Exception {
        testCull();
        testGather();
        testUnequalDistribution();
        testFiltered();
    }

}
