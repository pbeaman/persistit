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

package com.persistit.unit;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;

public class LotsaSmallKeys extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    /**
     * Tests bug 775752. Logic to limit the key count was missing in the delete
     * case. This caused an AIOOBE in Buffer#recomputeFindex.
     * 
     * @throws PersistitException
     */
    @Test
    public void testInsertAndDeleteSmallRecords() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "LotsOfSmallKeys", true);
        exchange.removeAll();
        exchange.setSplitPolicy(SplitPolicy.LEFT_BIAS);
        exchange.getValue().clear();
        for (int i = 0; i < 20000; i++) {
            exchange.clear().append(i).store();
        }
        for (int j = 0; j < 100; j++) {
            for (int i = j; i < 20000; i += 100) {
                exchange.clear().append(i).remove();
            }
        }
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    @Override
    public void runAllTests() throws Exception {
        testInsertAndDeleteSmallRecords();
    }

}
