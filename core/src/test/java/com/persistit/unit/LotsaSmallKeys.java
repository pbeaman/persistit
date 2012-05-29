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

package com.persistit.unit;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.policy.SplitPolicy;

public class LotsaSmallKeys extends PersistitUnitTestCase {

    private String _volumeName = "persistit";

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
