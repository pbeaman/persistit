/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.stress.unit;

import com.persistit.exception.PersistitException;

public class Stress4b extends Stress4Base {

    public Stress4b(String argsString) {
        super(argsString);
    }

    @Override
    public void repeatedTasks() throws PersistitException {
        writeRecords(_total, false, 1000, 1000);
        readRecords(_total, false, 1000, 1000);
        testForward();
        testReverse();
        testReads(_total);
        seed(_seed);
        writeRecords(_total, true, 20, 1000);
        seed(_seed);
        readRecords(_total, true, 20, 1000);
        seed(_seed * 7);
        writeRecords(_total, true, 20, 8000);
        seed(_seed * 7);
        readRecords(_total, true, 20, 8000);
        testReverse();
        seed(_seed * 7);
        writeRecords(_total / 200, true, 20, 80000);
        seed(_seed * 7);
        readRecords(_total / 200, true, 20, 80000);
        testReverse();
        seed(_seed * 17);
        removeRecords(_seed, true);
        testForward();
    }

}
