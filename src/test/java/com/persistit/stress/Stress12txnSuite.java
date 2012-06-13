/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import com.persistit.Persistit;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.stress.unit.Stress12txn;

public class Stress12txnSuite extends AbstractSuite {

    static String name() {
        return Stress12txnSuite.class.getSimpleName();
    }

    public static void main(String[] args) throws Exception {
        new Stress12txnSuite(args).runTest();
    }

    public Stress12txnSuite(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {

        deleteFiles(substitute("$datapath$/persistit*"));

        add(new Stress12txn("repeat=50 count=250000 size=100 seed=1"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=2"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=3"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=4"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=5"));
        add(new Stress12txn("repeat=50 count=250000 size=100 seed=6"));

        final Persistit persistit = makePersistit(16384, "20K", CommitPolicy.SOFT);

        try {
            execute(persistit);
        } finally {
            persistit.close();
        }
    }
}
