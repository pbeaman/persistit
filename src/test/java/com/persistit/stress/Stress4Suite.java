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
import com.persistit.stress.unit.Stress4a;
import com.persistit.stress.unit.Stress4b;

public class Stress4Suite extends AbstractSuite {

    static String name() {
        return Stress4Suite.class.getSimpleName();
    }
    
    public static void main(String[] args) throws Exception {
        new Stress4Suite(args).runTest();
    }

    private Stress4Suite(final String[] args) {
        super(name(), args);
    }

    public void runTest() throws Exception {
        
        if (isUntilStopped()) {
            setDuration(getDuration() / 2);
        }
        
        for (int pageSize = 8192; pageSize <= 16384; pageSize *= 2) {

            System.out.printf("Starting %s for page size %,d\n", Stress4Suite.class.getSimpleName(), pageSize);

            deleteFiles(substitute("$datapath$/persistit*"));

            add(new Stress4a("count=1000000"));
            add(new Stress4b("count=1000000"));

            final Persistit persistit = makePersistit(pageSize, "10000", CommitPolicy.SOFT);

            try {
                execute(persistit);
            } finally {
                persistit.close();
            }
            
            clear();
        }
    }
}
