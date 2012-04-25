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

package com.persistit.unit;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * Test simple scenario to ensure a second Persistit instance referring to the
 * same volumes can't start.
 * 
 * @author peter
 * 
 */
public class FileLockTest extends PersistitUnitTestCase {

    @Test
    public void testOverlap() throws Exception {
        final Properties properties = _persistit.getProperties();
        final Persistit p2 = new Persistit();
        try {
            p2.initialize(properties);
            fail("Created second Persistit instance");
        } catch (PersistitException pe) {
            // success - we intended to fail
        } finally {
            p2.close(false);
        }
        //
        _persistit.close(false);
        final Persistit p3 = new Persistit();
        // now this should succeed.
        try {
            p3.initialize(properties);
        } finally {
            p3.close(false);
        }
    }

    @Override
    public void runAllTests() throws Exception {
        testOverlap();
    }

}
