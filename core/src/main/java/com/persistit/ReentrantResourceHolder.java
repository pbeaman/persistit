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

import com.persistit.exception.PersistitInterruptedException;
import com.persistit.util.Debug;

/**
 * A container for a SharedResource that experiences reentrant claims (locks).
 * {@link Exchange} uses this to hold a Tree object, which experiences this
 * pattern. The purpose is to keep a count of so that when an Exchange has
 * finished using the Tree, it can verify that all claims were released. This is
 * primarily for debugging; correct code will always release each claim.
 */

class ReentrantResourceHolder {

    private final SharedResource _resource;
    private int _claimCount;

    ReentrantResourceHolder(final SharedResource r) {
        _resource = r;
    }

    void verifyReleased() {
        Debug.$assert0.t(_claimCount == 0);
        _claimCount = 0;
    }

    boolean claim(final boolean writer) throws PersistitInterruptedException {
        return claim(writer, SharedResource.DEFAULT_MAX_WAIT_TIME);
    }

    boolean claim(final boolean writer, final long timeout) throws PersistitInterruptedException {
        if (_claimCount == 0) {
            if (!_resource.claim(writer, timeout)) {
                return false;
            } else {
                _claimCount++;
                return true;
            }
        } else {
            if (writer && !_resource.isWriter()) {
                if (!_resource.upgradeClaim()) {
                    return false;
                }
            }
            _claimCount++;
            return true;
        }
    }

    void release() {
        if (_claimCount <= 0) {
            throw new IllegalStateException("This thread holds no claims");
        }
        if (--_claimCount == 0) {
            _resource.release();
        }
    }

    boolean upgradeClaim() {
        return _resource.upgradeClaim();
    }
}
