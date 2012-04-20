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

import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

/**
 * Interface for objects that perform Persistit&trade; operations within the
 * scope of a {@link Transaction}. Use this interface in conjunction with the
 * {@link Transaction#run} method.
 * 
 * @author peter
 * @version 1.0
 */
public interface TransactionRunnable {
    /**
     * <p>
     * Interface for application logic that is to be run within the scope of a
     * {@link Transaction}. The {@link Transaction#run} method accepts a
     * <code>TransactionRunnable</code> and runs it in a transactional context.
     * </p>
     * <p>
     * The implementation of this method generally should never invoke methods
     * of the enclosing transaction, (which is not supplied). The
     * implementation's application logic <i>may</i> throw a
     * {@link RollbackException} if rollback is necessary due to conditions
     * detected by the application itself. Persistit operations such as
     * {@link Exchange#fetch} and {@link Exchange#store} may also throw
     * <code>RollbackException</code>s when executed within a transaction; the
     * application logic implementing this method should generally not catch
     * <code>PersistitException</code>, including
     * <code>RollbackException</code>.  The the calling code in
     * <code>Transaction</code> is designed to handle them.
     * </p>
     * 
     * @throws PersistitException
     * @throws RollbackException
     */
    public void runTransaction() throws PersistitException, RollbackException;
}
