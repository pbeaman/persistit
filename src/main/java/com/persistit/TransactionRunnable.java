/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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
     * <code>PersistitException</code>, including <code>RollbackException</code>
     * . The the calling code in <code>Transaction</code> is designed to handle
     * them.
     * </p>
     * 
     * @throws PersistitException
     * @throws RollbackException
     */
    public void runTransaction() throws PersistitException, RollbackException;
}
