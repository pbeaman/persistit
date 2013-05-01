/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
