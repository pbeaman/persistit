/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package com.persistit;

import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

/**
 * Interface for objects that perform Persistit&trade; operations within the
 * scope of a {@link Transaction}.  Use this interface in conjunction with
 * the {@link Transaction#run} method.
 * @author pbeaman
 * @version 1.0
 */
public interface TransactionRunnable
{
    /**
     * <p>
     * Interface for application logic that is to be run within the scope of
     * a {@link Transaction}.  The {@link Transaction#run} method accepts a
     * <tt>TransactionRunnable</tt> and runs it in a transactional context.
     * </p>
     * <p>
     * The implementation of this method generally should 
     * never invoke methods of the enclosing transaction, (which
     * is not supplied). The implementation's application
     * logic <i>may</i> throw a {@link RollbackException} if rollback is 
     * necessary due to conditions detected by the application
     * itself.  Persistit operations such as {@link Exchange#fetch} and
     * {@link Exchange#store} may also throw <tt>RollbackException</tt>s when
     * executed within a transaction; the application logic implementing this
     * method should generally not catch <tt>PersistitException<tt>s, including
     * <tt>RollbackException<tt>s.  The the calling code in
     * <tt>Transaction</tt> is designed to handle them.
     * </p> 
     * @throws PersistitException
     * @throws RollbackException
     */
    public void runTransaction()
    throws PersistitException, RollbackException;
}
