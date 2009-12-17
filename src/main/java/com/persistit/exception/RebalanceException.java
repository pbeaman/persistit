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
package com.persistit.exception;

/**
 * Thrown to signify an internal condition that requires special handling
 * during deletion rebalancing.  This exception should always be caught and
 * handled within the Persistit implementation, and never thrown to application
 * code.
 *
 * @version 1.0
 */
public class RebalanceException
extends PersistitException
{
    private static final long serialVersionUID = 5712813170520119517L;
    
    public final static RebalanceException SINGLETON =
        new RebalanceException("Singleton");
         
    public RebalanceException()
    {
    }
    
    public RebalanceException(String msg)
    {
        super(msg);
    }
}