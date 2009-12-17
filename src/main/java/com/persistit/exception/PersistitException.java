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
 * The superclass for all checked Persistit&trade; Exceptions.
 *
 * @version 1.0
 */
public abstract class PersistitException
extends Exception
{
    private Exception _exception;
    
    protected PersistitException()
    {
        super();
    }
    
    protected PersistitException(String msg)
    {
        super(msg);
    }
    
    protected PersistitException(Exception exception)
    {
        super();
        _exception = exception;
    }
    
    protected PersistitException(String msg, Exception exception)
    {
        super(msg);
        _exception = exception;
    }
    
    /**
     * Provides an implementation for JDK1.3 and below.  This simply
     * overrides the JDK1.4 implementation of this method.
     */
    public Throwable getCause()
    {
        return _exception;
    }
}
