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
 * 
 * Created on Mar 1, 2004
 */
package com.persistit.exception;

/**
 * This is a wrapper for any {@link Exception} that occurs while initializing 
 * the logging subsystem.  It is frequently convenient for the caller of a
 * Persistit method to catch {@link PersistitException}s
 * without also needing to catch <tt>IOException</tt>s or other 
 * <tt>Exception</tt> subclasses.  For compatibility with
 * earlier J2SE releases this is implemented without using JDK 1.4
 * Exception chaining.
 * 
 * @version 1.0
 */
public class LogInitializationException
extends PersistitException
{
    private static final long serialVersionUID = -3253500224779009799L;
    
    public LogInitializationException(Exception e)
    {
        super(e);
    }
}
