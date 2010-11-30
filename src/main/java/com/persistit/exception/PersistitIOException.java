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

import java.io.IOException;

/**
 * This is a wrapper for an {@link IOException}. It is convenient for the caller
 * of a Persistit method to catch {@link PersistitException}s without also
 * needing to catch IOExceptions.
 * 
 * @version 1.0
 */
public class PersistitIOException extends PersistitException {
    private static final long serialVersionUID = 9108205596568958490L;

    public PersistitIOException(IOException ioe) {
        super(ioe);
    }

    public PersistitIOException(String msg) {
        super(msg);
    }

    public PersistitIOException(String msg, Exception exception) {
        super(msg, exception);
    }
}
