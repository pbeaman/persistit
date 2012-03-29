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
    
    private final String _detail;

    public PersistitIOException(IOException ioe) {
        super(ioe);
        _detail = null;
    }

    public PersistitIOException(String msg) {
        super(msg);
        _detail = null;
    }

    public PersistitIOException(String msg, IOException exception) {
        super(exception);
        _detail = msg;
    }

    /**
     * Override default implementation in {@link Throwable#getMessage()} to
     * return the detail message of the wrapped IOException.
     * 
     * @return the detail message string, including that of the cause
     */
    @Override
    public String getMessage() {
        if (getCause() == null) {
            return super.getMessage();
        } else if (_detail == null) {
            return getCause().toString();
        } else {
            return _detail + ":" + getCause().toString();
        }
    }
}
