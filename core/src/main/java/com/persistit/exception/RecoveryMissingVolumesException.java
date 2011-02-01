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
 * Created on May 7, 2004
 */
package com.persistit.exception;

/**
 * Thrown during recovery processing when the prewrite journal contains recovery
 * data for one or more Volumes that no longer exist.
 * 
 * @version 1.0
 */
public class RecoveryMissingVolumesException extends PersistitException {
    private static final long serialVersionUID = -9042109367136062128L;

    public RecoveryMissingVolumesException() {
        super();
    }

    public RecoveryMissingVolumesException(String msg) {
        super(msg);
    }

}
