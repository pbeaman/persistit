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
 * Created on Sep 6, 2004
 */
package com.persistit.ui;

import java.rmi.RemoteException;
import java.util.Map;

import javax.swing.JPanel;

public abstract class AdminPanel extends JPanel {

    protected abstract void setup(AdminUI ui) throws NoSuchMethodException,
            RemoteException;

    protected abstract void refresh(boolean reset) throws RemoteException;

    protected abstract Map getMenuMap();

    protected abstract void setDefaultButton();

    protected void setIsShowing(boolean isShowing) {
    }
}
