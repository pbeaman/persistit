#
# Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
# 
# This program and the accompanying materials are made available
# under the terms of the Eclipse Public License v1.0 which
# accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# 
# This program may also be available under different license terms.
# For more information, see www.akiban.com or contact licensing@akiban.com.
# 
# Contributors:
# Akiban Technologies, Inc.
#

#
# Invokes the com.persisit.ManagementMXBean#execute method using jmxterm.  See
# http://www.cyclopsgroup.org/projects/jmxterm/.
#
java -jar jmxterm-1.0-alpha-4-uber.jar --verbose silent --noninteract --url $1 <<EOF
run -d com.persistit -b com.persistit:type=Persistit execute $2
EOF


