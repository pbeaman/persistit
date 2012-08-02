#!/bin/bash
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
# Builds the Akiban Persistit doc set.  Currently this process is based on
# the Sphinx tool (http://sphinx.pocoo.org/).
#
# Here are the steps:
# 1. Run a Java program SphinxDocPrep to prepare a text rst file.
#    Among other things, SphinxDocPrep fills in JavaDoc hyperlinks.
# 2. Run sphinx-build to generate html files.
# 3. Use fold and sed to specially prepare an extra plain text file
#    for the release notes.
#
# Run this script from the doc/build/ directory of the persistit source
# tree. This script writes all output into the ../../target/sphinx 
# directory.
#

APIDOC_INDEX="../../target/site/apidocs/index-all.html"
APIDOC_URL=${APIDOC_URL:-"http://www.akiban.com/sites/all/libraries/persistit-api"}
DOCPREP_CLASS="SphinxDocPrep"
DOCPREP_CLASSPATH="../../target/sphinx/classes:../../target/classes"

DOC_SOURCE_PATH=".."
DOC_TARGET_PATH="../../target/sphinx/source"
DOC_FINAL_PATH="../../target/sphinx/html"
DOC_FILES="\
    ReleaseNotes.rst\
    BasicAPI.rst\
    Configuration.rst\
    GettingStarted.rst\
    Management.rst\
    Miscellaneous.rst\
    PhysicalStorage.rst\
    Security.rst\
    Serialization.rst\
    Transactions.rst\
"

rm -rf ../../target/sphinx/
mkdir -p ../../target/sphinx/{classes,html,source/_static,text}

cp ../index.rst ../conf.py ../../target/sphinx/source

javac -d ../../target/sphinx/classes -cp ../../target/classes/ src/*.java

for f in $DOC_FILES; do
    java -cp "$DOCPREP_CLASSPATH" "$DOCPREP_CLASS" in="${DOC_SOURCE_PATH}/${f}" out="${DOC_TARGET_PATH}/${f}" base="$APIDOC_URL" index="$APIDOC_INDEX"
done

sphinx-build -a "$DOC_TARGET_PATH" "$DOC_FINAL_PATH"

fold -s "${DOC_TARGET_PATH}/ReleaseNotes.rst" | sed -e 's/``//g' -e 's/\.\. note:/NOTE/' -e 's/::/:/' -e 's/^|$//' > ../../target/sphinx/text/ReleaseNotes

