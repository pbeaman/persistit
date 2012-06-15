#!/bin/bash
#
# Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3 (only) of the
# License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# This program may also be available under different license terms. For more
# information, see www.akiban.com or contact licensing@akiban.com.
#

#
# Build build artifacts associated with a release
#   - Documentation (apidocs and sphinx html, for website)
#       - akiban-persistit-X.X.X-website-docs.tar.gz
#   - Bundles/packages
#       - akiban-persistit-X.X.X.zip                (binary, AGPL)
#       - akiban-persistit-X.X.X.tar.gz             (binary, AGPL)
#       - akiban-persistit-X.X.X-source.zip         (source, AGPL)
#       - akiban-persistit-X.X.X-source.tar.gz      (source, AGPL)
#       - akiban-persistit-community-X.X.X.zip      (binary, EULA)
#       - akiban-persistit-community-X.X.X.tar.gz   (binary, EULA)
#

set -e

# $1 - APIDOC_URL (empty OK)
function docs_build {
    rm -rf target/site/apidocs
    rm -rf target/sphinx
    mvn javadoc:javadoc >/dev/null
    cd doc/build
    APIDOC_URL="$1" bash -e build-doc.sh >/dev/null
    cd ../../
    rm -r target/sphinx/html/{.buildinfo,.doctrees,objects.inv}
}

# $1 - revno
# $2 - args to maven
function maven_build {
    mvn $2 -DBZR_REVISION="$1" -DskipTests=true clean compile test-compile package >/dev/null
}


REQUIRED_PROGS="bzr mvn javac sphinx-build curl awk sed tr basename zip tar gpg"
BRANCH_DEFAULT="lp:~akiban-technologies/akiban-persistit"
COMM_LICENSE_URL="http://www.akiban.com/akiban-persistit-community-license-agreement-plaintext"

VERSION=""
BRANCH_URL=""
WORKSPACE="/tmp/persistit_release"

while getopts "hb:v:w:" FLAG; do
    case "${FLAG}" in
        h) ;;
        b) BRANCH="${OPTARG}" ;;
        v) VERSION="${OPTARG}" ;;
        w) WORKSPACE="${OPTARG}" ;;
        *) echo "Unhandled option" 1>&2 ; exit 1 ;;
    esac
done

if [ "${VERSION}" = "" ]; then
    echo "Missing required version arg -v" 1>&2
    exit 1
fi

if [ "${BRANCH}" = "" ]; then
    BRANCH_URL="${BRANCH_DEFAULT}/${VERSION}"
fi


echo "Build packages for version: ${VERSION}"
echo "Use source branch: ${BRANCH_URL}"
echo "Use workspace location: ${WORKSPACE}"


echo "Checking for required programs"
for PROG in ${REQUIRED_PROGS}; do
    if [ "$(which ${PROG})" = "" ]; then
        echo "    ${PROG} not found in PATH" 1>&2
        exit 1
    fi
done


NAME="akiban-persistit"
BRANCH_DIR="${WORKSPACE}/${VERSION}"
SOURCE_DIR="${WORKSPACE}/${NAME}-${VERSION}-source"
OPEN_DIR="${WORKSPACE}/${NAME}-${VERSION}"
COMM_DIR="${WORKSPACE}/${NAME}-community-${VERSION}"
WEBDOCS_DIR="${WORKSPACE}/${NAME}-${VERSION}-website-docs"


echo "Cleaning workspace ${WORKSPACE}"
rm -rf "${WORKSPACE}"
mkdir -p "${WORKSPACE}"
cd "${WORKSPACE}"


echo "Fetching revision number"
REVNO=$(bzr revno -q "${BRANCH_URL}")
echo "Revision $REVNO"


echo "Exporting branch"
bzr export -q "${BRANCH_DIR}" "${BRANCH_URL}"


echo "Making package directories"
cp -r "${BRANCH_DIR}" "${SOURCE_DIR}"
cp -r "${BRANCH_DIR}" "${OPEN_DIR}"
rm -r "${OPEN_DIR}"/{doc,examples/scripts,src,pom.xml}
mkdir "${OPEN_DIR}/doc"
cp -r "${OPEN_DIR}" "${COMM_DIR}"


echo "Building open edition and docs"
cd "${BRANCH_DIR}"
maven_build "${REVNO}"
docs_build "../apidocs"


echo "Copying docs and jars"
cd "${WORKSPACE}"
cp -r "${BRANCH_DIR}"/target/{site/apidocs,sphinx/html} "${OPEN_DIR}/doc"
cp "${BRANCH_DIR}/target/${NAME}-${VERSION}${REVNO}.jar" "${OPEN_DIR}/${NAME}-${VERSION}.jar"
cp "${BRANCH_DIR}/target/${NAME}-${VERSION}${REVNO}-sources.jar" "${OPEN_DIR}/${NAME}-${VERSION}-sources.jar"


echo "Downloading and formating community license"
cd "${WORKSPACE}"
curl -s "${COMM_LICENSE_URL}" |
    # Pull out the content between the two regexes, excluding the matches themselves
    awk '/<div class="content">/ {flag=1;next} /<\/div>/ {flag=0} flag {print}' |
    # Replace paragraph end marks for the first 4 paragraphs with newlines
    awk '{if(NR < 8) sub(/<\/p>/, "\n"); print }' |
    # Delete all: <p>, </p>, </div>, and &nbsp; occurrences
    sed -e 's/<p>//g' -e 's/<\/p>//g' -e 's/<\/div>//g' -e 's/&nbsp;//g' |
    # Replace unicode quotes with simple ones
    sed -e 's/[“”]/"/g' -e "s/’/'/g" |
    # Un-link email address(es)
    sed -e 's/<a href=".*">//g' -e 's/<\/a>//g' |
    # Collapse repeated spaces
    tr -s ' ' |
    # Wrap nicely at 80 characters 
    fold -s \
    > "${COMM_DIR}/LICENSE.txt"


echo "Building community edition and docs"
cd "${BRANCH_DIR}"
cp "${COMM_DIR}/LICENSE.txt" .
awk 'BEGIN { FS="\n"; RS="";}\
    {sub(/[ ]*<licenses>.*<\/licenses>/,\
    "<licenses>\n<license>\n<name>Proprietary</name>\n<url>http://www.akiban.com/akiban-persistit-community-license-agreement</url>\n<distribution>manual</distribution>\n</license>\n</licenses>\n"); print;}'\
    pom.xml > pom_comm.xml
maven_build "${REVNO}" "-f pom_comm.xml"
docs_build ""
cp "target/${NAME}-${VERSION}${REVNO}.jar" "${COMM_DIR}/${NAME}-${VERSION}.jar"


echo "Creatio g zip and tar.gz files"
cd "${WORKSPACE}"
for DIR in "${OPEN_DIR}" "${SOURCE_DIR}" "${COMM_DIR}"; do
    BASE_DIR="`basename ${DIR}`"
    zip -r "${DIR}.zip" "$BASE_DIR" >/dev/null
    tar czf "${DIR}.tar.gz" "${BASE_DIR}"
done


echo "Building docs for website"
mkdir "${WEBDOCS_DIR}"
cd "${BRANCH_DIR}"
docs_build ""
cp -r target/{site/apidocs,sphinx/html} "${WEBDOCS_DIR}"
cd ..
tar czf "${WEBDOCS_DIR}.tar.gz" "${WEBDOCS_DIR}"


if [ "$SKIP_SIGNING" = "" ]; then
    echo "Signing files for Launchpad upload"
    for FILE in `ls *.zip *.tar.gz`; do
        gpg --armor --sign --detach-sig "${FILE}"
    done
fi


echo "All output files are in: ${WORKSPACE}"
echo "Done"

