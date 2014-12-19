#!/bin/bash

COLOR_PROGRESS='\e[1;34m'
COLOR_FAIL='\e[1;31m'
COLOR_RESET='\e[0m' # No Color

function fail {
    echo -e "${COLOR_FAIL}ERROR: $1${COLOR_RESET}"
    exit 1
}

function error {
    echo -e "${COLOR_FAIL}ERROR: $1${COLOR_RESET}"
}

function progress {
    echo -e "${COLOR_PROGRESS}$1..${COLOR_RESET}"
}

echo "                                                                                                                                 ";
echo "88888888ba     ,ad8888ba,     ,ad8888ba,    88                                                                                   ";
echo "88      \"8b   d8\"'    \`\"8b   d8\"'    \`\"8b   88                                                                                   ";
echo "88      ,8P  d8'            d8'        \`8b  88                                                                                   ";
echo "88aaaaaa8P'  88             88          88  88,dPPYba,   ,adPPYba,   ,adPPYba,  8b,dPPYba,  8b       d8   ,adPPYba,  8b,dPPYba,  ";
echo "88\"\"\"\"\"\"'    88      88888  88          88  88P'    \"8a  I8[    \"\"  a8P_____88  88P'   \"Y8  \`8b     d8'  a8P_____88  88P'   \"Y8  ";
echo "88           Y8,        88  Y8,        ,8P  88       d8   \`\"Y8ba,   8PP\"\"\"\"\"\"\"  88           \`8b   d8'   8PP\"\"\"\"\"\"\"  88          ";
echo "88            Y8a.    .a88   Y8a.    .a8P   88b,   ,a8\"  aa    ]8I  \"8b,   ,aa  88            \`8b,d8'    \"8b,   ,aa  88          ";
echo "88             \`\"Y88888P\"     \`\"Y8888Y\"'    8Y\"Ybbd8\"'   \`\"YbbdP\"'   \`\"Ybbd8\"'  88              \"8\"       \`\"Ybbd8\"'  88          ";
echo "                                                                                                                                 ";
echo "                                                                                                                                 ";

progress 'Checking prerequisites'
git --version > /dev/null || fail "git is required"

MAVEN_VERSION=$(mvn --version | head -n 1 | grep -o '3\.')
[ "v$MAVEN_VERSION" = "v3." ] || fail "Maven 3 is required"

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | grep -o '1\.[78]')
[ "v$JAVA_VERSION" = "v1.8" -o "v$JAVA_VERSION" = "v1.7" ] || fail "Java 1.7 or 1.8 is required"

PYTHON_VERSION=$(python --version 2>&1 | grep -o '2\.7')
[ "v$PYTHON_VERSION" = "v2.7" ] || fail "Python 2.7 is required"

progress 'Building Gatherer'
(cd gatherer && mvn clean package && docker build -t pgobserver-gatherer .)

progress 'Building Frontend'
(cd frontend && docker build -t pgobserver-frontend .)