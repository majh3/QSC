#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mvn install:install-file \
  -Dfile="$SCRIPT_DIR/anyburl-23.1.jar" \
  -DgroupId=de.unima.ki.anyburl \
  -DartifactId=anyburl \
  -Dversion=23.1 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile="$SCRIPT_DIR/kiabora-1.2.0.jar" \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=kiabora \
  -Dversion=1.2.0 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile="$SCRIPT_DIR/pure-rewriter-1.1.0.jar" \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=pure-rewriter \
  -Dversion=1.1.0 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile="$SCRIPT_DIR/graal-api-1.3.1.jar" \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=api \
  -Dversion=1.3.1 \
  -Dpackaging=jar