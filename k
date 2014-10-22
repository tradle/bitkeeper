#!/bin/bash
java -classpath bin:lib/TomP2P-4.4.jar:lib/logback-core-1.0.1.jar:lib/gson-2.3.jar:lib/netty-3.5.3.Patched.jar:lib/jdbm-3.0-alpha4.jar:lib/slf4j-api-1.6.4.jar:lib/logback-classic-1.0.1.jar io.tradle.bitkeeper.KeeperServer $1
