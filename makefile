TEST_FILES   := $(shell find test -name *_test.rb -type f)
REDIS_BRANCH := unstable
TMP          := tmp
BUILD_DIR    := ${TMP}/redis-${REDIS_BRANCH}
TARBALL      := ${TMP}/redis-${REDIS_BRANCH}.tar.gz
BINARY       := ${BUILD_DIR}/src/redis-server
REDIS_TRIB   := ${BUILD_DIR}/src/redis-trib.rb
PID_PATH     := ${BUILD_DIR}/redis.pid
SOCKET_PATH  := ${BUILD_DIR}/redis.sock
PORT         := 6381
CLUSTER1_PORT := 7000
CLUSTER2_PORT := 7001
CLUSTER3_PORT := 7002
CLUSTER4_PORT := 7003
CLUSTER5_PORT := 7004
CLUSTER6_PORT := 7005
CLUSTER1_PID_PATH := ${BUILD_DIR}/redis7000.pid
CLUSTER2_PID_PATH := ${BUILD_DIR}/redis7001.pid
CLUSTER3_PID_PATH := ${BUILD_DIR}/redis7002.pid
CLUSTER4_PID_PATH := ${BUILD_DIR}/redis7003.pid
CLUSTER5_PID_PATH := ${BUILD_DIR}/redis7004.pid
CLUSTER6_PID_PATH := ${BUILD_DIR}/redis7005.pid
CLUSTER1_SOCKET_PATH := ${BUILD_DIR}/redis7000.sock
CLUSTER2_SOCKET_PATH := ${BUILD_DIR}/redis7001.sock
CLUSTER3_SOCKET_PATH := ${BUILD_DIR}/redis7002.sock
CLUSTER4_SOCKET_PATH := ${BUILD_DIR}/redis7003.sock
CLUSTER5_SOCKET_PATH := ${BUILD_DIR}/redis7004.sock
CLUSTER6_SOCKET_PATH := ${BUILD_DIR}/redis7005.sock
CLUSTER1_CONF_PATH := ${TMP}/nodes7000.conf
CLUSTER2_CONF_PATH := ${TMP}/nodes7001.conf
CLUSTER3_CONF_PATH := ${TMP}/nodes7002.conf
CLUSTER4_CONF_PATH := ${TMP}/nodes7003.conf
CLUSTER5_CONF_PATH := ${TMP}/nodes7004.conf
CLUSTER6_CONF_PATH := ${TMP}/nodes7005.conf

test: ${TEST_FILES}
	make start
	make start_cluster
	env SOCKET_PATH=${SOCKET_PATH} \
		ruby -v $$(echo $? | tr ' ' '\n' | awk '{ print "-r./" $$0 }') -e ''
	make stop
	make stop_cluster

${TMP}:
	mkdir $@

${TARBALL}: ${TMP}
	wget https://github.com/antirez/redis/archive/${REDIS_BRANCH}.tar.gz -O $@

${BINARY}: ${TARBALL} ${TMP}
	rm -rf ${BUILD_DIR}
	mkdir -p ${BUILD_DIR}
	tar xf ${TARBALL} -C ${TMP}
	cd ${BUILD_DIR} && make

stop:
	(test -f ${PID_PATH} && (kill $$(cat ${PID_PATH}) || true) && rm -f ${PID_PATH}) || true

start: ${BINARY}
	${BINARY}                     \
		--daemonize  yes            \
		--pidfile    ${PID_PATH}    \
		--port       ${PORT}        \
		--unixsocket ${SOCKET_PATH}

stop_cluster: ${BINARY}
	(test -f ${CLUSTER1_PID_PATH} && (kill $$(cat ${CLUSTER1_PID_PATH}) || true) && rm -f ${CLUSTER1_PID_PATH}) || true
	(test -f ${CLUSTER2_PID_PATH} && (kill $$(cat ${CLUSTER2_PID_PATH}) || true) && rm -f ${CLUSTER2_PID_PATH}) || true
	(test -f ${CLUSTER3_PID_PATH} && (kill $$(cat ${CLUSTER3_PID_PATH}) || true) && rm -f ${CLUSTER3_PID_PATH}) || true
	(test -f ${CLUSTER4_PID_PATH} && (kill $$(cat ${CLUSTER4_PID_PATH}) || true) && rm -f ${CLUSTER4_PID_PATH}) || true
	(test -f ${CLUSTER5_PID_PATH} && (kill $$(cat ${CLUSTER5_PID_PATH}) || true) && rm -f ${CLUSTER5_PID_PATH}) || true
	(test -f ${CLUSTER6_PID_PATH} && (kill $$(cat ${CLUSTER6_PID_PATH}) || true) && rm -f ${CLUSTER6_PID_PATH}) || true

start_cluster: ${BINARY}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER1_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER1_PID_PATH}       \
		--port                 ${CLUSTER1_PORT}           \
		--unixsocket           ${CLUSTER1_SOCKET_PATH}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER2_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER2_PID_PATH}       \
		--port                 ${CLUSTER2_PORT}           \
		--unixsocket           ${CLUSTER2_SOCKET_PATH}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER3_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER3_PID_PATH}       \
		--port                 ${CLUSTER3_PORT}           \
		--unixsocket           ${CLUSTER3_SOCKET_PATH}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER4_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER4_PID_PATH}       \
		--port                 ${CLUSTER4_PORT}           \
		--unixsocket           ${CLUSTER4_SOCKET_PATH}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER5_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER5_PID_PATH}       \
		--port                 ${CLUSTER5_PORT}           \
		--unixsocket           ${CLUSTER5_SOCKET_PATH}
	${BINARY}                                           \
		--daemonize            yes                        \
		--appendonly           yes                        \
		--cluster-enabled      yes                        \
		--cluster-config-file  ${CLUSTER6_CONF_PATH}      \
		--cluster-node-timeout 5000                       \
		--pidfile              ${CLUSTER6_PID_PATH}       \
		--port                 ${CLUSTER6_PORT}           \
		--unixsocket           ${CLUSTER6_SOCKET_PATH}
	sed -i -e 's#yes_or_die#p#g' -e 's#def p(#def yes_or_die(#g' ${REDIS_TRIB}
	bundle exec ruby ${REDIS_TRIB} create \
		--replicas 1            \
		127.0.0.1:7000          \
		127.0.0.1:7001          \
		127.0.0.1:7002          \
		127.0.0.1:7003          \
		127.0.0.1:7004          \
		127.0.0.1:7005

clean:
	(test -d ${BUILD_DIR} && cd ${BUILD_DIR}/src && make clean distclean) || true

.PHONY: test start stop start_cluster stop_cluster
