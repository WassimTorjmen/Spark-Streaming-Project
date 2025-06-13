#!/bin/bash
set -e
mkdir -p "${ZOO_DATA_DIR}"
echo "tickTime=2000"           >  ${ZOO_CFG_DIR}/zoo.cfg
echo "dataDir=${ZOO_DATA_DIR}" >> ${ZOO_CFG_DIR}/zoo.cfg
echo "clientPort=${ZOO_PORT}"  >> ${ZOO_CFG_DIR}/zoo.cfg
exec /opt/zookeeper/bin/zkServer.sh start-foreground
