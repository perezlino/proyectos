#!/bin/bash
rm /tmp/*.pid


service ssh start  # <----- Es redundante este comando, ya que este comando ya fue ejecutado en la imagen base
                   #        Sin embargo, igual lo dejaré.
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh 

bash