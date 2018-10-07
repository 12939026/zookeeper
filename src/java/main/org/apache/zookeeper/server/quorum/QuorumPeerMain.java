/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.Properties;

import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            System.exit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            System.exit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        LOG.info("Exiting normally");
        System.exit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException, AdminServerException
    {
    	//将配置文件解析成QuorumPeerConfig实体
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        // 启动日志自动清理，需要配置，默认不启动
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        //根据配置文件判断是集群环境还是单机环境
        if (args.length == 1 && config.isDistributed()) {//集群
            runFromConfig(config);
        } else {//单机
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config)
            throws IOException, AdminServerException
    {
      //注册log4j的mbean，用于jconsole之类的监控MBEAN
      try {
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }

      LOG.info("Starting quorum peer");
      //监控信息，和单机版一样
      MetricsProvider metricsProvider;
      try {
        metricsProvider = MetricsProviderBootstrap
                      .startMetricsProvider(config.getMetricsProviderClassName(),
                                            config.getMetricsProviderConfiguration());
      } catch (MetricsProviderLifeCycleException error) {
        throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(),
                      error);
      }
      try {

          ServerCnxnFactory cnxnFactory = null;
          ServerCnxnFactory secureCnxnFactory = null;

          //创建一个处理IO连接的组件cnxnFactory，这里和单机版不一样的地方是它不调用startup方法，具体的启动要
          //等在后面的选主逻辑完成后，确定了自己的身份再启动
          if (config.getClientPortAddress() != null) {
              cnxnFactory = ServerCnxnFactory.createFactory();
              cnxnFactory.configure(config.getClientPortAddress(),
                      config.getMaxClientCnxns(),
                      false);
          }

          //https的支持版本，注意，默认实现的NIO模式不支持
          if (config.getSecureClientPortAddress() != null) {
              secureCnxnFactory = ServerCnxnFactory.createFactory();
              secureCnxnFactory.configure(config.getSecureClientPortAddress(),
                      config.getMaxClientCnxns(),
                      true);
          }
          //直接调用new返回一个QuorumPeer
          quorumPeer = getQuorumPeer();
          //前面设置的监控组件，实际上never uesed?
          quorumPeer.setRootMetricsContext(metricsProvider.getRootContext());
          //日志（事务日志和数据快照）的操作类
          quorumPeer.setTxnFactory(new FileTxnSnapLog(
                      config.getDataLogDir(),
                      config.getDataDir()));
          //zzz:本地session？ 暂不明白是干啥用的
          quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
          quorumPeer.enableLocalSessionsUpgrading(
              config.isLocalSessionsUpgradingEnabled());
          //quorumPeer.setQuorumPeers(config.getAllMembers());
          //选举算法，现在基本都是使用3 FastLeaderElection
          quorumPeer.setElectionType(config.getElectionAlg());
          //myid文件里指定的服务器id
          quorumPeer.setMyid(config.getServerId());
          //最小时间单位 ticktime
          quorumPeer.setTickTime(config.getTickTime());
          //客户端的最小和最大超时时间，用于对客户端设置的超时时间进行限制
          //（若客户端设置的时间不在这个范围内，会被强制调整到范围里，默认2-20倍的ticktime）
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
          //leader等待follower启动的最大时间，默认ticktime10倍
          quorumPeer.setInitLimit(config.getInitLimit());
          //心跳检测的最大时间，默认ticktime5倍
          quorumPeer.setSyncLimit(config.getSyncLimit());
          //配置文件的地址
          quorumPeer.setConfigFileName(config.getConfigFilename());
          //内存数据库
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
          //集群信息的内存存储类
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
          if (config.getLastSeenQuorumVerifier()!=null) {
              quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
          }
          //初始化内存数据库
          quorumPeer.initConfigInZKDatabase();
          //注入cnxnFactory
          quorumPeer.setCnxnFactory(cnxnFactory);
          //https版本
          quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
          //是否有权投票，PARTICIPANT（默认）可以投票，OBSERVER不能投票
          quorumPeer.setLearnerType(config.getPeerType());
          //zzz:判断observer是否同步的属性名称(咱不理解什么意思)
          quorumPeer.setSyncEnabled(config.getSyncEnabled());
          //Zookeeper服务器是否监听所有可用IP地址的连接，默认false，
          //zzz:暂时不知道干啥用的，好像是会影响运维部署方面的
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());

          // sets quorum sasl authentication configurations
          // zzz:权限相关配置，后面在研究
          quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
          if(quorumPeer.isQuorumSaslAuthEnabled()){
              quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
              quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
              quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
              quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
              quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
          }
          //连接管理线程数大小
          quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
          // 权限管理类，默认不使用
          quorumPeer.initialize();
          //调用start方法启动线程，start方法重写过，有些其他逻辑，最终还是调用的Thread.start
          quorumPeer.start();  
          //等待线程的结束，线程结束表示服务器停止或者出异常了
          quorumPeer.join();   
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      } finally {
          if (metricsProvider != null) {
              try {
                  metricsProvider.stop();
              } catch (Throwable error) {
                  LOG.warn("Error while stopping metrics", error);
              }
          }
      }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}
