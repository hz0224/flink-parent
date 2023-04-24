//package org.apache.flink.leader_election;
//
//import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
//import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
//import org.apache.flink.runtime.leaderelection.LeaderContender;
//import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionDriverFactory;
//import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
//
//import javax.security.auth.login.Configuration;
//import java.util.UUID;
//
//import static org.apache.flink.runtime.util.ZooKeeperUtils.startCuratorFramework;
//
///**
// * Author：奈学教育
// * Description：
// */
//public class LeaderElectionDemo {
//
//    public static void main(String[] args) throws Exception {
//        // 请开始你的表演！
//        Configuration configuration = ;
//
//        CuratorFramework client = startCuratorFramework(configuration);
//        String latchPath = "/nx-flink/latch";
//        String leaderPath = "/nx-flink/leader";
//
//
//        ZooKeeperLeaderElectionDriverFactory zooKeeperLeaderElectionDriverFactory =
//                new ZooKeeperLeaderElectionDriverFactory(client, latchPath, leaderPath);
//
//        DefaultLeaderElectionService defaultLeaderElectionService = new DefaultLeaderElectionService(zooKeeperLeaderElectionDriverFactory);
//
//        defaultLeaderElectionService.start(new MyLeaderContender());
//    }
//}
//
//class MyLeaderContender implements LeaderContender{
//
//    @Override
//    public void grantLeadership(UUID leaderSessionID) {
//        System.out.println("回调逻辑");
//    }
//
//    @Override
//    public void revokeLeadership() {
//
//    }
//
//    @Override
//    public void handleError(Exception exception) {
//
//    }
//}