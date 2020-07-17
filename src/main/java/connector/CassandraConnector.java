package connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassandraConnector {

    private Cluster cluster;
    private Session session;

    public void connect(String node, Integer port){
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        session = cluster.connect();
    }

    public Session getSession(){
        return session;
    }

    public void close(){
        session.close();
        cluster.close();
    }
}
