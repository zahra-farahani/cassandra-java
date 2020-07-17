package repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class KeyspaceRepository {

    private Session session;

    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    public void createKeySpace(String keyspaceName, String replicaStrategy, Integer replicaFactor){
        StringBuilder query = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName)
                .append(" WITH replication = {")
                .append("'class':'")
                .append(replicaStrategy)
                .append("','replication_factor':")
                .append(replicaFactor).append("};");

        session.execute(query.toString());
    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + keyspace);
    }

    public void deleteKeyspace(String keyspaceName) {
        StringBuilder query = new StringBuilder("DROP KEYSPACE ").append(keyspaceName);
        session.execute(query.toString());
    }

}
