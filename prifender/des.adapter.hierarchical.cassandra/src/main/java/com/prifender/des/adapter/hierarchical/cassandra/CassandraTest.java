package com.prifender.des.adapter.hierarchical.cassandra;

import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraTest{

	private static Cluster cluster;
	private static Session session;

	public static void main(String[] args) {
		
		AuthProvider authProvider = new PlainTextAuthProvider("prifender", "gcEeYUs*ECg8");
	//	AuthProvider authProvider = new PlainTextAuthProvider("cassandra", "cassandra");
		    
		QueryOptions qo = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL);
		
	/*	SocketOptions so =
			       new SocketOptions()
			           .setReadTimeoutMillis(3000)
			           .setConnectTimeoutMillis(3000);
		*/
			           
		 cluster = Cluster.builder()
			       .addContactPoint("52.173.24.49")
			       .withAuthProvider(authProvider)
			       /*.withSocketOptions(so)
			       .withQueryOptions(qo)*/
			       .build();
		
	//	cluster = Cluster.builder().addContactPoint("192.168.0.101").withCredentials("cassandra", "cassandra").build();
		
		session = cluster.connect("cust_data_hr");
		if(session != null){
			
			/*String sql = "SELECT COUNT(*) FROM system.IndexInfo ";
			ResultSet rs = session.execute(sql);*/
			
			/*Metadata metadata = cluster.getMetadata();
			
			
			System.out.println(metadata.getClusterName());
		//	List<KeyspaceMetadata> mt = metadata.getKeyspaces();
			KeyspaceMetadata km = metadata.getKeyspace("system");
		//	for(KeyspaceMetadata km:mt){
				System.out.println("KeySpaceMetadataName : "+km.getName());
				System.out.println("KeySpaceTables"+km.getTables());
				Collection<TableMetadata> tbm = km.getTables();
				for(TableMetadata tm:tbm){
					System.out.println("TableMetadata Name"+tm.getName());
					List<ColumnMetadata>  cmt = tm.getColumns();
					for(ColumnMetadata cm:cmt){
						System.out.println("ColumnMetadata Name: "+cm.getName());
						System.out.println("ColumnMetadata Types: "+cm.getType());
						if(cm.getName().equals("tokens")){
							System.out.println(cm.getTable());
						}
					}
				//}
			}*/
			System.out.println("Hey Connected Successfully");
			//close();
		}

	}

	/*public Session getSession() {
		return this.session;
	}*/

	public static void close() {
		session.close();
		cluster.close();
	}
}
