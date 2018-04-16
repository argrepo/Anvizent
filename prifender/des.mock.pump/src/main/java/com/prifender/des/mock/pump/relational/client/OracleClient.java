package com.prifender.des.mock.pump.relational.client;

import java.sql.ResultSet;
import java.sql.Statement;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile( "Oracle" )
public final class OracleClient extends RelationalDatabaseClient
{
    public static final String JDBC_PREFIX = "jdbc:oracle:";
    
    @Inject
    public OracleClient( @Value( "${database.url}" ) final String url, @Value( "${database.namespace}" ) final String namespace )
    {
        super( JDBC_PREFIX, url, namespace );
    }

    @Override
    protected String detectDatabaseNameVersion( final Statement st ) throws Exception
    {
        final ResultSet rs = st.executeQuery( "SELECT * FROM V$VERSION" );
        rs.next();
        return rs.getString( 1 );
    }

}
