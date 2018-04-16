package com.prifender.des.mock.pump.relational.client;

import java.sql.ResultSet;
import java.sql.Statement;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile( "MySql" )
public final class MySqlClient extends RelationalDatabaseClient
{
    public static final String JDBC_PREFIX = "jdbc:mysql:";
    
    @Inject
    public MySqlClient( @Value( "${database.url}" ) final String url, @Value( "${database.namespace}" ) final String namespace )
    {
        super( JDBC_PREFIX, url, namespace );
    }

    @Override
    protected String detectDatabaseNameVersion( final Statement st ) throws Exception
    {
        final ResultSet rs = st.executeQuery( "SELECT version()" );
        rs.next();
        return "MySQL " + rs.getString( 1 );
    }

}
