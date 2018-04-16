package com.prifender.des.mock.pump.relational.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public abstract class RelationalDatabaseClient
{
    private final String url;
    private final String namespace;
    private String nameAndVersion;
    
    public RelationalDatabaseClient( final String expectedUrlPrefix, final String url, final String namespace )
    {
        if( ! url.startsWith( expectedUrlPrefix ) )
        {
            throw new IllegalArgumentException();
        }
        
        this.url = url;
        this.namespace = namespace;
    }
    
    public final Connection connect() throws Exception
    {
        return DriverManager.getConnection( this.url );
    }
    
    public final String url()
    {
        return this.url;
    }
    
    public final String namespace()
    {
        return this.namespace;
    }
    
    public final String databaseNameVersion() throws Exception
    {
        if( this.nameAndVersion == null )
        {
            try( final Connection cn = connect() )
            {
                try( final Statement st = cn.createStatement() )
                {
                    this.nameAndVersion = detectDatabaseNameVersion( st );
                }
            }
        }
        
        return this.nameAndVersion;
    }
    
    protected abstract String detectDatabaseNameVersion( final Statement st ) throws Exception;
    
}
