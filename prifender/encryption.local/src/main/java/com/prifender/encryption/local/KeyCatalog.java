package com.prifender.encryption.local;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public final class KeyCatalog
{
    public static final class VersionedKey
    {
        private final int version;
        private final String key;
        
        public VersionedKey( final int version, final String key )
        {
            this.version = version;
            this.key = key;
        }
        
        public int version()
        {
            return this.version;
        }
        
        public String key()
        {
            return this.key;
        }
    }
    
    private final KeyCatalogStorage storage;
    private NavigableMap<Integer,String> keyByVersion;
    
    public KeyCatalog( final KeyCatalogStorage storage )
    {
        if( storage == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.storage = storage;
    }
    
    public synchronized void load() throws GeneralSecurityException
    {
        try
        {
            this.keyByVersion = this.storage.read();
        }
        catch( final GeneralSecurityException e )
        {
            this.keyByVersion = null;
            throw e;
        }
    }
    
    protected synchronized void loadIfNecessary() throws GeneralSecurityException
    {
        if( this.keyByVersion == null )
        {
            load();
        }
    }
    
    public synchronized boolean empty()
    {
        try
        {
            loadIfNecessary();
        }
        catch( final Exception e ) {}
        
        return this.keyByVersion == null || this.keyByVersion.isEmpty();
    }
    
    public synchronized List<VersionedKey> list() throws GeneralSecurityException
    {
        loadIfNecessary();
        
        final List<VersionedKey> list = new ArrayList<VersionedKey>();
        
        for( final Map.Entry<Integer,String> entry : this.keyByVersion.entrySet() )
        {
            list.add( new VersionedKey( entry.getKey(), entry.getValue() ) );
        }
        
        return list;
    }
    
    public synchronized VersionedKey latest() throws GeneralSecurityException
    {
        loadIfNecessary();
        
        final Map.Entry<Integer,String> latestDekVersion = this.keyByVersion.lastEntry();
        
        if( latestDekVersion == null )
        {
            throw new GeneralSecurityException( "No keys in the catalog" );
        }
        
        return new VersionedKey( latestDekVersion.getKey(), latestDekVersion.getValue() );
    }
    
    public synchronized String key( final int version ) throws GeneralSecurityException
    {
        loadIfNecessary();
        
        final String key = this.keyByVersion.get( version );
        
        if( key == null )
        {
            throw new GeneralSecurityException( "Unknown key version" );
        }
        
        return key;
    }
    
    public synchronized int add( final String key ) throws GeneralSecurityException
    {
        if( key == null )
        {
            throw new IllegalArgumentException();
        }
        
        loadIfNecessary();
        
        final Map.Entry<Integer,String> latest = this.keyByVersion.lastEntry();
        final int version = ( latest == null ? 1 : latest.getKey() + 1 );
        
        this.keyByVersion.put( version, key );
        
        save();
        
        return version;
    }
    
    public synchronized void save() throws GeneralSecurityException
    {
        loadIfNecessary();
        this.storage.write( this.keyByVersion );
    }

}
