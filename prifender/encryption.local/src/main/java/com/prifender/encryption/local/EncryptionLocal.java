package com.prifender.encryption.local;

import java.io.File;
import java.security.GeneralSecurityException;

import org.springframework.stereotype.Component;

import com.prifender.encryption.api.Encryption;

@Component
public final class EncryptionLocal implements Encryption
{
    private static final String KEK_PATH_VAR = "KEK_PATH";
    private static final String DEK_PATH_VAR = "DEK_PATH";
    
    private final KeyCatalog catalog;
    
    public EncryptionLocal() throws GeneralSecurityException
    {
        this( new JsonFileEncryptedKeyCatalogStorage( new File( readFromEnv( KEK_PATH_VAR ) ), new File( readFromEnv( DEK_PATH_VAR ) ) ) );
    }
    
    public EncryptionLocal( final KeyCatalogStorage keyCatalogStorage ) throws GeneralSecurityException
    {
        if( keyCatalogStorage == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.catalog = new KeyCatalog( keyCatalogStorage );
        
        if( this.catalog.empty() )
        {
            this.catalog.add( EncryptionUtil.generateKey() );
        }
    }
    
    @Override
    public void reset() throws GeneralSecurityException
    {
        this.catalog.load();
    }

    @Override
    public String encrypt( final String value ) throws GeneralSecurityException
    {
        if( value == null )
        {
            return null;
        }
        
        final KeyCatalog.VersionedKey latest = this.catalog.latest();
        
        return latest.version() + "#" + EncryptionUtil.encrypt( latest.key(), value );
    }

    @Override
    public String decrypt( final String value ) throws GeneralSecurityException
    {
        if( value == null )
        {
            return null;
        }
        
        final int separator = value.indexOf( '#' );
        
        if( separator == -1 || separator == value.length() - 1 )
        {
            throw new GeneralSecurityException( "Invalid version encoding" );
        }
        
        final int keyVersion;
        
        try
        {
            keyVersion = Integer.parseInt( value.substring( 0, separator ) );
        }
        catch( final NumberFormatException e )
        {
            throw new GeneralSecurityException( e );
        }
        
        final String key = this.catalog.key( keyVersion );
        
        return EncryptionUtil.decrypt( key, value.substring( separator + 1 ) );
    }

    protected static final String readFromEnv( final String var )
    {
        if( var == null )
        {
            throw new IllegalArgumentException();
        }
        
        final String value = System.getenv( var );
        
        if( value == null || value.length() == 0 )
        {
            throw new RuntimeException( "Expecting env variable " + var );
        }
        
        return value;
    }

}
