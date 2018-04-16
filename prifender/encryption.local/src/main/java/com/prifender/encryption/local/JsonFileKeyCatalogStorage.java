package com.prifender.encryption.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;

public class JsonFileKeyCatalogStorage extends JsonKeyCatalogStorage
{
    public static final String ENCODING = "UTF-8";
    
    private final File file;
    
    public JsonFileKeyCatalogStorage( final File file )
    {
        if( file == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.file = file;
    }
    
    File file()
    {
        return this.file;
    }

    @Override
    protected String readJson() throws GeneralSecurityException
    {
        if( this.file.exists() )
        {
            try
            {
                return new String( Files.readAllBytes( this.file.toPath() ), ENCODING );
            }
            catch( final IOException e )
            {
                throw new GeneralSecurityException( e );
            }
        }
        
        return null;
    }
    
    @Override
    protected void writeJson( final String json ) throws GeneralSecurityException
    {
        try
        {
            Files.write( this.file.toPath(), json.getBytes( ENCODING ) );
        }
        catch( final IOException e )
        {
            throw new GeneralSecurityException( e );
        }
    }

}
