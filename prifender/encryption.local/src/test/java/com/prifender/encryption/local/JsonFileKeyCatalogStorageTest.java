package com.prifender.encryption.local;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.UUID;

public class JsonFileKeyCatalogStorageTest extends JsonKeyCatalogStorageTest
{
    @Override
    protected KeyCatalogStorage createStorage( final String json ) throws Exception
    {
        final File file = File.createTempFile( UUID.randomUUID().toString(), null );
        
        file.deleteOnExit();
        
        if( json == null )
        {
            file.delete();
            assertFalse( file.exists() );
        }
        else
        {
            writeFile( file, json );
        }
        
        return new JsonFileKeyCatalogStorage( file );
    }
    
    @Override
    protected String readStoredContent( final KeyCatalogStorage storage ) throws Exception
    {
        return readFile( ( (JsonFileKeyCatalogStorage) storage ).file() );
    }
    
    @Override
    protected void writeStoredContent( final KeyCatalogStorage storage, final String json ) throws Exception
    {
        writeFile( ( (JsonFileKeyCatalogStorage) storage ).file(), json );
    }
    
    protected static final String readFile( final File file ) throws Exception
    {
        try
        {
            return new String( Files.readAllBytes( file.toPath() ), JsonFileKeyCatalogStorage.ENCODING );
        }
        catch( final IOException e )
        {
            throw new GeneralSecurityException( e );
        }
    }

    protected static final void writeFile( final File file, final String json ) throws Exception
    {
        try
        {
            Files.write( file.toPath(), json.getBytes( JsonFileKeyCatalogStorage.ENCODING ) );
        }
        catch( final IOException e )
        {
            throw new GeneralSecurityException( e );
        }
    }

}
