package com.prifender.encryption.local;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.util.UUID;

public final class JsonFileEncryptedKeyCatalogStorageTest extends JsonFileKeyCatalogStorageTest
{
    @Override
    protected KeyCatalogStorage createStorage( final String json ) throws Exception
    {
        final File kekFile = File.createTempFile( "test", null );
        final String kek = EncryptionUtil.generateKey();
        
        writeFile( kekFile, kek );
        kekFile.deleteOnExit();
        
        final File file = File.createTempFile( UUID.randomUUID().toString(), null );
        
        file.deleteOnExit();
        
        if( json == null )
        {
            file.delete();
            assertFalse( file.exists() );
        }
        else
        {
            writeFile( file, EncryptionUtil.encrypt( kek, json ) );
        }
        
        return new JsonFileEncryptedKeyCatalogStorage( kekFile, file );
    }
    
    @Override
    protected String readStoredContent( final KeyCatalogStorage storage ) throws Exception
    {
        final JsonFileEncryptedKeyCatalogStorage st = (JsonFileEncryptedKeyCatalogStorage) storage;
        final String kek = st.readKek();
        final String encryptedKeyCatalog = readFile( st.file() );
        final String decryptedKeyCatalog = EncryptionUtil.decrypt( kek, encryptedKeyCatalog );
        
        return decryptedKeyCatalog;
    }
    
    @Override
    protected void writeStoredContent( final KeyCatalogStorage storage, final String json ) throws Exception
    {
        final JsonFileEncryptedKeyCatalogStorage st = (JsonFileEncryptedKeyCatalogStorage) storage;
        final String kek = st.readKek();
        final String encryptedKeyCatalog = EncryptionUtil.encrypt( kek, json );
        
        writeFile( st.file(), encryptedKeyCatalog );
    }

}
