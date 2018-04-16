package com.prifender.encryption.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.GeneralSecurityException;

import org.junit.jupiter.api.Test;

public class JsonKeyCatalogStorageTest
{
    @Test
    public void testLoadingAndParsing() throws Exception
    {
        final String key1 = EncryptionUtil.generateKey();
        final String key2 = EncryptionUtil.generateKey();
        
        final KeyCatalogStorage storage = createStorage( "[ { \"version\": 1, \"key\": \"" + key1 + "\" }, { \"version\": 2, \"key\": \"" + key2 + "\" } ]" );
        final KeyCatalog catalog = new KeyCatalog( storage );
        
        assertFalse( catalog.empty() );
        
        final KeyCatalog.VersionedKey latest = catalog.latest();
        
        assertEquals( 2, latest.version() );
        assertEquals( key2, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
    }
    
    @Test
    public void testAddingKey() throws Exception
    {
        final String key1 = EncryptionUtil.generateKey();
        final String key2 = EncryptionUtil.generateKey();
        final String key3 = EncryptionUtil.generateKey();
        
        // Load the catalog
        
        KeyCatalogStorage storage = createStorage( "[ { \"version\": 1, \"key\": \"" + key1 + "\" }, { \"version\": 2, \"key\": \"" + key2 + "\" } ]" );
        KeyCatalog catalog = new KeyCatalog( storage );
        
        assertFalse( catalog.empty() );
        
        // Add a key
        
        catalog.add( key3 );

        KeyCatalog.VersionedKey latest = catalog.latest();
        
        assertEquals( 3, latest.version() );
        assertEquals( key3, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
        assertEquals( key3, catalog.key( 3 ) );
        
        final String storedContentAfterAdd = readStoredContent( storage );
        
        assertTrue( storedContentAfterAdd.contains( key3 ) );
        
        // Load the catalog again and verify that change has been persisted

        storage = createStorage( storedContentAfterAdd );
        catalog = new KeyCatalog( storage );

        latest = catalog.latest();
        
        assertEquals( 3, latest.version() );
        assertEquals( key3, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
        assertEquals( key3, catalog.key( 3 ) );
    }

    @Test
    public void testLoadingEmptyCatalog1() throws Exception
    {
        testLoadingEmptyCatalog( "[]" );
    }
    
    @Test
    public void testLoadingEmptyCatalog2() throws Exception
    {
        testLoadingEmptyCatalog( "" );
    }
    
    @Test
    public void testLoadingEmptyCatalog3() throws Exception
    {
        testLoadingEmptyCatalog( null );
    }
    
    private void testLoadingEmptyCatalog( final String startingContent ) throws Exception
    {
        final String key1 = EncryptionUtil.generateKey();
        final String key2 = EncryptionUtil.generateKey();
        
        // Load the catalog
        
        KeyCatalogStorage storage = createStorage( startingContent );
        KeyCatalog catalog = new KeyCatalog( storage );
        
        assertTrue( catalog.empty() );
        
        // Add a key
        
        catalog.add( key1 );
        catalog.add( key2 );

        KeyCatalog.VersionedKey latest = catalog.latest();
        
        assertEquals( 2, latest.version() );
        assertEquals( key2, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
        
        // Load the catalog again and verify that change has been persisted

        storage = createStorage( readStoredContent( storage ) );
        catalog = new KeyCatalog( storage );

        latest = catalog.latest();
        
        assertEquals( 2, latest.version() );
        assertEquals( key2, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
    }
    
    @Test
    public void testReloadAfterStorageChange() throws Exception
    {
        final String key1 = EncryptionUtil.generateKey();
        final String key2 = EncryptionUtil.generateKey();
        final String key3 = EncryptionUtil.generateKey();
        final String key4 = EncryptionUtil.generateKey();
        
        // Load the catalog
        
        final KeyCatalogStorage storage = createStorage( "[ { \"version\": 1, \"key\": \"" + key1 + "\" }, { \"version\": 2, \"key\": \"" + key2 + "\" } ]" );
        final KeyCatalog catalog = new KeyCatalog( storage );
        
        // Verify the catalog content
        
        assertFalse( catalog.empty() );
        
        KeyCatalog.VersionedKey latest = catalog.latest();
        
        assertEquals( 2, latest.version() );
        assertEquals( key2, latest.key() );
        assertEquals( key1, catalog.key( 1 ) );
        assertEquals( key2, catalog.key( 2 ) );
        
        // Change the stored content and reload the catalog
        
        writeStoredContent( storage, "[ { \"version\": 3, \"key\": \"" + key3 + "\" }, { \"version\": 4, \"key\": \"" + key4 + "\" } ]" );
        catalog.load();
        
        // Verify new catalog content
        
        assertFalse( catalog.empty() );
        
        latest = catalog.latest();
        
        assertEquals( 4, latest.version() );
        assertEquals( key4, latest.key() );
        assertEquals( key3, catalog.key( 3 ) );
        assertEquals( key4, catalog.key( 4 ) );
    }
    
    protected KeyCatalogStorage createStorage( final String json ) throws Exception
    {
        return new MemoryStorage( json );
    }
    
    protected String readStoredContent( final KeyCatalogStorage storage ) throws Exception
    {
        return ( (MemoryStorage) storage ).readJson();
    }
    
    protected void writeStoredContent( final KeyCatalogStorage storage, final String json ) throws Exception
    {
        ( (MemoryStorage) storage ).writeJson( json );
    }

    private static final class MemoryStorage extends JsonKeyCatalogStorage
    {
        private String json;
        
        public MemoryStorage( final String json )
        {
            this.json = json;
        }
        
        @Override
        public String readJson() throws GeneralSecurityException
        {
            return this.json;
        }

        @Override
        public void writeJson( final String json ) throws GeneralSecurityException
        {
            this.json = json;
        }
    }

}
