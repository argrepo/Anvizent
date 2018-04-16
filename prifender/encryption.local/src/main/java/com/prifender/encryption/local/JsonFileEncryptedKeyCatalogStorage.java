package com.prifender.encryption.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;

public final class JsonFileEncryptedKeyCatalogStorage extends JsonFileKeyCatalogStorage
{
    private final File kekFile;
    
    public JsonFileEncryptedKeyCatalogStorage( final File kekFile, final File dekCatalogFile )
    {
        super( dekCatalogFile );
        
        if( kekFile == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.kekFile = kekFile;
    }
    
    String readKek() throws GeneralSecurityException
    {
        if( ! this.kekFile.exists() )
        {
            final File dekFile = file();
            
            if( dekFile.exists() )
            {
                throw new GeneralSecurityException( "KEK file does not exist, but DEK file does exist!" );
            }
            
            try
            {
                Files.write( this.kekFile.toPath(), EncryptionUtil.generateKey().getBytes( ENCODING ) );
            }
            catch( final IOException e )
            {
                throw new GeneralSecurityException( e );
            }
        }
        
        try
        {
            return new String( Files.readAllBytes( this.kekFile.toPath() ), ENCODING );
        }
        catch( final IOException e )
        {
            throw new GeneralSecurityException( e );
        }
    }

    @Override
    protected String readJson() throws GeneralSecurityException
    {
        return EncryptionUtil.decrypt( readKek(), super.readJson() );
    }
    
    @Override
    protected void writeJson( final String json ) throws GeneralSecurityException
    {
        super.writeJson( EncryptionUtil.encrypt( readKek(), json ) );
    }

}
