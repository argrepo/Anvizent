package com.prifender.encryption.service.client;

import java.security.GeneralSecurityException;

import org.springframework.stereotype.Component;

import com.prifender.encryption.api.Encryption;

@Component
public final class EncryptionServiceClient implements Encryption
{
    private final DefaultApi api;

    public EncryptionServiceClient()
    {
        this.api = new DefaultApi( new ApiClient().setBasePath( readFromEnv( "ENCRYPTION_SERVICE" ) ) );
    }

    @Override
    public void reset() throws GeneralSecurityException
    {
        try
        {
            this.api.reset();
        }
        catch( final ApiException e )
        {
            throw new GeneralSecurityException( e );
        }
    }
    
    @Override
	public String encrypt( final String value ) throws GeneralSecurityException
	{
	    if( value == null )
	    {
	        return null;
	    }
	    
        try
        {
            return this.api.encrypt( value );
        }
        catch( final ApiException e )
        {
            throw new GeneralSecurityException( e );
        }
	}

    @Override
	public String decrypt( final String value ) throws GeneralSecurityException
    {
        if( value == null )
        {
            return null;
        }
        
        try
        {
            return this.api.decrypt( value );
        }
        catch( final ApiException e )
        {
            throw new GeneralSecurityException( e );
        }
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

    public static void main( final String[] args ) throws Exception
    {
        final Encryption encryption = new EncryptionServiceClient();
        
        if( args.length == 1 && args[ 0 ].equals( "-r" ) )
        {
            encryption.reset();
        }
        else if( args.length == 2 && args[ 0 ].equals( "-e" ) )
        {
            System.out.println( encryption.encrypt( args[ 1 ] ) );
        }
        else if( args.length == 2 && args[ 0 ].equals( "-d" ) )
        {
            System.out.println( encryption.decrypt( args[ 1 ] ) );
        }
        else
        {
            System.out.println( "java -jar [path] -r" );
            System.out.println( "java -jar [path] -e [content]" );
            System.out.println( "java -jar [path] -d [content]" );
        }
    }

}
