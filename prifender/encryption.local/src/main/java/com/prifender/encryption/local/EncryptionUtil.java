package com.prifender.encryption.local;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class EncryptionUtil
{
    private static final String CHARACTER_ENCODING = "UTF-8";
    private static final String SECRET_KEY_SPEC = "AES";
    private static final String MESSAGE_DIGEST = "SHA-1";
    private static final String CIPHER = "AES/CBC/PKCS5Padding";
    private static final int IV_SIZE = 16;
    private static final int KEY_SIZE = 128;
    
    private EncryptionUtil() {}
    
    public static final String generateKey() throws GeneralSecurityException
    {
        final KeyGenerator keyGen = KeyGenerator.getInstance( SECRET_KEY_SPEC );
        keyGen.init( KEY_SIZE );
        return Base64.getEncoder().encodeToString( keyGen.generateKey().getEncoded() );
    }
    
    public static final String encrypt( final String key, final String value ) throws GeneralSecurityException
    {
        if( value == null )
        {
            return null;
        }
        
        final byte[] valueBytes;
        
        try
        {
            valueBytes = value.getBytes( CHARACTER_ENCODING );
        }
        catch( final UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
        
        // Generate the IV
        
        final byte[] iv = new byte[ IV_SIZE ];
        final SecureRandom random = new SecureRandom();
        random.nextBytes( iv );
        final IvParameterSpec ivParameterSpec = new IvParameterSpec( iv );

        // Hash the key
        
        final byte[] keyBytes = new byte[ KEY_SIZE / 8 ];
        final MessageDigest md = MessageDigest.getInstance( MESSAGE_DIGEST );
        md.update( key.getBytes() );
        System.arraycopy( md.digest(), 0, keyBytes, 0, keyBytes.length );
        final SecretKeySpec secretKeySpec = new SecretKeySpec( keyBytes, SECRET_KEY_SPEC );

        // Encrypt
        
        final Cipher cipher = Cipher.getInstance( CIPHER );
        cipher.init( Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec );
        final byte[] encrypted = cipher.doFinal( valueBytes );

        // Combine the IV and the encrypted part
        
        final byte[] encryptedIVAndText = new byte[ IV_SIZE + encrypted.length ];
        System.arraycopy( iv, 0, encryptedIVAndText, 0, IV_SIZE );
        System.arraycopy( encrypted, 0, encryptedIVAndText, IV_SIZE, encrypted.length );

        return Base64.getEncoder().encodeToString( encryptedIVAndText );
    }
    
    public static final String decrypt( final String key, final String value ) throws GeneralSecurityException
    {
        if( value == null )
        {
            return null;
        }
        
        final byte[] valueBytes = Base64.getDecoder().decode( value );
        
        // Extract the IV
        
        final byte[] iv = new byte[ IV_SIZE ];
        System.arraycopy( valueBytes, 0, iv, 0, iv.length );
        final IvParameterSpec ivParameterSpec = new IvParameterSpec( iv );

        // Extract the encrypted part
        
        final int encryptedSize = valueBytes.length - IV_SIZE;
        final byte[] encryptedBytes = new byte[ encryptedSize ];
        System.arraycopy( valueBytes, IV_SIZE, encryptedBytes, 0, encryptedSize );
        
        // Hash the key
        
        final byte[] keyBytes = new byte[ KEY_SIZE / 8 ];
        final MessageDigest md = MessageDigest.getInstance( MESSAGE_DIGEST );
        md.update( key.getBytes() );
        System.arraycopy( md.digest(), 0, keyBytes, 0, keyBytes.length );
        final SecretKeySpec secretKeySpec = new SecretKeySpec( keyBytes, SECRET_KEY_SPEC );

        // Decrypt
        
        final Cipher cipher = Cipher.getInstance( CIPHER );
        cipher.init( Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec );
        final byte[] decrypted = cipher.doFinal( encryptedBytes );

        try
        {
            return new String( decrypted, CHARACTER_ENCODING );
        }
        catch( final UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public static void main( final String[] args ) throws Exception
    {
        if( args.length == 1 && args[ 0 ].equals( "-k" ) )
        {
            System.out.println( generateKey() );
        }
        else if( args.length == 3 && args[ 0 ].equals( "-e" ) )
        {
            System.out.println( encrypt( args[ 1 ], args[ 2 ] ) );
        }
        else if( args.length == 3 && args[ 0 ].equals( "-d" ) )
        {
            System.out.println( decrypt( args[ 1 ], args[ 2 ] ) );
        }
        else
        {
            System.out.println( "java -jar [path] -k" );
            System.out.println( "java -jar [path] -e [key] [value]" );
            System.out.println( "java -jar [path] -d [key] [value]" );
        }
    }

}
