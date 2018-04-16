package com.prifender.encryption.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class EncryptionUtilTest
{
    @Test
    public void testEncryptDecrypt() throws Exception
    {
        final String text = EncryptionUtilTest.class.getName();
        final String key = EncryptionUtil.generateKey();
        final String enc = EncryptionUtil.encrypt( key, text );
        
        assertNotEquals( text, enc );
        assertEquals( text, EncryptionUtil.decrypt( key, enc ) );
    }

    @Test
    public void testNonRepeatingEncryption() throws Exception
    {
        final String text = EncryptionUtilTest.class.getName();
        final String key = EncryptionUtil.generateKey();
        final String enc1 = EncryptionUtil.encrypt( key, text );
        final String enc2 = EncryptionUtil.encrypt( key, text );
        
        assertNotEquals( text, enc1 );
        assertNotEquals( enc1, enc2 );
        assertEquals( text, EncryptionUtil.decrypt( key, enc1 ) );
        assertEquals( text, EncryptionUtil.decrypt( key, enc2 ) );
    }
    
    @Test
    public void testNullEncryptValue() throws Exception
    {
        assertNull( EncryptionUtil.encrypt( "abc", null ) );
    }

    @Test
    public void testNullDecryptValue() throws Exception
    {
        assertNull( EncryptionUtil.decrypt( "abc", null ) );
    }

}
