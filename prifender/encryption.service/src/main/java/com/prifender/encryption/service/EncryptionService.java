package com.prifender.encryption.service;

import java.security.GeneralSecurityException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.prifender.encryption.api.Encryption;

@Component
public final class EncryptionService
{
    @Autowired
    private Encryption encryption;
    
    public void reset() throws GeneralSecurityException
    {
        this.encryption.reset();
    }
    
    public String encrypt( final String value ) throws GeneralSecurityException
    {
        return this.encryption.encrypt( value );
    }
    
    public String decrypt( final String value ) throws GeneralSecurityException
    {
        return this.encryption.decrypt( value );
    }

}
