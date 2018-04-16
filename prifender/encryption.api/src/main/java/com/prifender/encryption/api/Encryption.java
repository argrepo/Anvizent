package com.prifender.encryption.api;

import java.security.GeneralSecurityException;

public interface Encryption
{
    void reset() throws GeneralSecurityException;
    
	String encrypt( String value ) throws GeneralSecurityException;
	
	String decrypt( String value ) throws GeneralSecurityException;
}
