package com.prifender.encryption.local;

import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.NavigableMap;

public interface KeyCatalogStorage
{
    NavigableMap<Integer,String> read() throws GeneralSecurityException;
    
    void write( Map<Integer,String> catalog ) throws GeneralSecurityException;

}
