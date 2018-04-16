package com.prifender.encryption.local;

import java.io.IOException;
import java.io.StringWriter;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

public abstract class JsonKeyCatalogStorage implements KeyCatalogStorage
{
    private static final String ATTR_VERSION = "version";
    private static final String ATTR_KEY = "key";
    
    protected abstract String readJson() throws GeneralSecurityException;
    
    @Override
    public NavigableMap<Integer,String> read() throws GeneralSecurityException
    {
        final String catalogText = readJson();
        final TreeMap<Integer,String> catalog = new TreeMap<>();
        
        if( catalogText != null && ! catalogText.isEmpty() )
        {
            final JsonArray catalogParsed = new JsonParser().parse( catalogText ).getAsJsonArray();
            
            for( final JsonElement entry : catalogParsed )
            {
                final JsonObject entryObject = entry.getAsJsonObject();
                final int version = entryObject.get( ATTR_VERSION ).getAsInt();
                final String key = entryObject.get( ATTR_KEY ).getAsString();
                
                catalog.put( version, key );
            }
        }
        
        return catalog;
    }
    
    protected abstract void writeJson( String json ) throws GeneralSecurityException;

    @Override
    public void write( final Map<Integer,String> catalog ) throws GeneralSecurityException
    {
        final StringWriter sw = new StringWriter();
        
        try
        {
            final JsonWriter writer = new JsonWriter( sw );
            
            writer.beginArray();
            
            for( final Map.Entry<Integer,String> entry : catalog.entrySet() )
            {
                writer.beginObject();
                writer.name( ATTR_VERSION ).value( entry.getKey() );
                writer.name( ATTR_KEY ).value( entry.getValue() );
                writer.endObject();
            }
            
            writer.endArray();
            writer.close();
        }
        catch( final IOException e )
        {
            throw new GeneralSecurityException( e );
        }
        
        writeJson( sw.toString() );
    }

}
