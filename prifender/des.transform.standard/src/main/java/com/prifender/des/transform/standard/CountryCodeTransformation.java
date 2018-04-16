package com.prifender.des.transform.standard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;

public final class CountryCodeTransformation implements Transformation
{
    private static final Pattern THE_PATTERN_START = Pattern.compile( "^the ", Pattern.CASE_INSENSITIVE );
    private static final Pattern THE_PATTERN_MIDDLE = Pattern.compile( " the ", Pattern.CASE_INSENSITIVE );
    
    private final Map<String,String> lookup = new TreeMap<String,String>( String.CASE_INSENSITIVE_ORDER );
    
    public CountryCodeTransformation() throws IOException
    {
        try( final BufferedReader reader = new BufferedReader( new InputStreamReader( CountryCodeTransformation.class.getResourceAsStream( "/countries.csv" ), "UTF-8" ) ) )
        {
            for( String line = reader.readLine(); line != null; line = reader.readLine() )
            {
                final String trimmedLine = line.trim();
                
                if( trimmedLine.length() > 0 )
                {
                    final String[] record = trimmedLine.split( ";" );
                    
                    if( record.length == 4 )
                    {
                        final String code = record[ 1 ];
                        
                        this.lookup.put( record[ 0 ], code );
                        this.lookup.put( record[ 2 ], code );
                        this.lookup.put( code, code );
                    }
                }
            }
        }
    }

    @Override
    public String getName()
    {
        return "CountryCode";
    }

    @Override
    public String getDescription()
    {
        return "Converts a country name or an ISO 3166 alpha-3 country code into ISO 3166 alpha-2 country code. ";
    }

    @Override
    public Object transform( final Object object, final Object[] args ) throws TransformationException
    {
        if( object == null )
        {
            return null;
        }
        
        if( ! ( object instanceof String ) )
        {
            throw new TransformationException( "The CountryCode transformation requires a String input, but received " + object.getClass().getName() );
        }
        
        String str = (String) object;
        str = THE_PATTERN_START.matcher( str ).replaceAll( "" );
        str = THE_PATTERN_MIDDLE.matcher( str ).replaceAll( " " );
        
        return this.lookup.get( str );
    }

}
