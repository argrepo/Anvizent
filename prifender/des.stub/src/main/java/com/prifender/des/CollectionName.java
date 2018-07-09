package com.prifender.des;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class CollectionName 
{
    private static final String SEGMENT_SEPARATOR = ".";
    private static final char START_BRACKET = '[';
    private static final char END_BRACKET = ']';
    
    private final String[] segments;
    
    private CollectionName( final String[] segments )
    {
        if( segments == null )
        {
            throw new IllegalArgumentException( "Null name segments array" );
        }
        
        if( segments.length == 0 )
        {
            throw new IllegalArgumentException( "Empty name segments array" );
        }
        
        this.segments = Arrays.copyOf( segments, segments.length );
        
        for( int i = 0, n = this.segments.length; i < n; i++ )
        {
            String segment = this.segments[ i ];
            
            if( segment == null )
            {
                throw new IllegalArgumentException( "Null name segment found" );
            }
            
            segment = segment.trim();
            
            if( segment.length() == 0 )
            {
                throw new IllegalArgumentException( "Empty name segment found" );
            }
            
            this.segments[ i ] = segment;
        }
    }
    
    private CollectionName( final String name )
    {
        if( name == null )
        {
            throw new IllegalArgumentException();
        }
        
        final String trimmedName = name.trim();
        final List<String> result = new ArrayList<String>();
        StringBuilder buf = new StringBuilder();
        boolean insideBrackets = false;
        
        for( int i = 0, n = trimmedName.length(); i < n; i++ )
        {
            final char ch = trimmedName.charAt( i );
            
            if( ! insideBrackets && ch == '.' )
            {
                if( buf.length() == 0 )
                {
                    throw new IllegalArgumentException( name );
                }
                
                result.add( buf.toString() );
                
                buf = new StringBuilder();
            }
            else if( buf.length() == 0 )
            {
                if( ch == START_BRACKET )
                {
                    insideBrackets = true;
                }
                else
                {
                    buf.append( ch );
                }
            }
            else if( insideBrackets && ch == END_BRACKET )
            {
                insideBrackets = false;
            }
            else
            {
                buf.append( ch );
            }
        }
        
        if( buf.length() == 0 || insideBrackets )
        {
            throw new IllegalArgumentException( name );
        }
        
        result.add( buf.toString() );
        
        this.segments = result.toArray( new String[ result.size() ] );
    }
    
    public static CollectionName fromSegments( final String... segments )
    {
        return new CollectionName( segments );
    }
    
    public static CollectionName parse( final String name )
    {
        return new CollectionName( name );
    }
    
    public int depth()
    {
        return this.segments.length;
    }
    
    public String segment( final int index )
    {
        return this.segments[ index ];
    }
    
    public String[] segments()
    {
        return Arrays.copyOf( this.segments, this.segments.length );
    }
    
    @Override
    public int hashCode()
    {
        return Arrays.deepHashCode( this.segments );
    }

    @Override
    public boolean equals( final Object obj )
    {
        if( obj instanceof CollectionName )
        {
            final CollectionName name = (CollectionName) obj;
            return Arrays.deepEquals( this.segments, name.segments );
        }
        
        return false;
    }

    @Override
    public String toString()
    {
        final StringBuilder buf = new StringBuilder();
        
        for( final String segment : this.segments )
        {
            if( buf.length() > 0 )
            {
                buf.append( SEGMENT_SEPARATOR );
            }
            
            if( segment.contains( SEGMENT_SEPARATOR ) )
            {
                buf.append( START_BRACKET );
                buf.append( segment );
                buf.append( END_BRACKET );
            }
            else
            {
                buf.append( segment );
            }
        }
        
        return buf.toString();
    }
    

}
