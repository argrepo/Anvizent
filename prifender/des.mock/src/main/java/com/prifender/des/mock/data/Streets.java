package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class Streets
{
    private Streets() {}

    public static String random()
    {
        final StringBuilder buf = new StringBuilder();
        
        buf.append( RNG.nextInt( 30000 ) );
        buf.append( ' ' );
        
        final boolean prefixDirection = RNG.nextBoolean();
        
        if( prefixDirection )
        {
            buf.append( StreetDirections.random() );
            buf.append( ' ' );
        }
        
        if( RNG.nextBoolean() )
        {
            buf.append( StreetNames.random() );
        }
        else
        {
            final int streetNumber = RNG.nextInt( 300 );
            final int lastDigit = streetNumber % 10;
            
            buf.append( streetNumber );
            
            if( lastDigit == 1 )
            {
                buf.append( "st" );
            }
            else if( lastDigit == 2 )
            {
                buf.append( "nd" );
            }
            else if( lastDigit == 3 )
            {
                buf.append( "rd" );
            }
            else
            {
                buf.append( "th" );
            }
        }
        
        buf.append( ' ' );
        buf.append( StreetTypes.random() );
        
        if( ! prefixDirection )
        {
            buf.append( ' ' );
            buf.append( StreetDirections.random() );
        }
        
        return buf.toString();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
