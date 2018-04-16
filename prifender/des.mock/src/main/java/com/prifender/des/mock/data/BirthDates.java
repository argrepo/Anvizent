package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class BirthDates
{
    private BirthDates() {}
    
    public static String random()
    {
        final StringBuilder buf = new StringBuilder();
        
        buf.append( 1950 + RNG.nextInt( 30 ) );
        buf.append( '-' );
        buf.append( String.format( "%02d", RNG.nextInt( 12 ) + 1 ) );
        buf.append( '-' );
        buf.append( String.format( "%02d", RNG.nextInt( 28 ) + 1 ) );
        
        return buf.toString();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
