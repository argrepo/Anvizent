package com.prifender.des.mock.data;

public final class StreetDirections
{
    private static final StringDataSet DATA = new StringDataSet( "StreetDirections.txt" );
    
    private StreetDirections() {}
    
    public static String random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
