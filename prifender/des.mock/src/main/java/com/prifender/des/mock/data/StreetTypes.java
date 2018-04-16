package com.prifender.des.mock.data;

public final class StreetTypes
{
    private static final StringDataSet DATA = new StringDataSet( "StreetTypes.txt" );
    
    private StreetTypes() {}
    
    public static String random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
