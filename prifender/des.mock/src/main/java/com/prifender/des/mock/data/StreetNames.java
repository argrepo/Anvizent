package com.prifender.des.mock.data;

public final class StreetNames
{
    private static final StringDataSet DATA = new StringDataSet( "StreetNames.txt" );
    
    private StreetNames() {}
    
    public static String random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
