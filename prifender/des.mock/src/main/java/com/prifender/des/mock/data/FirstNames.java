package com.prifender.des.mock.data;

public final class FirstNames
{
    private static final StringDataSet DATA = new StringDataSet( "FirstNames.txt" );
    
    private FirstNames() {}
    
    public static String random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
