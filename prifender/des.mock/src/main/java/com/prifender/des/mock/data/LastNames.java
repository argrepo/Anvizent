package com.prifender.des.mock.data;

public final class LastNames
{
    private LastNames() {}
    
    public static String random()
    {
        return StreetNames.random();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
