package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class Salaries
{
    private Salaries() {}
    
    public static int random()
    {
        return 20000 + RNG.nextInt( 200000 );
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
