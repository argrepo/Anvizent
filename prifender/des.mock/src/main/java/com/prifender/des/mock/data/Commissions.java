package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class Commissions
{
    private Commissions() {}
    
    public static int random()
    {
        return RNG.nextInt( 31 );
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
