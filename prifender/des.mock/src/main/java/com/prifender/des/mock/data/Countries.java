package com.prifender.des.mock.data;

public final class Countries
{
    private static final WeightedDataSet<String> DATA = new WeightedDataSet<String>( "Countries.txt" )
    {
        @Override
        protected WeightedDataSet.Entry<String> parse( final String line )
        {
            final String[] segments = line.split( ";" );
            return new WeightedDataSet.Entry<String>( segments[ 0 ], Integer.valueOf( segments[ 1 ] ) );
        }
    };
    
    private Countries() {}
    
    public static String random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        if( args.length == 1 && args[ 0 ].equals( "-t" ) )
        {
            DATA.testDistribution();
        }
        else
        {
            System.err.println( random() );
        }
    }

}
