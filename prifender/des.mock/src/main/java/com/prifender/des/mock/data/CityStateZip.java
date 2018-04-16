package com.prifender.des.mock.data;

public final class CityStateZip
{
    public static final class Entry
    {
        public final String city;
        public final String state;
        public final String zip;
        
        public Entry( final String city, final String state, final String zip )
        {
            final String[] cityWords = city.split( " " );
            final StringBuilder cityBuffer = new StringBuilder();
            
            for( int i = 0, n = cityWords.length; i < n; i++ )
            {
                if( i > 0 )
                {
                    cityBuffer.append( ' ' );
                }
                
                final String word = cityWords[ i ];
                
                cityBuffer.append( word.substring( 0, 1 ).toUpperCase() );
                
                if( word.length() > 1 )
                {
                    cityBuffer.append( word.substring( 1 ).toLowerCase() );
                }
            }
            
            this.city = cityBuffer.toString();
            this.state = state;
            this.zip = zip;
        }
    }
    
    private static final DataSet<Entry> DATA = new DataSet<Entry>( "CityStateZip.txt" )
    {
        @Override
        protected Entry parse( final String line )
        {
            final String[] segments = line.split( ";" );
            return new Entry( segments[ 0 ], segments[ 1 ], segments[ 2 ] );
        }
    };
    
    public CityStateZip() {}
    
    public static Entry random()
    {
        return DATA.random();
    }
    
    public static void main( final String[] args )
    {
        final Entry entry = random();
        System.err.println( entry.city + ", " + entry.state + " " + entry.zip );
    }

}
