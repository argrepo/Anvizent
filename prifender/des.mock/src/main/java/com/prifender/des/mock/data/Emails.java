package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class Emails
{
    private static final StringDataSet DOMAINS = new StringDataSet( "Domains.txt" );
    
    private Emails() {}
    
    public static String random( final String firstName, final String lastName )
    {
        final String firstNameLowerCase = firstName.toLowerCase();
        final String lastNameLowerCase = lastName.toLowerCase();
        final StringBuilder buf = new StringBuilder();
        
        switch( RNG.nextInt( 4 ) )
        {
            case 0:
            {
                buf.append( firstNameLowerCase );
                buf.append( '.' );
                buf.append( lastNameLowerCase );
                
                break;
            }
            case 1:
            {
                buf.append( firstNameLowerCase.charAt( 0 ) );
                buf.append( lastNameLowerCase );
                
                break;
            }
            case 2:
            {
                buf.append( firstNameLowerCase );
                buf.append( lastNameLowerCase.charAt( 0 ) );
                
                break;
            }
            case 3:
            {
                buf.append( firstNameLowerCase );
                buf.append( RNG.nextInt( 1000 ) + 1 );
                
                break;
            }
            default:
            {
                throw new IllegalStateException();
            }
        }
        
        buf.append( '@' );
        buf.append( DOMAINS.random() );
        
        return buf.toString();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random( args[ 0 ], args[ 1 ] ) );
    }

}
