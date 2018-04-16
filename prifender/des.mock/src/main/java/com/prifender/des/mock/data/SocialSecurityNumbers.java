package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

import java.util.regex.Pattern;

public final class SocialSecurityNumbers
{
    // The SSN validation pattern is based on information at https://en.wikipedia.org/wiki/Social_Security_number#Valid_SSNs
    
    private static final Pattern PATTERN = Pattern.compile( "^(?!(000|666|9))\\d{3}-(?!00)\\d{2}-(?!0000)\\d{4}$" );
    
    private SocialSecurityNumbers() {}
    
    private static int digit()
    {
        return RNG.nextInt( 10 );
    }
    
    public static String random()
    {
        String ssn = null;
        
        while( ssn == null || ! PATTERN.matcher( ssn ).matches() )
        {
            final StringBuilder buf = new StringBuilder();
            
            buf.append( digit() );
            buf.append( digit() );
            buf.append( digit() );
            buf.append( '-' );
            buf.append( digit() );
            buf.append( digit() );
            buf.append( '-' );
            buf.append( digit() );
            buf.append( digit() );
            buf.append( digit() );
            buf.append( digit() );
            
            ssn = buf.toString();
        }
        
        return ssn;
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
