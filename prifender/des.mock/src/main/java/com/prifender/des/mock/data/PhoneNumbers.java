package com.prifender.des.mock.data;

import static com.prifender.des.mock.data.DataSet.RNG;

public final class PhoneNumbers
{
    private PhoneNumbers() {}
    
    private static int digit()
    {
        return RNG.nextInt( 10 );
    }
    
    private static int digitNonZero()
    {
        return RNG.nextInt( 9 ) + 1;
    }

    public static String random()
    {
        final StringBuilder buf = new StringBuilder();
        
        switch( RNG.nextInt( 5 ) )
        {
            case 0:
            {
                // 123-456-7890
                
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                
                break;
            }
            case 1:
            {
                // (123) 456-7890
                
                buf.append( '(' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( ')' );
                buf.append( ' ' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                
                break;
            }
            case 2:
            {
                // 1-123-456-7890
                
                buf.append( '1' );
                buf.append( '-' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                
                break;
            }
            case 3:
            {
                // +1-123-456-7890
                
                buf.append( '+' );
                buf.append( '1' );
                buf.append( '-' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                
                break;
            }
            case 4:
            {
                // 456-7890
                
                buf.append( digitNonZero() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( '-' );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                buf.append( digit() );
                
                break;
            }
            default:
            {
                throw new IllegalStateException();
            }
        }
        
        return buf.toString();
    }
    
    public static void main( final String[] args )
    {
        System.err.println( random() );
    }

}
