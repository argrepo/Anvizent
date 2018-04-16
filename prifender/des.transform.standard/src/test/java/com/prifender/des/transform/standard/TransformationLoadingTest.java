package com.prifender.des.transform.standard;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.Transformations;

public final class TransformationLoadingTest
{
    @Test
    public void testLoading() throws Exception
    {
        final Transformations transformations = new Transformations();
        
        boolean loadedCountryCodeTransformation = false;
        boolean loadedTrimTransformation = false;
        boolean loadedUpperCaseTransformation = false;
        
        for( final Transformation t : transformations.list() )
        {
            if( t instanceof CountryCodeTransformation )
            {
                loadedCountryCodeTransformation = true;
            }
            else if( t instanceof TrimTransformation )
            {
                loadedTrimTransformation = true;
            }
            else if( t instanceof UpperCaseTransformation )
            {
                loadedUpperCaseTransformation = true;
            }
        }
        
        assertTrue( loadedCountryCodeTransformation && loadedTrimTransformation && loadedUpperCaseTransformation );
    }
}
