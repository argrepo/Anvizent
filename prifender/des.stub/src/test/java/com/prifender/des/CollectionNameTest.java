package com.prifender.des;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public final class CollectionNameTest
{
    @Test
    public void testFromSegmentsBasic() throws Exception
    {
        assertEquals( "abc", CollectionName.fromSegments( "abc" ).toString() );
        assertEquals( "abc.def", CollectionName.fromSegments( "abc", "def" ).toString() );
        assertEquals( "abc.def.ghi", CollectionName.fromSegments( "abc", "def", "ghi" ).toString() );
    }
    
    @Test
    public void testFromSegmentsWithBrackets() throws Exception
    {
        assertEquals( "[ab.c]", CollectionName.fromSegments( "ab.c" ).toString() );
        assertEquals( "abc.[de.f]", CollectionName.fromSegments( "abc", "de.f" ).toString() );
        assertEquals( "abc.[d.e.f].ghi", CollectionName.fromSegments( "abc", "d.e.f", "ghi" ).toString() );
    }

    @Test
    public void testFromSegmentsValidation1() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.fromSegments( null ) );
        assertEquals( "Null name segments array", e.getMessage() );
    }

    @Test
    public void testFromSegmentsValidation2() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.fromSegments( new String[ 0 ] ) );
        assertEquals( "Empty name segments array", e.getMessage() );
    }

    @Test
    public void testFromSegmentsValidation3() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.fromSegments( "a", null, "b" ) );
        assertEquals( "Null name segment found", e.getMessage() );
    }

    @Test
    public void testFromSegmentsValidation4() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.fromSegments( "a", "  ", "b" ) );
        assertEquals( "Empty name segment found", e.getMessage() );
    }

    @Test
    public void testParseBasic() throws Exception
    {
        assertArrayEquals( new String[] { "abc" }, CollectionName.parse( "abc" ).segments() );
        assertArrayEquals( new String[] { "abc", "def" }, CollectionName.parse( "abc.def" ).segments() );
        assertArrayEquals( new String[] { "abc", "def", "ghi" }, CollectionName.parse( "abc.def.ghi" ).segments() );
    }

    @Test
    public void testParseWithBrackets() throws Exception
    {
        assertArrayEquals( new String[] { "abc" }, CollectionName.parse( "abc" ).segments() );
        assertArrayEquals( new String[] { "abc", "de.f" }, CollectionName.parse( "abc.[de.f]" ).segments() );
        assertArrayEquals( new String[] { "abc", "d.e.f", "ghi" }, CollectionName.parse( "abc.[d.e.f].ghi" ).segments() );
    }
    
    @Test
    public void testParseValidation1() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( null ) );
        assertEquals( null, e.getMessage() );
    }

    @Test
    public void testParseValidation2() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( "  " ) );
        assertEquals( "  ", e.getMessage() );
    }

    @Test
    public void testParseValidation3() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( "." ) );
        assertEquals( ".", e.getMessage() );
    }

    @Test
    public void testParseValidation4() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( ".a" ) );
        assertEquals( ".a", e.getMessage() );
    }

    @Test
    public void testParseValidation5() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( "a." ) );
        assertEquals( "a.", e.getMessage() );
    }

    @Test
    public void testParseValidation6() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( "a..b" ) );
        assertEquals( "a..b", e.getMessage() );
    }

    @Test
    public void testParseValidation7() throws Exception
    {
        final IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> CollectionName.parse( "a.[b" ) );
        assertEquals( "a.[b", e.getMessage() );
    }

}
