package com.prifender.des.mock.pump.relational;

import java.util.ArrayList;
import java.util.List;

public final class Schema
{
    private static final List<Table> FULL = new ArrayList<Table>();
    
    static
    {
        FULL.add( EmployeesTable.INSTANCE );
        FULL.add( PhoneNumbersTable.INSTANCE );
        FULL.add( AddressesTable.INSTANCE );
        FULL.add( EmployeeToAddressTable.INSTANCE );
        FULL.add( PayChecksTable.INSTANCE );
    }
    
    private static final List<Table> MINIMAL = new ArrayList<Table>();
    
    static
    {
        MINIMAL.add( EmployeesTable.INSTANCE );
        MINIMAL.add( PhoneNumbersTable.INSTANCE );
        MINIMAL.add( AddressesTable.INSTANCE );
        MINIMAL.add( EmployeeToAddressTable.INSTANCE );
    }
    
    public static List<Table> full()
    {
        return FULL;
    }
    
    public static List<Table> minimal()
    {
        return MINIMAL;
    }
    
    public static Table table( final String name )
    {
        if( name == null )
        {
            throw new IllegalArgumentException();
        }
        
        for( final Table table : FULL )
        {
            if( table.collection().equals( name ) )
            {
                return table;
            }
        }
        
        throw new IllegalArgumentException();
    }

}
