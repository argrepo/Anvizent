package com.prifender.des.mock.relational;

import static com.prifender.des.mock.MockDataExtractionServiceProblems.unknownCollection;
import static com.prifender.des.mock.data.DataSet.RNG;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.mock.data.BirthDates;
import com.prifender.des.mock.data.CityStateZip;
import com.prifender.des.mock.data.Commissions;
import com.prifender.des.mock.data.Countries;
import com.prifender.des.mock.data.Emails;
import com.prifender.des.mock.data.FirstNames;
import com.prifender.des.mock.data.HireDates;
import com.prifender.des.mock.data.LastNames;
import com.prifender.des.mock.data.Nationalities;
import com.prifender.des.mock.data.PhoneNumbers;
import com.prifender.des.mock.data.Salaries;
import com.prifender.des.mock.data.SocialSecurityNumbers;
import com.prifender.des.mock.data.Streets;
import com.prifender.des.model.Constraint;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Type;

public final class RelationalDataSource
{
    public static final String TABLE_EMPLOYEES = "Employees";
    public static final String TABLE_EMPLOYEE_TO_ADDRESS = "EmployeeToAddress";
    public static final String TABLE_ADDRESSES = "Addresses";
    public static final String TABLE_PHONE_NUMBERS = "PhoneNumbers";
    public static final String TABLE_PAY_CHECKS = "PayChecks";
    
    private static final Metadata METADATA = new Metadata()
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_EMPLOYEES )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "first_name" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "last_name" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "ssn" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "email" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "hire_date" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "dob" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "nationality" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "salary" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "commission_pct" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "manager_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.PRIMARY_KEY )
                        .addAttributesItem( "id" )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.FOREIGN_KEY )
                        .addAttributesItem( "manager_id" )
                        .target( TABLE_EMPLOYEES )
                        .addTargetAttributesItem( "id" )
                    )
                )
            )
        )
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_ADDRESSES )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "street" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "city" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "region" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "postal_code" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "country" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.PRIMARY_KEY )
                        .addAttributesItem( "id" )
                    )
                )
            )
        )
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_EMPLOYEE_TO_ADDRESS )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "employee_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "address_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.PRIMARY_KEY )
                        .addAttributesItem( "employee_id" )
                        .addAttributesItem( "address_id" )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.FOREIGN_KEY )
                        .addAttributesItem( "employee_id" )
                        .target( TABLE_EMPLOYEES )
                        .addTargetAttributesItem( "id" )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.FOREIGN_KEY )
                        .addAttributesItem( "address_id" )
                        .target( TABLE_ADDRESSES )
                        .addTargetAttributesItem( "id" )
                    )
                )
            )
        )
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_PHONE_NUMBERS )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "employee_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "number" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.PRIMARY_KEY )
                        .addAttributesItem( "employee_id" )
                        .addAttributesItem( "number" )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.FOREIGN_KEY )
                        .addAttributesItem( "employee_id" )
                        .target( TABLE_EMPLOYEES )
                        .addTargetAttributesItem( "id" )
                    )
                )
            )
        )
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_PAY_CHECKS )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "employee_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "payment_date" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "amount" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.PRIMARY_KEY )
                        .addAttributesItem( "id" )
                    )
                    .addConstraintsItem
                    (
                        new Constraint()
                        .kind( Constraint.KindEnum.FOREIGN_KEY )
                        .addAttributesItem( "employee_id" )
                        .target( TABLE_EMPLOYEES )
                        .addTargetAttributesItem( "id" )
                    )
                )
            )
        );
    
    private final int size;
    
    public RelationalDataSource( final int size )
    {
        this.size = size;
    }
    
    public static boolean isValidTable( final String tableName )
    {
        for( final NamedType table : METADATA.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                return true;
            }
        }
        
        return false;
    }
    
    public static boolean isValidColumn( final String tableName, final String columnName )
    {
        for( final NamedType table : METADATA.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                final Type entryType = table.getType().getEntryType();
                
                for( final NamedType column : entryType.getAttributes() )
                {
                    if( column.getName().equals( columnName ) )
                    {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    public static Metadata metadata()
    {
        return METADATA;
    }
    
    public static Set<String> tables()
    {
        final Set<String> tables = new LinkedHashSet<String>();
        
        for( final NamedType table : METADATA.getObjects() )
        {
            tables.add( table.getName() );
        }
        
        return Collections.unmodifiableSet( tables );
    }
    
    public Table table( final String name ) throws DataExtractionServiceException
    {
        if( name != null )
        {
            if( name.equals( TABLE_EMPLOYEES ) )
            {
                return employees();
            }
            else if( name.equals( TABLE_EMPLOYEE_TO_ADDRESS ) )
            {
                return employeeToAddress();
            }
            else if( name.equals( TABLE_ADDRESSES ) )
            {
                return addresses();
            }
            else if( name.equals( TABLE_PHONE_NUMBERS ) )
            {
                return phoneNumbers();
            }
            else if( name.equals( TABLE_PAY_CHECKS ) )
            {
                return payChecks();
            }
        }
        
        throw new DataExtractionServiceException( unknownCollection( name ) );
    }
    
    public Table employees()
    {
        return new Table()
        {
            private int position = 0;
            private final Set<String> ssns = new HashSet<String>();
            
            @Override
            public int size()
            {
                return RelationalDataSource.this.size;
            }
            
            @Override
            public boolean hasNext()
            {
                return ( this.position < RelationalDataSource.this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                final Map<String,Object> record = new LinkedHashMap<String,Object>();
                final String firstName = FirstNames.random();
                final String lastName = LastNames.random();
                
                String ssn = null;
                
                while( ssn == null || this.ssns.contains( ssn ) )
                {
                    ssn = SocialSecurityNumbers.random();
                }
                
                this.ssns.add( ssn );
                
                record.put( "id", this.position );
                record.put( "first_name", firstName );
                record.put( "last_name", lastName );
                record.put( "ssn", ssn );
                record.put( "email", Emails.random( firstName, lastName ) );
                record.put( "hire_date", HireDates.random() );
                record.put( "dob", BirthDates.random() );
                record.put( "nationality", Nationalities.random() );
                record.put( "salary", Salaries.random() );
                record.put( "commission_pct", Commissions.random() );
                
                int managerId = RNG.nextInt( RelationalDataSource.this.size ) + 1;
                
                while( managerId == this.position )
                {
                    managerId = RNG.nextInt( RelationalDataSource.this.size ) + 1;
                }
                
                record.put( "manager_id", managerId );
                
                return record;
            }
        };
    }
    
    public Table addresses()
    {
        return new Table()
        {
            private final int size = RelationalDataSource.this.size * 2;
            private int position = 0;
            
            @Override
            public int size()
            {
                return this.size;
            }

            @Override
            public boolean hasNext()
            {
                return ( this.position < this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                final Map<String,Object> record = new LinkedHashMap<String,Object>();
                final CityStateZip.Entry cityStateZip = CityStateZip.random();
                
                record.put( "id", this.position );
                record.put( "street", Streets.random() );
                record.put( "city", cityStateZip.city );
                record.put( "region", cityStateZip.state );
                record.put( "postal_code", cityStateZip.zip );
                record.put( "country", Countries.random() );
                
                return record;
            }
        };
    }
    
    public Table employeeToAddress()
    {
        return new Table()
        {
            final class Record
            {
                private final int employeeId;
                private final int addressId;
                
                public Record()
                {
                    this.employeeId = RNG.nextInt( RelationalDataSource.this.size ) + 1;
                    this.addressId = RNG.nextInt( RelationalDataSource.this.size * 2 ) + 1;
                }
                
                @Override
                public boolean equals( final Object obj )
                {
                    if( obj instanceof Record )
                    {
                        final Record record = (Record) obj;
                        return this.employeeId == record.employeeId && this.addressId == record.addressId;
                    }
                    
                    return false;
                }
                
                @Override
                public int hashCode()
                {
                    return this.employeeId ^ this.addressId;
                }
            }
            
            private final int size = RelationalDataSource.this.size * 2;
            private int position = 0;
            private final Set<Record> records = new HashSet<Record>();
            
            @Override
            public int size()
            {
                return this.size;
            }

            @Override
            public boolean hasNext()
            {
                return ( this.position < this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                Record record = new Record();
                
                while( this.records.contains( record ) )
                {
                    record = new Record();
                }
                
                this.records.add( record );
                
                final Map<String,Object> result = new LinkedHashMap<String,Object>();
                
                result.put( "employee_id", record.employeeId );
                result.put( "address_id", record.addressId );
                
                return result;
            }
        };
    }
    
    public Table phoneNumbers()
    {
        return new Table()
        {
            private final int size = RelationalDataSource.this.size * 2;
            private int position = 0;
            
            @Override
            public int size()
            {
                return this.size;
            }

            @Override
            public boolean hasNext()
            {
                return ( this.position < this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                final Map<String,Object> record = new LinkedHashMap<String,Object>();
                
                record.put( "employee_id", RNG.nextInt( RelationalDataSource.this.size ) + 1 );
                record.put( "number", PhoneNumbers.random() );
                
                return record;
            }
        };
    }
    
    public Table payChecks()
    {
        return new Table()
        {
            private final int size = RelationalDataSource.this.size * 10;
            private int position = 0;
            
            @Override
            public int size()
            {
                return this.size;
            }

            @Override
            public boolean hasNext()
            {
                return ( this.position < this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                final Map<String,Object> record = new LinkedHashMap<String,Object>();
                
                record.put( "id", this.position );
                record.put( "employee_id", RNG.nextInt( RelationalDataSource.this.size ) + 1 ) ;
                record.put( "payment_date", HireDates.random() );
                record.put( "amount", 2000 + RNG.nextInt( 2000 ) );
                
                return record;
            }
        };
    }
    
    public static void main( final String[] args ) throws Exception
    {
        final RelationalDataSource data = new RelationalDataSource( 5 );
        
        for( final String table : tables() )
        {
            System.out.println();
            System.out.println( table );
            System.out.println();
            
            for( final Iterator<Map<String,Object>> itr = data.table( table ); itr.hasNext(); )
            {
                System.out.println( mapToJson( itr.next() ) );
            }
        }
        
        System.out.println();
    }
    
    private static JsonObject mapToJson( final Map<String,Object> map )
    {
        final JsonObjectBuilder builder = Json.createObjectBuilder();
        
        for( final Map.Entry<String,Object> entry : map.entrySet() )
        {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            
            if( value instanceof String )
            {
                builder.add( key, (String) value );
            }
            else if( value instanceof Integer )
            {
                builder.add( key, (Integer) value );
            }
            else
            {
                throw new IllegalArgumentException();
            }
        }
        
        return builder.build();
    }
    
    public static interface Table extends Iterator<Map<String,Object>>
    {
        int size();
    }

}
