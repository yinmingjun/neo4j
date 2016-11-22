/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.collection.primitive.hopscotch;

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntCollections.PrimitiveIntBaseIterator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntVisitor;

public abstract class AbstractIntHopScotchCollection<VALUE> extends AbstractHopScotchCollection<VALUE>
        implements PrimitiveIntCollection
{
    public AbstractIntHopScotchCollection( Table<VALUE> table )
    {
        super( table );
    }

    @Override
    public PrimitiveIntIterator iterator()
    {
        final TableKeyIterator<VALUE> longIterator = new TableKeyIterator<>( table, this );
        return new PrimitiveIntBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                return longIterator.hasNext() ? next( (int) longIterator.next() ) : false;
            }
        };
    }

    @Override
    public <E extends Exception> void visitKeys( PrimitiveIntVisitor<E> visitor ) throws E
    {
        if (table instanceof IntKeyUnsafeTable)
        {
            IntKeyUnsafeTable unsafeTable = (IntKeyUnsafeTable) this.table;
            unsafeTable.reset();
        }
        long startTime = System.nanoTime();
        int capacity = table.capacity();
        int size = table.size();
        long nullKey = table.nullKey();

        long keyLookupTotal = 0;
        long visitorTotal = 0;

        int visitedRecords = 0;
        for ( int i = 0; i < capacity && visitedRecords < size; i++ )
        {
            long keyLookUpStart = System.nanoTime();
            long key = table.key( i );
            keyLookupTotal += (System.nanoTime() - keyLookUpStart);
            if ( key != nullKey )
            {
                long visitStart = System.nanoTime();
                boolean visited = visitor.visited( (int) key );
                visitedRecords++;
                visitorTotal += (System.nanoTime() - visitStart);
                if ( visited )
                {
                    printStatistic( startTime, capacity, size, i, keyLookupTotal, visitorTotal );
                    return;
                }
            }
        }
        if (table instanceof IntKeyUnsafeTable)
        {
            IntKeyUnsafeTable unsafeTable = (IntKeyUnsafeTable) this.table;
            unsafeTable.printStatistic();
        }
        printStatistic( startTime, capacity, size, visitedRecords, keyLookupTotal, visitorTotal );
    }

    private void printStatistic( long startTime, int capacity, int size, int visited, long keyLookup, long visitor )
    {
        System.out.println(
                "Visit " + capacity + " keys in " + nanoToMillis( System.nanoTime() - startTime ) + " ms. " +
                        "Table size is: " + size + ". " + "Visited: " + visited + ". " +
                        "Key lookup total call time: " + nanoToMillis( keyLookup ) + ", visitor total call time: " +
                        nanoToMillis( visitor ) );

    }

    private long nanoToMillis( long nanos )
    {
        return TimeUnit.NANOSECONDS.toMillis( nanos );
    }
}
