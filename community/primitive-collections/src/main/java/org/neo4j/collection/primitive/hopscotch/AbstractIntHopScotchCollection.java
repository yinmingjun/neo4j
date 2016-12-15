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
        int counter = 0;
        Loop loop = new Loop<E>( visitor, capacity, nullKey, keyLookupTotal, visitorTotal, visitedRecords, counter )
                .loop();
        counter = loop.getCounter();
        visitedRecords = loop.getVisitedRecords();
        keyLookupTotal = loop.getKeyLookupTotal();
        visitorTotal = loop.getVisitorTotal();
        System.out.println( "Loop counter is: " + counter );
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

    private class Loop<E extends Exception>
    {
        private PrimitiveIntVisitor<E> visitor;
        private int capacity;
        private long nullKey;
        private long keyLookupTotal;
        private long visitorTotal;
        private int visitedRecords;
        private int counter;

        public Loop( PrimitiveIntVisitor<E> visitor, int capacity, long nullKey, long keyLookupTotal, long visitorTotal,
                int visitedRecords, int counter )
        {
            this.visitor = visitor;
            this.capacity = capacity;
            this.nullKey = nullKey;
            this.keyLookupTotal = keyLookupTotal;
            this.visitorTotal = visitorTotal;
            this.visitedRecords = visitedRecords;
            this.counter = counter;
        }

        public long getKeyLookupTotal()
        {
            return keyLookupTotal;
        }

        public long getVisitorTotal()
        {
            return visitorTotal;
        }

        public int getVisitedRecords()
        {
            return visitedRecords;
        }

        public int getCounter()
        {
            return counter;
        }

        public Loop loop() throws E
        {
            for (; counter < capacity; counter++ )
            {
                long keyLookUpStart = System.nanoTime();
                long key = table.key( counter );
                keyLookupTotal += (System.nanoTime() - keyLookUpStart);
                if ( key != nullKey )
                {
                    long visitStart = System.nanoTime();
                    visitor.visited( (int) key );
                    visitedRecords++;
                    visitorTotal += (System.nanoTime() - visitStart);
                }
            }
            return this;
        }
    }
}
