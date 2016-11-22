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

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class IntKeyUnsafeTable<VALUE> extends UnsafeTable<VALUE>
{
    private long totalTime = 0;
    private long callCounter = 0;

    public IntKeyUnsafeTable( int capacity, VALUE valueMarker )
    {
        super( capacity, 4, valueMarker );
    }

    @Override
    protected long internalKey( long keyAddress )
    {
        long start = System.nanoTime();
        int value = UnsafeUtil.getInt( keyAddress );
        totalTime += (System.nanoTime() - start);
        callCounter++;
        return  value;
    }

    @Override
    protected void internalPut( long keyAddress, long key, VALUE value )
    {
        assert (int)key == key : "Illegal key " + key + ", it's bigger than int";

        // We can "safely" cast to int here, assuming that this call trickles in via a PrimitiveIntCollection
        UnsafeUtil.putInt( keyAddress, (int) key );
    }

    @Override
    protected Table<VALUE> newInstance( int newCapacity )
    {
        return new IntKeyUnsafeTable<>( newCapacity, valueMarker );
    }

    public void printStatistic()
    {
        System.out.println( "Table internal key time: " + TimeUnit.NANOSECONDS.toMillis( totalTime ) + " ms. "
                + "Number " + callCounter + "of calls." );
        UnsafeUtil.printSummary();
    }

    public void reset()
    {
        UnsafeUtil.resetTime();
        totalTime = 0;
        callCounter = 0;
    }
}
