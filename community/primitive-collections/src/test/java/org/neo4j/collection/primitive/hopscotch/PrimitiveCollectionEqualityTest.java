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

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveCollection;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.function.Factory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings( "unchecked" )
@RunWith( Theories.class )
public class PrimitiveCollectionEqualityTest
{
    private interface Value<T extends PrimitiveCollection>
    {
        void add( T coll );

        /**
         * @return 'true' if what was removed was exactly the value that was put in.
         */
        boolean remove( T coll );
    }

    private abstract static class ValueProducer<T extends PrimitiveCollection>
    {
        private final Class<T> applicableType;

        public ValueProducer( Class<T> applicableType )
        {
            this.applicableType = applicableType;
        }

        public boolean isApplicable( Factory<? extends PrimitiveCollection> factory )
        {
            try ( PrimitiveCollection coll = factory.newInstance() )
            {
                return applicableType.isInstance( coll );
            }
        }

        public abstract Value<T> randomValue();
    }

    // ==== Test Value Producers ====

    @DataPoint
    public static ValueProducer<PrimitiveIntSet> intV = new ValueProducer<PrimitiveIntSet>( PrimitiveIntSet.class )
    {
        @Override
        public Value<PrimitiveIntSet> randomValue()
        {
            final int x = randomInt();
            return new Value<PrimitiveIntSet>()
            {
                @Override
                public void add( PrimitiveIntSet coll )
                {
                    coll.add( x );
                }

                @Override
                public boolean remove( PrimitiveIntSet coll )
                {
                    return coll.remove( x );
                }
            };
        }
    };

    // ==== Primitive Collection Implementations ====

    @DataPoint
    public static Factory<PrimitiveIntSet> offheapIntSet = Primitive::offHeapIntSet;

    @DataPoint
    public static Factory<PrimitiveIntSet> offheapIntSetWithCapacity = () -> Primitive.offHeapIntSet( randomCapacity() );

    private static final PrimitiveIntSet observedRandomInts = Primitive.intSet();
    private static final PrimitiveLongSet observedRandomLongs = Primitive.longSet();

    /**
     * Produce a random int that hasn't been seen before by any test.
     */
    private static int randomInt()
    {
        int n;
        do
        {
            n = ThreadLocalRandom.current().nextInt();
        }
        while ( n == -1 || !observedRandomInts.add( n ) );
        return n;
    }

    private static int randomCapacity()
    {
        return ThreadLocalRandom.current().nextInt( 30, 1200 );
    }

    private void assertEquals( PrimitiveCollection a, PrimitiveCollection b )
    {
        assertThat( a, is( equalTo( b ) ) );
    }

    @Theory
    public void addingTheSameValuesMustProduceEqualCollections(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
                PrimitiveCollection b = factoryB.newInstance() )
        {
            Value value = values.randomValue();
            value.add( a );
            value.add( b );
            assertEquals( a, b );
        }
    }
}
