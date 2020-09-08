/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.LogVersionRepository;

public class SimpleLogVersionRepository implements LogVersionRepository
{
    private final AtomicLong logVersion = new AtomicLong();
    private final AtomicLong checkpointLogVersion = new AtomicLong();

    public SimpleLogVersionRepository()
    {
        this( INITIAL_LOG_VERSION, INITIAL_LOG_VERSION );
    }

    public SimpleLogVersionRepository( long initialLogVersion )
    {
        this( initialLogVersion, INITIAL_LOG_VERSION );
    }

    public SimpleLogVersionRepository( long initialLogVersion, long initialCheckpointLogVersion )
    {
        this.logVersion.set( initialLogVersion );
        this.checkpointLogVersion.set( initialCheckpointLogVersion );
    }

    @Override
    public long incrementAndGetVersion( PageCursorTracer cursorTracer )
    {
        return logVersion.incrementAndGet();
    }

    @Override
    public long getCurrentLogVersion()
    {
        return logVersion.get();
    }

    @Override
    public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
    {
        this.logVersion.set( version );
    }

    @Override
    public long getCheckpointLogVersion()
    {
        return checkpointLogVersion.get();
    }

    @Override
    public void setCheckpointLogVersion( long version, PageCursorTracer cursorTracer )
    {
        checkpointLogVersion.set( version );
    }

    @Override
    public long incrementAndGetCheckpointLogVersion( PageCursorTracer cursorTracer )
    {
        return checkpointLogVersion.incrementAndGet();
    }
}
