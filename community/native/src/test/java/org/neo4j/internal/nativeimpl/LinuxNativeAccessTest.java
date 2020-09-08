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
package org.neo4j.internal.nativeimpl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.nativeimpl.NativeAccess.ERROR;

class LinuxNativeAccessTest
{
    private final LinuxNativeAccess nativeAccess = new LinuxNativeAccess();

    @Test
    @DisabledOnOs( OS.LINUX )
    void disabledOnNonLinux()
    {
        assertFalse( nativeAccess.isAvailable() );
    }

    @Nested
    @EnabledOnOs( OS.LINUX )
    class AccessLinuxMethodsTest
    {
        @TempDir
        Path tempFile;

        @Test
        void availableOnLinux()
        {
            assertTrue( nativeAccess.isAvailable() );
        }

        @Test
        void accessErrorMessageOnError() throws IOException, IllegalAccessException
        {
            Path file = tempFile.resolve( "file" );
            int descriptor = getClosedDescriptor( file );
            var nativeCallResult = nativeAccess.tryPreallocateSpace( descriptor, 1024 );
            assertNotEquals( 0, nativeCallResult.getErrorCode() );
            assertThat( nativeCallResult.getErrorMessage() ).isNotEmpty();
        }

        @Test
        void failToPreallocateOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            var preallocateResult = nativeAccess.tryPreallocateSpace( 0, 1024 );
            assertEquals( ERROR, preallocateResult.getErrorCode() );
            assertTrue( preallocateResult.isError() );

            var negativeDescriptor = nativeAccess.tryPreallocateSpace( -1, 1024 );
            assertEquals( ERROR, negativeDescriptor.getErrorCode() );
            assertTrue( negativeDescriptor.isError() );

            Path file = tempFile.resolveSibling( "file" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryPreallocateSpace( descriptor, 1024 ) );
        }

        @Test
        void preallocateCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            FileStore fileStore = Files.getFileStore( tempFile );
            long blockSize = fileStore.getBlockSize();
            Path file = tempFile.resolve( "preallocated1" );
            Path file2 = tempFile.resolve( "preallocated2" );
            Path file3 = tempFile.resolve( "preallocated3" );
            long size1 = blockSize - 1;
            long size2 = blockSize;
            long size3 = 2 * blockSize;

            preallocate( file, size1 );
            preallocate( file2, size2 );
            preallocate( file3, size3 );

            assertEquals( size1, Files.size( file ) );
            assertEquals( size2, Files.size( file2 ) );
            assertEquals( size3, Files.size( file3 ) );
        }

        @Test
        void failToAdviseSequentialOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            var nativeCallResult = nativeAccess.tryAdviseSequentialAccess( 0 );
            assertEquals( ERROR, nativeCallResult.getErrorCode() );
            assertTrue( nativeCallResult.isError() );

            var negativeDescriptorResult = nativeAccess.tryAdviseSequentialAccess( -1 );
            assertEquals( ERROR, negativeDescriptorResult.getErrorCode() );
            assertTrue( negativeDescriptorResult.isError() );

            Path file = tempFile.resolve( "sequentialFile" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryAdviseSequentialAccess( descriptor ) );
        }

        @Test
        void adviseSequentialAccessOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            Path file = tempFile.resolve( "correctSequentialFile" );
            try ( RandomAccessFile randomFile = new RandomAccessFile( file.toFile(), "rw" ) )
            {
                int descriptor = getDescriptor( randomFile );
                var nativeCallResult = nativeAccess.tryAdviseSequentialAccess( descriptor );
                assertEquals( 0, nativeCallResult.getErrorCode() );
                assertFalse( nativeCallResult.isError() );
            }
        }

        @Test
        void failToSkipCacheOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            assertEquals( ERROR, nativeAccess.tryEvictFromCache( 0 ).getErrorCode() );
            assertEquals( ERROR, nativeAccess.tryEvictFromCache( -1 ).getErrorCode() );

            Path file = tempFile.resolve( "file" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryEvictFromCache( descriptor ) );
        }

        @Test
        void skipCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            Path file = tempFile.resolve( "file" );
            try ( RandomAccessFile randomFile = new RandomAccessFile( file.toFile(), "rw" ) )
            {
                int descriptor = getDescriptor( randomFile );
                assertFalse( nativeAccess.tryEvictFromCache( descriptor ).isError() );
            }
        }
    }

    private void preallocate( Path file, long bytes ) throws IOException, IllegalAccessException
    {
        try ( RandomAccessFile randomFile = new RandomAccessFile( file.toFile(), "rw" ) )
        {
            int descriptor = getDescriptor( randomFile );
            assertFalse( nativeAccess.tryPreallocateSpace( descriptor, bytes ).isError() );
        }
    }

    private int getClosedDescriptor( Path file ) throws IOException, IllegalAccessException
    {
        try ( RandomAccessFile randomFile = new RandomAccessFile( file.toFile(), "rw" ) )
        {
            return getDescriptor( randomFile );
        }
    }

    private int getDescriptor( RandomAccessFile randomFile ) throws IOException, IllegalAccessException
    {
        FileDescriptor fd = randomFile.getFD();
        return FieldUtils.getDeclaredField( FileDescriptor.class, "fd", true ).getInt( fd );
    }
}
