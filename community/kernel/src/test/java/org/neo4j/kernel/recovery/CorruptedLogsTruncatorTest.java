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
package org.neo4j.kernel.recovery;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class CorruptedLogsTruncatorTest
{
    private static final long SINGLE_LOG_FILE_SIZE = CURRENT_FORMAT_LOG_HEADER_SIZE + 9L;
    private static final int TOTAL_NUMBER_OF_LOG_FILES = 12;

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final LifeSupport life = new LifeSupport();

    private Path databaseDirectory;
    private LogFiles logFiles;
    private CorruptedLogsTruncator logPruner;

    @BeforeEach
    void setUp() throws Exception
    {
        databaseDirectory = testDirectory.homePath();
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDirectory, fs )
                .withRotationThreshold( SINGLE_LOG_FILE_SIZE )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        life.add( logFiles );
        logPruner = new CorruptedLogsTruncator( databaseDirectory, logFiles, fs, INSTANCE );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    @Test
    void doNotPruneEmptyLogs() throws IOException
    {
        logPruner.truncate( new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE ) );
        assertTrue( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, databaseDirectory ) );
    }

    @Test
    void doNotPruneNonCorruptedLogs() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long fileSizeBeforePrune = Files.size( logFile.getHighestLogFile() );
        LogPosition endOfLogsPosition = new LogPosition( highestLogVersion, fileSizeBeforePrune );
        assertEquals( TOTAL_NUMBER_OF_LOG_FILES - 1, highestLogVersion );

        logPruner.truncate( endOfLogsPosition );

        assertEquals( TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length );
        assertEquals( fileSizeBeforePrune, Files.size( logFile.getHighestLogFile() ) );
        assertTrue( ArrayUtils.isEmpty( databaseDirectory.toFile().listFiles( File::isDirectory ) ) );
    }

    @Test
    void pruneAndArchiveLastLog() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        var logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        Path highestLogFile = logFile.getHighestLogFile();
        long fileSizeBeforePrune = Files.size( highestLogFile );
        int bytesToPrune = 5;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition( highestLogVersion, byteOffset );

        logPruner.truncate( prunePosition );

        assertEquals( TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length );
        assertEquals( byteOffset, Files.size( highestLogFile ) );

        Path corruptedLogsDirectory = databaseDirectory.resolve( CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( Files.exists( corruptedLogsDirectory ) );
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull( files );
        assertEquals( 1, files.length );

        File corruptedLogsArchive = files[0];
        checkArchiveName( highestLogVersion, byteOffset, corruptedLogsArchive );
        try ( ZipFile zipFile = new ZipFile( corruptedLogsArchive ) )
        {
            assertEquals( 1, zipFile.size() );
            checkEntryNameAndSize( zipFile, highestLogFile.getFileName().toString(), bytesToPrune );
        }
    }

    @Test
    void pruneAndArchiveMultipleLogs() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        long highestCorrectLogFileIndex = 5;
        var logFile = logFiles.getLogFile();
        Path highestCorrectLogFile = logFile.getLogFileForVersion( highestCorrectLogFileIndex );
        long fileSizeBeforePrune = Files.size( highestCorrectLogFile );
        long highestLogFileLength = Files.size( logFile.getHighestLogFile() );
        int bytesToPrune = 7;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition( highestCorrectLogFileIndex, byteOffset );
        life.shutdown();

        logPruner.truncate( prunePosition );

        life.start();
        assertEquals( 6, logFiles.logFiles().length );
        assertEquals( byteOffset, Files.size( highestCorrectLogFile ) );

        Path corruptedLogsDirectory = databaseDirectory.resolve( CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( Files.exists( corruptedLogsDirectory ) );
        File[] files = corruptedLogsDirectory.toFile().listFiles();
        assertNotNull( files );
        assertEquals( 1, files.length );

        File corruptedLogsArchive = files[0];
        checkArchiveName( highestCorrectLogFileIndex, byteOffset, corruptedLogsArchive );
        try ( ZipFile zipFile = new ZipFile( corruptedLogsArchive ) )
        {
            assertEquals( 7, zipFile.size() );
            checkEntryNameAndSize( zipFile, highestCorrectLogFile.getFileName().toString(), bytesToPrune );
            long nextLogFileIndex = highestCorrectLogFileIndex + 1;
            int lastFileIndex = TOTAL_NUMBER_OF_LOG_FILES - 1;
            for ( long index = nextLogFileIndex; index < lastFileIndex; index++ )
            {
                checkEntryNameAndSize( zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + index, SINGLE_LOG_FILE_SIZE );
            }
            checkEntryNameAndSize( zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + lastFileIndex, highestLogFileLength );
        }
    }

    private void checkEntryNameAndSize( ZipFile zipFile, String entryName, long expectedSize ) throws IOException
    {
        ZipEntry entry = zipFile.getEntry( entryName );
        InputStream inputStream = zipFile.getInputStream( entry );
        int entryBytes = 0;
        while ( inputStream.read() >= 0 )
        {
            entryBytes++;
        }
        assertEquals( expectedSize, entryBytes );
    }

    private void checkArchiveName( long highestLogVersion, long byteOffset, File corruptedLogsArchive )
    {
        String name = corruptedLogsArchive.getName();
        assertTrue( name.startsWith( "corrupted-neostore.transaction.db-" + highestLogVersion + "-" + byteOffset ) );
        assertTrue( FilenameUtils.isExtension( name, "zip" ) );
    }

    private void generateTransactionLogFiles( LogFiles logFiles ) throws IOException
    {
        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChecksumChannel writer = logFile.getTransactionLogWriter().getWriter().getChannel();
        for ( byte i = 0; i < 107; i++ )
        {
            writer.put( i );
            writer.prepareForFlush();
            if ( logFile.rotationNeeded() )
            {
                logFile.rotate();
            }
        }
    }
}
