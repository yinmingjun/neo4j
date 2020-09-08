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

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.map.primitive.ObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.TransactionLogVersionSelector;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
class RecoveryCorruptedTransactionLogIT
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private RandomRule random;

    private static final int HEADER_OFFSET = CURRENT_FORMAT_LOG_HEADER_SIZE;
    private static final byte CHECKPOINT_COMMAND_SIZE =
                    2 + // header
                    2 * Long.BYTES + // command content
                    Integer.BYTES; // checksum
    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final RecoveryMonitor recoveryMonitor = new RecoveryMonitor();
    private Path databaseDirectory;
    private final Monitors monitors = new Monitors();
    private LogFiles logFiles;
    private TestDatabaseManagementServiceBuilder databaseFactory;
    private StorageEngineFactory storageEngineFactory;
    boolean useSeparateCheckpointLogs;

    @BeforeEach
    void setUp()
    {
        databaseDirectory = databaseLayout.databaseDirectory();
        monitors.addMonitorListener( recoveryMonitor );
        databaseFactory = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setInternalLogProvider( logProvider )
                .setMonitors( monitors )
                .setFileSystem( fileSystem );
        startStopDatabase();
    }

    @Test
    void evenTruncateNewerTransactionLogFile() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfClosedTransactions = getTransactionIdStore( database ).getLastClosedTransactionId() -
                lastClosedTransactionBeforeStart;
        managementService.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile( this::randomBytes );

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
    }

    @Test
    void doNotTruncateNewerTransactionLogFileWhenFailOnError() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        managementService1.shutdown();
        removeLastCheckpointRecordFromLastLogFile();
        addRandomBytesToLastLogFile( this::randomInvalidVersionsBytes );

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            DatabaseStateService dbStateService = db.getDependencyResolver().resolveDependency( DatabaseStateService.class );
            assertTrue( dbStateService.causeOfFailure( db.databaseId() ).isPresent() );
            assertThat( dbStateService.causeOfFailure( db.databaseId() ).get() ).hasRootCauseInstanceOf( UnsupportedLogVersionException.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void truncateNewerTransactionLogFileWhenForced() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long numberOfClosedTransactions = transactionIdStore.getLastClosedTransactionId() - 1;
        managementService.shutdown();

        removeLastCheckpointRecordFromLastLogFile();
        Supplier<Byte> randomBytesSupplier = this::randomBytes;
        BytesCaptureSupplier capturingSupplier = new BytesCaptureSupplier( randomBytesSupplier );
        addRandomBytesToLastLogFile( capturingSupplier );
        assertFalse( recoveryMonitor.wasRecoveryRequired() );

        startStopDbRecoveryOfCorruptedLogs();

        try
        {
            assertEquals( numberOfClosedTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
            assertTrue( recoveryMonitor.wasRecoveryRequired() );
            assertThat( logProvider ).containsMessages( "Fail to read transaction log version 0.",
                    "Fail to read transaction log version 0. Last valid transaction start offset is: " +
                            ((useSeparateCheckpointLogs ? 5548 : 5570) + HEADER_OFFSET) + "." );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( "Generated random bytes: " + capturingSupplier.getCapturedBytes(), t );
        }
    }

    @ParameterizedTest( name = "[{index}] ({0})" )
    @MethodSource( "corruptedLogEntryWriters" )
    void recoverFirstCorruptedTransactionSingleFileNoCheckpoint( String testName, LogEntryWriterProvider logEntryWriterProvider ) throws IOException
    {
        addCorruptedCommandsToLastLogFile( logEntryWriterProvider );

        startStopDbRecoveryOfCorruptedLogs();

        assertThat( logProvider ).containsMessages( "Fail to read transaction log version 0.",
                                                                  "Fail to read first transaction of log version 0.",
                "Recovery required from position LogPosition{logVersion=0, byteOffset=" + HEADER_OFFSET + "}",
                "Fail to recover all transactions. Any later transactions after position LogPosition{logVersion=0, " +
                        "byteOffset=" + HEADER_OFFSET + "} are unreadable and will be truncated." );

        logFiles = buildDefaultLogFiles( StoreId.UNKNOWN, useSeparateCheckpointLogs );
        assertEquals( 0, logFiles.getLogFile().getHighestLogVersion() );
        ObjectLongMap<Class<?>> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        if ( !useSeparateCheckpointLogs )
        {
            assertEquals( 1, logEntriesDistribution.size() );
            assertEquals( 2, logEntriesDistribution.get( LogEntryInlinedCheckPoint.class ) );
        }
        else
        {
            assertEquals( 0, logEntriesDistribution.size() );
        }
    }

    @Test
    void failToStartWithTransactionLogsWithDataAfterLastEntry() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        managementService.shutdown();

        writeRandomBytesAfterLastCommandInLastLogFile( () -> ByteBuffer.wrap( new byte[]{1, 2, 3, 4, 5} ) );

        startStopDatabase();
        assertThat( logProvider ).assertExceptionForLogMessage( "Fail to read transaction log version 0.")
                .hasMessageContaining( "Transaction log files with version 0 has some data available after last readable " +
                        "log entry. Last readable position " + ((useSeparateCheckpointLogs ? 996 : 1040) + HEADER_OFFSET) );
    }

    @Test
    void startWithTransactionLogsWithDataAfterLastEntryAndCorruptedLogsRecoveryEnabled() throws IOException
    {
        long initialTransactionOffset = HEADER_OFFSET + (useSeparateCheckpointLogs ? 996 : 1018);
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        assertEquals( initialTransactionOffset, getLastClosedTransactionOffset( database ) );
        managementService.shutdown();

        writeRandomBytesAfterLastCommandInLastLogFile( () -> ByteBuffer.wrap( new byte[]{1, 2, 3, 4, 5} ) );

        managementService = databaseFactory.setConfig( fail_on_corrupted_log_files, false ).build();
        try
        {
            assertThat( logProvider ).containsMessages( "Recovery required from position " +
                            "LogPosition{logVersion=0, byteOffset=" + ((useSeparateCheckpointLogs ? 996 : 1018) + HEADER_OFFSET) + "}" )
                    .assertExceptionForLogMessage( "Fail to read transaction log version 0." )
                    .hasMessageContaining( "Transaction log files with version 0 has some data available after last readable log entry. " +
                            "Last readable position " + ((useSeparateCheckpointLogs ? 996 : 1040) + HEADER_OFFSET) );
            GraphDatabaseAPI restartedDb = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            assertEquals( initialTransactionOffset + (useSeparateCheckpointLogs ? 0 : CHECKPOINT_COMMAND_SIZE),
                    getLastClosedTransactionOffset( restartedDb ) );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void failToStartWithNotLastTransactionLogHavingZerosInTheEnd() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        managementService.shutdown();

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            logFiles.getLogFile().rotate();

            // append zeros in the end of previous file causing illegal suffix
            try ( StoreFileChannel writeChannel = fileSystem.write( originalFile ) )
            {
                writeChannel.position( writeChannel.size() );
                for ( int i = 0; i < 10; i++ )
                {
                    writeChannel.writeAll( ByteBuffer.wrap( new byte[]{0, 0, 0, 0, 0} ) );
                }
            }
        }

        startStopDatabase();
        assertThat( logProvider ).assertExceptionForLogMessage( "Fail to read transaction log version 0." )
                .hasMessageContaining( "Transaction log files with version 0 has 50 unreadable bytes" );
    }

    @Test
    void startWithNotLastTransactionLogHavingZerosInTheEndAndCorruptedLogRecoveryEnabled() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        managementService.shutdown();

        long originalLogDataLength;
        Path firstLogFile;
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            LogFile logFile = logFiles.getLogFile();
            LogPosition readablePosition = getLastReadablePosition( logFile );
            firstLogFile = logFiles.getLogFile().getHighestLogFile();
            originalLogDataLength = readablePosition.getByteOffset();
            logFile.rotate();

            // append zeros in the end of previous file causing illegal suffix
            try ( StoreFileChannel writeChannel = fileSystem.write( firstLogFile ) )
            {
                writeChannel.position( writeChannel.size() );
                for ( int i = 0; i < 10; i++ )
                {
                    writeChannel.writeAll( ByteBuffer.wrap( new byte[]{0, 0, 0, 0, 0} ) );
                }
            }
        }

        startStopDbRecoveryOfCorruptedLogs();

        assertEquals( useSeparateCheckpointLogs ? originalLogDataLength : (originalLogDataLength + 2 * CHECKPOINT_COMMAND_SIZE),
                fileSystem.getFileSize( firstLogFile ) );

        assertThat( logProvider ).containsMessages( "Recovery required from position LogPosition{logVersion=0, byteOffset=" +
                ((useSeparateCheckpointLogs ? 996 : 1018) + HEADER_OFFSET)  + "}" )
                .assertExceptionForLogMessage( "Fail to read transaction log version 0." )
                .hasMessage( "Transaction log files with version 0 has 50 unreadable bytes. Was able to read upto " +
                        ((useSeparateCheckpointLogs ? 996 : 1040) + HEADER_OFFSET) +
                        " but " + ((useSeparateCheckpointLogs ? 1046 : 1090) + HEADER_OFFSET) + " is available." );
    }

    @Test
    void startWithoutProblemsIfRotationForcedBeforeFileEnd() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        managementService.shutdown();

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            // append zeros in the end of file before rotation should not be problematic since rotation will prepare tx log file and truncate
            // in it its current position.
            try ( StoreFileChannel writeChannel = fileSystem.write( originalFile ) )
            {
                writeChannel.position( writeChannel.size() );
                for ( int i = 0; i < 10; i++ )
                {
                    writeChannel.writeAll( ByteBuffer.wrap( new byte[]{0, 0, 0, 0, 0} ) );
                }
            }
            logFiles.getLogFile().rotate();
        }

        startStopDatabase();
        assertThat( logProvider ).doesNotContainMessage( "Fail to read transaction log version 0." );
    }

    @Test
    void startWithoutProblemsIfRotationForcedBeforeFileEndAndCorruptedLogFilesRecoveryEnabled() throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransaction( database );
        managementService.shutdown();

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            Path originalFile = logFiles.getLogFile().getHighestLogFile();
            // append zeros in the end of file before rotation should not be problematic since rotation will prepare tx log file and truncate
            // in it its current position.
            try ( StoreFileChannel writeChannel = fileSystem.write( originalFile ) )
            {
                writeChannel.position( writeChannel.size() );
                for ( int i = 0; i < 10; i++ )
                {
                    writeChannel.writeAll( ByteBuffer.wrap( new byte[]{0, 0, 0, 0, 0} ) );
                }
            }
            logFiles.getLogFile().rotate();
        }

        startStopDbRecoveryOfCorruptedLogs();
        assertThat( logProvider ).doesNotContainMessage( "Fail to read transaction log version 0." );
    }

    @Test
    void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruption()
            throws IOException
    {
        addCorruptedCommandsToLastLogFile( CorruptedLogEntryWriter::new );

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {

            DatabaseStateService dbStateService = db.getDependencyResolver().resolveDependency( DatabaseStateService.class );
            assertTrue( dbStateService.causeOfFailure( db.databaseId() ).isPresent() );
            assertThat( dbStateService.causeOfFailure( db.databaseId() ).get() ).hasRootCauseInstanceOf( NegativeArraySizeException.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void failToRecoverFirstCorruptedTransactionSingleFileNoCheckpointIfFailOnCorruptionVersion() throws IOException
    {
        addCorruptedCommandsToLastLogFile( CorruptedLogEntryVersionWriter::new );

        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {

            DatabaseStateService dbStateService = db.getDependencyResolver().resolveDependency( DatabaseStateService.class );
            assertTrue( dbStateService.causeOfFailure( db.databaseId() ).isPresent() );
            assertThat( dbStateService.causeOfFailure( db.databaseId() ).get() ).hasRootCauseInstanceOf( UnsupportedLogVersionException.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest( name = "[{index}] ({0})" )
    @MethodSource( "corruptedLogEntryWriters" )
    void recoverNotAFirstCorruptedTransactionSingleFileNoCheckpoint( String testName, LogEntryWriterProvider logEntryWriterProvider ) throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        for ( int i = 0; i < 10; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition( highestLogFile ).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile( logEntryWriterProvider );
        long modifiedFileLength = fileSystem.getFileSize( highestLogFile );

        assertThat( modifiedFileLength ).isGreaterThan( originalFileLength );

        startStopDbRecoveryOfCorruptedLogs();

        assertThat( logProvider ).containsMessages( "Fail to read transaction log version 0.",
                "Recovery required from position LogPosition{logVersion=0, byteOffset=" + HEADER_OFFSET  + "}",
                "Fail to recover all transactions.",
                "Any later transaction after LogPosition{logVersion=0, byteOffset=" +
                        ((useSeparateCheckpointLogs ? 6117 : 6139) + HEADER_OFFSET) + "} are unreadable and will be truncated." );

        assertEquals( 0, logFiles.getLogFile().getHighestLogVersion() );
        ObjectLongMap<Class<?>> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        if ( useSeparateCheckpointLogs )
        {
            assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
            assertEquals( originalFileLength, fileSystem.getFileSize( highestLogFile ) );
        }
        else
        {
            // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
            // during the setup and starting db as part of the test
            assertEquals( 3, logEntriesDistribution.get( LogEntryInlinedCheckPoint.class ) );
            assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
            assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, fileSystem.getFileSize( highestLogFile ) );
        }
    }

    @ParameterizedTest( name = "[{index}] ({0})" )
    @MethodSource( "corruptedLogEntryWriters" )
    void recoverNotAFirstCorruptedTransactionMultipleFilesNoCheckpoints( String testName, LogEntryWriterProvider logEntryWriterProvider ) throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        TransactionIdStore transactionIdStore = getTransactionIdStore( database );
        long lastClosedTransactionBeforeStart = transactionIdStore.getLastClosedTransactionId();
        generateTransactionsAndRotate( database, 3 );
        for ( int i = 0; i < 7; i++ )
        {
            generateTransaction( database );
        }
        long numberOfTransactions = transactionIdStore.getLastClosedTransactionId() - lastClosedTransactionBeforeStart;
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition( highestLogFile ).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile( logEntryWriterProvider );
        long modifiedFileLength = fileSystem.getFileSize( highestLogFile );

        assertThat( modifiedFileLength ).isGreaterThan( originalFileLength );

        startStopDbRecoveryOfCorruptedLogs();

        assertThat( logProvider ).containsMessages( "Fail to read transaction log version 3.",
                "Recovery required from position LogPosition{logVersion=0, byteOffset=" + HEADER_OFFSET + "}",
                "Fail to recover all transactions.",
                "Any later transaction after LogPosition{logVersion=3, byteOffset=" + (4552 + HEADER_OFFSET) + "} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getLogFile().getHighestLogVersion() );
        ObjectLongMap<Class<?>> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        if ( useSeparateCheckpointLogs )
        {
            assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
            assertEquals( originalFileLength, fileSystem.getFileSize( highestLogFile ) );
        }
        else
        {
            // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
            // during the setup and starting db as part of the test
            assertEquals( 3, logEntriesDistribution.get( LogEntryInlinedCheckPoint.class ) );
            assertEquals( numberOfTransactions, recoveryMonitor.getNumberOfRecoveredTransactions() );
            assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, fileSystem.getFileSize( highestLogFile ) );
        }
    }

    @ParameterizedTest( name = "[{index}] ({0})" )
    @MethodSource( "corruptedLogEntryWriters" )
    void recoverNotAFirstCorruptedTransactionMultipleFilesMultipleCheckpoints( String testName, LogEntryWriterProvider logEntryWriterProvider )
            throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        long transactionsToRecover = 7;
        generateTransactionsAndRotateWithCheckpoint( database, 3 );
        for ( int i = 0; i < transactionsToRecover; i++ )
        {
            generateTransaction( database );
        }
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition( highestLogFile ).getByteOffset();
        removeLastCheckpointRecordFromLastLogFile();

        addCorruptedCommandsToLastLogFile( logEntryWriterProvider );
        long modifiedFileLength = fileSystem.getFileSize( highestLogFile );

        assertThat( modifiedFileLength ).isGreaterThan( originalFileLength );

        startStopDbRecoveryOfCorruptedLogs();

        assertThat( logProvider ).containsMessages( "Fail to read transaction log version 3.",
                "Recovery required from position LogPosition{logVersion=3, byteOffset=" + (569 + HEADER_OFFSET) + "}",
                "Fail to recover all transactions.",
                "Any later transaction after LogPosition{logVersion=3, byteOffset=" +
                        ((useSeparateCheckpointLogs ? 4552 : 4574) + HEADER_OFFSET) + "} are unreadable and will be truncated." );

        assertEquals( 3, logFiles.getLogFile().getHighestLogVersion() );
        ObjectLongMap<Class<?>> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        assertEquals( transactionsToRecover, recoveryMonitor.getNumberOfRecoveredTransactions() );
        if ( useSeparateCheckpointLogs )
        {
            assertEquals( originalFileLength, fileSystem.getFileSize( highestLogFile ) );
        }
        else
        {
            assertEquals( 6, logEntriesDistribution.get( LogEntryInlinedCheckPoint.class ) );
            assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, fileSystem.getFileSize( highestLogFile ) );
        }
    }

    @ParameterizedTest( name = "[{index}] ({0})" )
    @MethodSource( "corruptedLogEntryWriters" )
    void recoverFirstCorruptedTransactionAfterCheckpointInLastLogFile( String testName, LogEntryWriterProvider logEntryWriterProvider ) throws IOException
    {
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransactionsAndRotate( database, 5 );
        managementService.shutdown();

        Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
        long originalFileLength = getLastReadablePosition( highestLogFile ).getByteOffset();
        addCorruptedCommandsToLastLogFile( logEntryWriterProvider );
        long modifiedFileLength = fileSystem.getFileSize( highestLogFile );

        assertThat( modifiedFileLength ).isGreaterThan( originalFileLength );

        startStopDbRecoveryOfCorruptedLogs();

        assertThat( logProvider ).containsMessages( "Fail to read transaction log version 5.",
                "Fail to read first transaction of log version 5.",
                "Recovery required from position LogPosition{logVersion=5, byteOffset=" + (569 + HEADER_OFFSET) + "}",
                "Fail to recover all transactions. " +
                "Any later transactions after position LogPosition{logVersion=5, byteOffset=" + (569 + HEADER_OFFSET) + "} " +
                "are unreadable and will be truncated." );

        assertEquals( 5, logFiles.getLogFile().getHighestLogVersion() );
        ObjectLongMap<Class<?>> logEntriesDistribution = getLogEntriesDistribution( logFiles );
        if ( useSeparateCheckpointLogs )
        {
            assertEquals( originalFileLength, fileSystem.getFileSize( highestLogFile ) );
        }
        else
        {
            // 2 shutdowns will create a checkpoint and recovery that will be triggered by removing tx logs for default db
            // during the setup and starting db as part of the test
            assertEquals( 3, logEntriesDistribution.get( LogEntryInlinedCheckPoint.class ) );
            assertEquals( originalFileLength + CHECKPOINT_COMMAND_SIZE, fileSystem.getFileSize( highestLogFile ) );
        }
    }

    @Test
    void repetitiveRecoveryOfCorruptedLogs() throws IOException
    {
        assumeTrue( useSeparateCheckpointLogs );
        DatabaseManagementService service = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) service.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransactionsAndRotate( database, 4, false );
        service.shutdown();
        removeLastCheckpointRecordFromLastLogFile();

        int expectedRecoveredTransactions = 8;
        while ( expectedRecoveredTransactions > 0 )
        {
            truncateBytesFromLastLogFile( 1 + random.nextInt( 10 ) );
            startStopDbRecoveryOfCorruptedLogs();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertEquals( expectedRecoveredTransactions, numberOfRecoveredTransactions );
            expectedRecoveredTransactions--;
            removeLastCheckpointRecordFromLastLogFile();
        }
    }

    @Test
    void repetitiveRecoveryIfCorruptedLogsWithCheckpoints() throws IOException
    {
        assumeFalse( useSeparateCheckpointLogs );
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransactionsAndRotate( database, 4, true );
        managementService.shutdown();

        while ( logFiles.getLogFile().getHighestLogVersion() > 0 )
        {
            int bytesToTrim = 1 + CHECKPOINT_COMMAND_SIZE + random.nextInt( 100 );
            truncateBytesFromLastLogFile( bytesToTrim );
            databaseFactory.build().shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertThat( numberOfRecoveredTransactions ).isGreaterThanOrEqualTo( 0 );
        }

        Path corruptedLogArchives = databaseDirectory.resolve( CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertThat( corruptedLogArchives.toFile().listFiles() ).isNotEmpty();
    }

    @Test
    void repetitiveRecoveryIfCorruptedLogsSmallTailsWithCheckpoints() throws IOException
    {
        DatabaseManagementService managementService1 = databaseFactory.setConfig( logical_log_rotation_threshold, ByteUnit.mebiBytes( 1 ) ).build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService1.database( DEFAULT_DATABASE_NAME );
        logFiles = buildDefaultLogFiles( getStoreId( database ), useSeparateCheckpointLogs );
        generateTransactionsAndRotate( database, 4, true );
        managementService1.shutdown();

        byte[] trimSizes = new byte[]{16, 48};
        int trimSize = 0;
        while ( logFiles.getLogFile().getHighestLogVersion() > 0 )
        {
            byte bytesToTrim = (byte) (trimSizes[trimSize++ % trimSizes.length] + CHECKPOINT_COMMAND_SIZE);
            truncateBytesFromLastLogFile( bytesToTrim );
            databaseFactory.setConfig( fail_on_corrupted_log_files, false ).build().shutdown();
            int numberOfRecoveredTransactions = recoveryMonitor.getNumberOfRecoveredTransactions();
            assertThat( numberOfRecoveredTransactions ).isGreaterThanOrEqualTo( 0 );
        }

        Path corruptedLogArchives = databaseDirectory.resolve( CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertThat( corruptedLogArchives.toFile().listFiles() ).isNotEmpty();
    }

    private StoreId getStoreId( GraphDatabaseAPI database )
    {
        return database.getDependencyResolver().resolveDependency( StoreIdProvider.class ).getStoreId();
    }

    private static TransactionIdStore getTransactionIdStore( GraphDatabaseAPI database )
    {
        return database.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    private void removeLastCheckpointRecordFromLastLogFile() throws IOException
    {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        var checkpoint = checkpointFile.findLatestCheckpoint();
        if ( checkpoint.isPresent() )
        {
            try ( StoreChannel storeChannel = fileSystem.write( checkpointFile.getCurrentFile() ) )
            {
                LogPosition logPosition = useSeparateCheckpointLogs ? checkpoint.get().getEntryPosition() : checkpoint.get().getLogPosition();
                storeChannel.truncate( logPosition.getByteOffset() );
            }
        }
    }

    private void truncateBytesFromLastLogFile( long bytesToTrim ) throws IOException
    {
        if ( logFiles.getLogFile().getHighestLogVersion() > 0 )
        {
            Path highestLogFile = logFiles.getLogFile().getHighestLogFile();
            long readableOffset = getLastReadablePosition( highestLogFile ).getByteOffset();
            if ( fileSystem.getFileSize( highestLogFile ) > readableOffset )
            {
                fileSystem.truncate( highestLogFile, readableOffset );
                return;
            }
            if ( bytesToTrim > readableOffset )
            {
                fileSystem.deleteFile( highestLogFile );
                if ( logFiles.logFiles().length > 0 )
                {
                    truncateBytesFromLastLogFile( bytesToTrim ); //start truncating from next file
                }
            }
            else
            {
                fileSystem.truncate( highestLogFile, readableOffset - bytesToTrim );
            }
        }
    }

    private void writeRandomBytesAfterLastCommandInLastLogFile( Supplier<ByteBuffer> source ) throws IOException
    {
        int someRandomPaddingAfterEndOfDataInLogFile = random.nextInt( 1, 10 );
        try ( Lifespan lifespan = new Lifespan() )
        {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add( logFiles );

            LogPosition position = getLastReadablePosition( transactionLogFile );

            try ( StoreFileChannel writeChannel = fileSystem.write( logFiles.getLogFile().getHighestLogFile() ) )
            {
                writeChannel.position( position.getByteOffset() + someRandomPaddingAfterEndOfDataInLogFile );
                for ( int i = 0; i < 10; i++ )
                {
                    writeChannel.writeAll( source.get() );
                }
            }
        }
    }

    private LogPosition getLastReadablePosition( Path logFile ) throws IOException
    {
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        LogFile txLogFile = logFiles.getLogFile();
        long logVersion = txLogFile.getLogVersion( logFile );
        LogPosition startPosition = txLogFile.extractHeader( logVersion ).getStartPosition();
        try ( ReadableLogChannel reader = openTransactionFileChannel( logVersion, startPosition ) )
        {
            while ( entryReader.readLogEntry( reader ) != null )
            {
                // scroll to the end of readable entries
            }
        }
        catch ( IncompleteLogHeaderException e )
        {
            return new LogPosition( logVersion, 0 );
        }
        return entryReader.lastPosition();
    }

    private ReadAheadLogChannel openTransactionFileChannel( long logVersion, LogPosition startPosition ) throws IOException
    {
        PhysicalLogVersionedStoreChannel storeChannel = logFiles.getLogFile().openForVersion( logVersion );
        storeChannel.position( startPosition.getByteOffset() );
        return new ReadAheadLogChannel( storeChannel, EmptyMemoryTracker.INSTANCE );
    }

    private LogPosition getLastReadablePosition( LogFile logFile ) throws IOException
    {
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        LogPosition startPosition = logFile.extractHeader( logFiles.getLogFile().getHighestLogVersion() ).getStartPosition();
        try ( ReadableLogChannel reader = logFile.getReader( startPosition ) )
        {
            while ( entryReader.readLogEntry( reader ) != null )
            {
                // scroll to the end of readable entries
            }
        }
        return entryReader.lastPosition();
    }

    private void addRandomBytesToLastLogFile( Supplier<Byte> byteSource ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan() )
        {
            LogFile transactionLogFile = logFiles.getLogFile();
            lifespan.add( logFiles );

            var channel = transactionLogFile.getTransactionLogWriter().getWriter().getChannel();
            for ( int i = 0; i < 10; i++ )
            {
                channel.put( byteSource.get() );
            }
        }
    }

    private byte randomInvalidVersionsBytes()
    {
        return (byte) random.nextInt( TransactionLogVersionSelector.LATEST.version() + 1, Byte.MAX_VALUE );
    }

    private byte randomBytes()
    {
        return (byte) random.nextInt( Byte.MIN_VALUE, Byte.MAX_VALUE );
    }

    private void addCorruptedCommandsToLastLogFile( LogEntryWriterProvider logEntryWriterProvider ) throws IOException
    {
        PositiveLogFilesBasedLogVersionRepository versionRepository = new PositiveLogFilesBasedLogVersionRepository( logFiles );
        LogFiles internalLogFiles = LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withLogVersionRepository( versionRepository )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withStoreId( StoreId.UNKNOWN )
                .withSeparateFilesForCheckpoint( useSeparateCheckpointLogs )
                .withCommandReaderFactory( StorageEngineFactory.selectStorageEngine().commandReaderFactory() )
                .build();
        try ( Lifespan lifespan = new Lifespan( internalLogFiles ) )
        {
            LogFile transactionLogFile = internalLogFiles.getLogFile();
            TransactionLogWriter writer = new TransactionLogWriter( logEntryWriterProvider.create( transactionLogFile.getTransactionLogWriter().getWriter() ) );
            Collection<StorageCommand> commands = new ArrayList<>();
            commands.add( new Command.PropertyCommand( new PropertyRecord( 1 ), new PropertyRecord( 2 ) ) );
            commands.add( new Command.NodeCommand( new NodeRecord( 2 ), new NodeRecord( 3 ) ) );
            PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
            transaction.setHeader( new byte[0], 0, 0, 0, 0, ANONYMOUS );
            writer.append( transaction, 1000, BASE_TX_CHECKSUM );
        }
    }

    private long getLastClosedTransactionOffset( GraphDatabaseAPI database )
    {
        MetadataProvider metaDataStore = database.getDependencyResolver().resolveDependency( MetadataProvider.class );
        return metaDataStore.getLastClosedTransaction()[2];
    }

    private ObjectLongMap<Class<?>> getLogEntriesDistribution( LogFiles logFiles ) throws IOException
    {
        LogFile transactionLogFile = logFiles.getLogFile();

        LogPosition fileStartPosition = transactionLogFile.extractHeader( 0 ).getStartPosition();
        VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );

        MutableObjectLongMap<Class<?>> multiset = new ObjectLongHashMap<>();
        try ( ReadableLogChannel fileReader = transactionLogFile.getReader( fileStartPosition ) )
        {
            LogEntry logEntry = entryReader.readLogEntry( fileReader );
            while ( logEntry != null )
            {
                multiset.addToValue( logEntry.getClass(), 1 );
                logEntry = entryReader.readLogEntry( fileReader );
            }
        }
        return multiset;
    }

    private LogFiles buildDefaultLogFiles( StoreId storeId, boolean useSeparateCheckpointLogs ) throws IOException
    {
        return LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withLogVersionRepository( new SimpleLogVersionRepository() )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withSeparateFilesForCheckpoint( useSeparateCheckpointLogs )
                .withStoreId( storeId )
                .withLogProvider( logProvider )
                .withCommandReaderFactory( StorageEngineFactory.selectStorageEngine().commandReaderFactory() )
                .build();
    }

    private static void generateTransactionsAndRotateWithCheckpoint( GraphDatabaseAPI database, int logFilesToGenerate )
            throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, true );
    }

    private static void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate ) throws IOException
    {
        generateTransactionsAndRotate( database, logFilesToGenerate, false );
    }

    private static void generateTransactionsAndRotate( GraphDatabaseAPI database, int logFilesToGenerate, boolean checkpoint )
            throws IOException
    {
        DependencyResolver resolver = database.getDependencyResolver();
        LogFiles logFiles = resolver.resolveDependency( LogFiles.class );
        CheckPointer checkpointer = resolver.resolveDependency( CheckPointer.class );
        while ( logFiles.getLogFile().getHighestLogVersion() < logFilesToGenerate )
        {
            logFiles.getLogFile().rotate();
            generateTransaction( database );
            if ( checkpoint )
            {
                checkpointer.forceCheckPoint( new SimpleTriggerInfo( "testForcedCheckpoint" ) );
            }
        }
    }

    private static void generateTransaction( GraphDatabaseAPI database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node startNode = transaction.createNode( Label.label( "startNode" ) );
            startNode.setProperty( "key", "value" );
            Node endNode = transaction.createNode( Label.label( "endNode" ) );
            endNode.setProperty( "key", "value" );
            startNode.createRelationshipTo( endNode, RelationshipType.withName( "connects" ) );
            transaction.commit();
        }
    }

    private void startStopDbRecoveryOfCorruptedLogs()
    {
        DatabaseManagementService managementService = databaseFactory
                .setConfig( fail_on_corrupted_log_files, false ).build();
        managementService.shutdown();
    }

    private void startStopDatabase()
    {
        DatabaseManagementService managementService = databaseFactory.build();
        storageEngineFactory = ((GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME )).getDependencyResolver().resolveDependency(
                StorageEngineFactory.class );
        managementService.shutdown();
    }

    private static Stream<Arguments> corruptedLogEntryWriters()
    {
        return Stream.of(
                Arguments.of( "CorruptedLogEntryWriter", (LogEntryWriterProvider) CorruptedLogEntryWriter::new ),
                Arguments.of( "CorruptedLogEntryVersionWriter", (LogEntryWriterProvider) CorruptedLogEntryVersionWriter::new )
        );
    }

    @FunctionalInterface
    private interface LogEntryWriterProvider
    {
        LogEntryWriter<FlushablePositionAwareChecksumChannel> create( LogEntryWriter<FlushablePositionAwareChecksumChannel> writer );
    }

    private static class CorruptedLogEntryWriter extends DelegatingLogEntryWriter
    {

        CorruptedLogEntryWriter( LogEntryWriter<FlushablePositionAwareChecksumChannel> writer )
        {
            super( writer );
        }

        @Override
        public void writeStartEntry( long timeWritten, long latestCommittedTxWhenStarted, int previousChecksum, byte[] additionalHeaderData ) throws IOException
        {
            writeLogEntryHeader( TX_START, channel );
        }
    }

    private static class CorruptedLogEntryVersionWriter extends DelegatingLogEntryWriter
    {
        CorruptedLogEntryVersionWriter( LogEntryWriter<FlushablePositionAwareChecksumChannel> delegate )
        {
            super( delegate );
        }

        /**
         * Use a non-existing log entry version.
         * Implementation stolen from {@link LogEntryWriter#writeStartEntry(long, long, int, byte[])}.
         */
        @Override
        public void writeStartEntry( long timeWritten, long latestCommittedTxWhenStarted, int previousChecksum, byte[] additionalHeaderData ) throws IOException
        {
            byte nonExistingLogEntryVersion = (byte) (TransactionLogVersionSelector.LATEST.version() + 10);
            channel.put( nonExistingLogEntryVersion ).put( TX_START );
            channel.putLong( timeWritten )
                    .putLong( latestCommittedTxWhenStarted )
                    .putInt( previousChecksum )
                    .putInt( additionalHeaderData.length )
                    .put( additionalHeaderData, additionalHeaderData.length );
        }
    }

    private static class RecoveryMonitor implements org.neo4j.kernel.recovery.RecoveryMonitor
    {
        private final List<Long> recoveredTransactions = new ArrayList<>();
        private int numberOfRecoveredTransactions;
        private final AtomicBoolean recoveryRequired = new AtomicBoolean();

        @Override
        public void recoveryRequired( LogPosition recoveryPosition )
        {
            recoveryRequired.set( true );
        }

        @Override
        public void transactionRecovered( long txId )
        {
            recoveredTransactions.add( txId );
        }

        @Override
        public void recoveryCompleted( int numberOfRecoveredTransactions, long recoveryTimeInMilliseconds )
        {
            this.numberOfRecoveredTransactions = numberOfRecoveredTransactions;
        }

        boolean wasRecoveryRequired()
        {
            return recoveryRequired.get();
        }

        int getNumberOfRecoveredTransactions()
        {
            return numberOfRecoveredTransactions;
        }
    }

    private static class PositiveLogFilesBasedLogVersionRepository implements LogVersionRepository
    {
        private long version;
        private long checkpointVersion;

        PositiveLogFilesBasedLogVersionRepository( LogFiles logFiles )
        {
            this.version = (logFiles == null) ? 0 : logFiles.getLogFile().getHighestLogVersion();
        }

        @Override
        public long getCurrentLogVersion()
        {
            return version;
        }

        @Override
        public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
        {
            this.version = version;
        }

        @Override
        public long incrementAndGetVersion( PageCursorTracer cursorTracer )
        {
            version++;
            return version;
        }

        @Override
        public long getCheckpointLogVersion()
        {
            return checkpointVersion;
        }

        @Override
        public void setCheckpointLogVersion( long version, PageCursorTracer cursorTracer )
        {
            checkpointVersion = version;
        }

        @Override
        public long incrementAndGetCheckpointLogVersion( PageCursorTracer cursorTracer )
        {
            checkpointVersion++;
            return checkpointVersion;
        }
    }

    private static class BytesCaptureSupplier implements Supplier<Byte>
    {
        private final Supplier<Byte> generator;
        private final List<Byte> capturedBytes = new ArrayList<>();

        BytesCaptureSupplier( Supplier<Byte> generator )
        {
            this.generator = generator;
        }

        @Override
        public Byte get()
        {
            Byte data = generator.get();
            capturedBytes.add( data );
            return data;
        }

        public List<Byte> getCapturedBytes()
        {
            return capturedBytes;
        }
    }

    private static class DelegatingLogEntryWriter extends LogEntryWriter<FlushablePositionAwareChecksumChannel>
    {
        private final LogEntryWriter<FlushablePositionAwareChecksumChannel> delegate;

        DelegatingLogEntryWriter( LogEntryWriter<FlushablePositionAwareChecksumChannel> logEntryWriter )
        {
            super( logEntryWriter.getChannel() );
            this.delegate = logEntryWriter;
        }

        @Override
        public void writeLogEntryHeader( byte type, WritableChannel channel ) throws IOException
        {
            delegate.writeLogEntryHeader( type, channel );
        }

        @Override
        public void writeStartEntry( long timeWritten, long latestCommittedTxWhenStarted, int previousChecksum, byte[] additionalHeaderData ) throws IOException
        {
            delegate.writeStartEntry( timeWritten, latestCommittedTxWhenStarted, previousChecksum, additionalHeaderData );
        }

        @Override
        public int writeCommitEntry( long transactionId, long timeWritten ) throws IOException
        {
            return delegate.writeCommitEntry( transactionId, timeWritten );
        }

        @Override
        public void serialize( TransactionRepresentation tx ) throws IOException
        {
            delegate.serialize( tx );
        }

        @Override
        public void serialize( CommittedTransactionRepresentation tx ) throws IOException
        {
            delegate.serialize( tx );
        }

        @Override
        public void serialize( Collection<StorageCommand> commands ) throws IOException
        {
            delegate.serialize( commands );
        }

        @Override
        public void serialize( StorageCommand command ) throws IOException
        {
            delegate.serialize( command );
        }

        @Override
        public void writeLegacyCheckPointEntry( LogPosition logPosition ) throws IOException
        {
            delegate.writeLegacyCheckPointEntry( logPosition );
        }

        @Override
        public FlushablePositionAwareChecksumChannel getChannel()
        {
            return delegate.getChannel();
        }
    }
}
