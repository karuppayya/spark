# ShuffleVault Pluggable Design

## Overview

This document describes the pluggable design for ShuffleVault, which enables remote shuffle storage (S3) support in Apache Spark without requiring core modifications. This approach uses Spark's existing extension points to make the feature configurable via the `spark.shuffle.manager` setting.

## Architecture

### Components

1. **ShuffleVaultHandle** (`core/src/main/scala/org/apache/spark/shuffle/vault/ShuffleVaultHandle.scala`)
   - A marker `ShuffleHandle` subclass that indicates a shuffle should use remote storage
   - Wraps the underlying shuffle handle from the base shuffle manager
   - Pattern matches on this type allow code to route to remote storage paths

2. **ShuffleVaultManager** (`core/src/main/scala/org/apache/spark/shuffle/vault/ShuffleVaultManager.scala`)
   - A custom `ShuffleManager` implementation that wraps `SortShuffleManager`
   - Intercepts `registerShuffle()` calls and returns `ShuffleVaultHandle` when remote storage is requested
   - Delegates all other operations to the base shuffle manager
   - Fully backward compatible - falls back to standard shuffle when not enabled

3. **ShuffleDependency.isRemoteShuffleStorage** (helper method in `Dependency.scala`)
   - Provides a clean API to check if a shuffle uses remote storage
   - Checks if `shuffleHandle` is an instance of `ShuffleVaultHandle`
   - Used throughout the codebase instead of directly accessing the boolean field

## Usage

### Enabling ShuffleVault

Users enable ShuffleVault by configuring the shuffle manager:

```scala
spark.conf.set("spark.shuffle.manager",
  "org.apache.spark.shuffle.vault.ShuffleVaultManager")
```

### How It Works

1. **Shuffle Registration Flow**:
   ```
   SQL Layer creates ShuffleDependency
     └─> Sets useRemoteShuffleStorage = true (for PassThroughPartitioning)
         └─> ShuffleDependency calls shuffleManager.registerShuffle()
             └─> ShuffleVaultManager checks useRemoteShuffleStorage
                 ├─> If true: returns ShuffleVaultHandle
                 └─> If false: returns standard handle (BaseShuffleHandle, etc.)
   ```

2. **Runtime Detection**:
   ```scala
   // Shuffle writers and other components check handle type
   if (dependency.isRemoteShuffleStorage) {
     // Use remote storage (S3) path
     blockManagerId = RemoteShuffleStorage.BLOCK_MANAGER_ID
   } else {
     // Use local storage path
     blockManagerId = blockManager.shuffleServerId
   }
   ```

## Code Changes

### New Files

- `core/src/main/scala/org/apache/spark/shuffle/vault/ShuffleVaultHandle.scala`
- `core/src/main/scala/org/apache/spark/shuffle/vault/ShuffleVaultManager.scala`

### Modified Files

1. **core/src/main/scala/org/apache/spark/Dependency.scala**
   - Added `isRemoteShuffleStorage` helper method to check handle type
   - Existing `useRemoteShuffleStorage` parameter remains for backward compatibility

2. **core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala**
   - Changed `dep.useRemoteShuffleStorage` → `dep.isRemoteShuffleStorage`

3. **core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala**
   - Changed `s.shuffleDep.useRemoteShuffleStorage` → `s.shuffleDep.isRemoteShuffleStorage`

4. **core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala**
   - Changed `dep.useRemoteShuffleStorage` → `dep.isRemoteShuffleStorage`

5. **core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java**
   - Changed `dep.useRemoteShuffleStorage()` → `dep.isRemoteShuffleStorage()`

6. **core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java**
   - Changed `dep.useRemoteShuffleStorage()` → `dep.isRemoteShuffleStorage()`

## Benefits

### ✅ No Core Modifications Required
- The feature is completely pluggable via configuration
- Standard Spark distributions work without any changes when ShuffleVault is disabled
- Can be maintained and distributed separately from Apache Spark

### ✅ Follows Spark Design Patterns
- Uses the same pattern as `BypassMergeSortShuffleHandle` and `SerializedShuffleHandle`
- Leverages Spark's existing `ShuffleManager` extension point
- Type-safe with pattern matching on handle types

### ✅ Backward Compatible
- When ShuffleVaultManager is not configured, Spark uses standard shuffle manager
- When configured but remote storage is not requested, falls back to local shuffle
- No breaking changes to existing APIs

### ✅ Community Friendly
- Can be proposed to the community as an optional feature
- Easy to maintain as an external plugin if not accepted upstream
- Clear separation between core Spark and ShuffleVault logic

### ✅ Clean API
- `dependency.isRemoteShuffleStorage` provides a clear, self-documenting check
- Encapsulates implementation details (handle type checking)
- Future-proof: can change implementation without affecting call sites

## Future Enhancements

### Custom Shuffle Writers/Readers
The `ShuffleVaultManager` can be extended to provide custom writers and readers:

```scala
override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

  handle match {
    case vaultHandle: ShuffleVaultHandle[K @unchecked, V @unchecked, _] =>
      // Return custom writer that writes directly to S3
      new ShuffleVaultWriter(vaultHandle, mapId, context, metrics)
    case _ =>
      // Standard local shuffle
      sortShuffleManager.getWriter(handle, mapId, context, metrics)
  }
}
```

### Configuration Options
Additional configuration can be added to control ShuffleVault behavior:

```properties
# Enable/disable ShuffleVault globally
spark.shuffle.vault.enabled=true

# S3 bucket configuration
spark.shuffle.vault.s3.bucket=my-shuffle-bucket
spark.shuffle.vault.s3.prefix=shuffles/

# Threshold for using remote storage (shuffle size, partition count, etc.)
spark.shuffle.vault.minPartitions=1000
```

## Migration Path

For teams currently using `useRemoteShuffleStorage` in a forked Spark:

1. **Phase 1 (Current)**: Add ShuffleVaultHandle and ShuffleVaultManager, update call sites to use `isRemoteShuffleStorage`
2. **Phase 2**: Configure `spark.shuffle.manager` to use `ShuffleVaultManager` in production
3. **Phase 3**: Optionally remove `useRemoteShuffleStorage` parameter if it's no longer needed for construction
4. **Phase 4**: Implement custom writers/readers if needed for optimized remote storage access

## Testing

Test that the pluggable design works correctly:

1. **With ShuffleVaultManager disabled** (default):
   ```scala
   // Uses standard SortShuffleManager
   // All shuffles use local storage
   ```

2. **With ShuffleVaultManager enabled**:
   ```scala
   spark.conf.set("spark.shuffle.manager",
     "org.apache.spark.shuffle.vault.ShuffleVaultManager")

   // Shuffles with useRemoteShuffleStorage=true get ShuffleVaultHandle
   // Shuffles with useRemoteShuffleStorage=false get standard handles
   ```

3. **Verify handle type checking**:
   ```scala
   assert(dependency.isRemoteShuffleStorage ==
     dependency.shuffleHandle.isInstanceOf[ShuffleVaultHandle[_, _, _]])
   ```
