# WeightedFIFO Scheduling Algorithm Usage

## Overview
WeightedFIFO is a configurable scheduling algorithm that extends standard FIFO scheduling by considering task weights. It's now implemented as a separate, pluggable component.

## Configuration

To enable WeightedFIFO scheduling:

### 1. Register the Algorithm Provider

Add the WeightedFIFOAlgorithmProvider to your Spark configuration:

```properties
spark.scheduler.algorithm.providers=org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider
```

### 2. Set the Scheduling Mode

Set the scheduling mode to WEIGHTED_FIFO:

```properties
spark.scheduler.mode=WEIGHTED_FIFO
```

### 3. (Optional) Configure Weight Provider

By default, all TaskSets have weight=1. To customize weights, configure a `TaskSetWeightProvider`:

#### Using RemoteShuffleWeightProvider (Built-in)

For prioritizing remote shuffle stages:

```properties
spark.scheduler.taskset.weight.provider.class=org.apache.spark.scheduler.RemoteShuffleWeightProvider
```

This gives remote shuffle stages (identified by `remote=true` property) a weight of 1000, while local stages get weight of 1.

#### Using Custom Weight Provider

```properties
spark.scheduler.taskset.weight.provider.class=com.example.MyWeightProvider
```

## Example Configurations

### Configuration 1: WeightedFIFO with RemoteShuffleWeightProvider

```scala
val conf = new SparkConf()
  .setAppName("WeightedFIFO with Remote Shuffle Priority")
  .set("spark.scheduler.algorithm.providers",
       "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")
  .set("spark.scheduler.mode", "WEIGHTED_FIFO")
  .set("spark.scheduler.taskset.weight.provider.class",
       "org.apache.spark.scheduler.RemoteShuffleWeightProvider")

val sc = new SparkContext(conf)
```

### Configuration 2: WeightedFIFO with Custom Weight Provider

```scala
val conf = new SparkConf()
  .setAppName("WeightedFIFO Example")
  .set("spark.scheduler.algorithm.providers",
       "org.apache.spark.scheduler.WeightedFIFOAlgorithmProvider")
  .set("spark.scheduler.mode", "WEIGHTED_FIFO")
  .set("spark.scheduler.taskset.weight.provider.class",
       "com.example.CustomWeightProvider")

val sc = new SparkContext(conf)
```

## Scheduling Priority Order

WeightedFIFO orders TaskSets by:
1. **Priority** (lower value = higher priority)
2. **Weight** (higher value = higher priority)
3. **Stage ID** (lower value = higher priority)

## Built-in Weight Providers

### RemoteShuffleWeightProvider

Prioritizes remote shuffle stages over local stages:
- **Remote stages** (shuffles reading from S3 via ShuffleVaultManager): weight = 1000
- **Local stages** (regular shuffles): weight = 1

This is useful for jobs that read shuffle data from remote storage (e.g., S3) to reduce I/O latency by starting remote I/O operations earlier.

**How it works:**
The provider queries the `ShuffleVaultManager` to determine if a shuffle is registered for remote storage. When a stage reads from a shuffle that was written to S3, it's automatically detected and given higher priority.

**Requirements:**
- Must use `ShuffleVaultManager` as the shuffle manager:
  ```properties
  spark.shuffle.manager=org.apache.spark.shuffle.vault.ShuffleVaultManager
  ```
- The ShuffleVaultManager automatically detects and registers shuffles that should use remote storage based on the operation scope (e.g., "Consolidation exchange" for SQL shuffle consolidation)

**Automatic detection:**
No additional configuration needed beyond the shuffle manager and weight provider. The system automatically detects which shuffles use remote storage and prioritizes stages that read from them.

## Custom Weight Provider Examples

### Example 1: Weight Based on Task Count

```scala
class TaskCountWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSetManager: TaskSetManager): Int = {
    // Higher number of tasks = higher priority
    taskSetManager.numTasks * 10
  }
}
```

### Example 2: Weight Based on Stage ID

```scala
class StageIdWeightProvider extends TaskSetWeightProvider {
  override def getWeight(taskSetManager: TaskSetManager): Int = {
    // Later stages get higher priority
    taskSetManager.stageId
  }
}
```

## Architecture

The WeightedFIFO implementation consists of:
- `WeightedFIFOSchedulingAlgorithm.scala` - The scheduling algorithm logic
- `WeightedFIFOAlgorithmProvider` - Provider for registering the algorithm
- Configuration properties for enabling and customizing behavior

This modular design allows the algorithm to be:
- Independently maintained
- Easily enabled/disabled via configuration
- Extended with custom weight providers
