# Generic Mapping Engine for Databricks

## 📋 Overview

The Generic Mapping Engine provides a reusable, Spark-optimized solution for mapping any two time-series tables on Databricks. It supports multiple mapping algorithms (Correlation and DTW) and is designed for production use with built-in data validation, performance optimization, and comprehensive error handling.

## 🚀 Features

- **Generic Interface**: Works with any two tables - just specify column names
- **Multiple Methods**:
  - **Correlation**: Fast, efficient for simple linear relationships
  - **DTW (Dynamic Time Warping)**: Accurate for complex, non-linear patterns
  - **Auto**: Automatically selects the best method per entity
- **Spark Optimized**: 
  - Automatic partitioning and repartitioning
  - Intelligent caching
  - Broadcast join optimization
  - Adaptive query execution
- **Data Validation**: Built-in schema and quality checks
- **Production Ready**: Comprehensive error handling and monitoring
- **Flexible Configuration**: Customizable for different use cases

## 📦 Files

- `mapping_engine.py` - Main mapping engine implementation
- `databricks_intro_tutorial.py` - **NEW!** Comprehensive Databricks tutorial for beginners
- `example_usage.py` - Quick start examples
- `test_mapping_engine.py` - Comprehensive test suite and examples
- `README.md` - This documentation
- `QUICK_REFERENCE.md` - Quick reference guide
- `ARCHITECTURE.md` - Architecture documentation

## 🎯 Quick Start

### 🆕 New to the Mapping Engine? Start Here!

**For a complete step-by-step tutorial**, check out the new Databricks introduction notebook:

```python
%run ./databricks_intro_tutorial
```

This interactive tutorial covers:
- ✅ Complete setup and data preparation
- ✅ Step-by-step mapping examples  
- ✅ All three methods (correlation, DTW, auto)
- ✅ Result analysis and visualization
- ✅ Performance optimization tips
- ✅ Troubleshooting guide
- ✅ Real-world use cases

**Estimated time:** 15-20 minutes

### Basic Usage

```python
# Import the engine
%run ./mapping_engine

# Map two tables
result = map_tables_simple(
    spark,
    transactions,      # Your first table
    revenue,          # Your second table
    key_col='customer_id',
    time_col='month',
    value1_col='amount',
    value2_col='revenue',
    method='auto',    # or 'correlation' or 'dtw'
    max_lag=6
)

# View results
display(result)
```

## 📖 Detailed Usage

### 1. Simple Interface

The simplest way to use the mapping engine:

```python
result = map_tables_simple(
    spark,
    table1=transactions,
    table2=revenue,
    key_col='customer_id',    # Grouping key (e.g., customer, product)
    time_col='month',          # Time dimension
    value1_col='txn_amount',   # Value column in table1
    value2_col='revenue',      # Value column in table2
    method='correlation',      # 'correlation', 'dtw', or 'auto'
    max_lag=3                  # Maximum time lag to consider
)
```

### 2. Advanced Configuration

For more control, use the configuration class:

```python
# Create custom configuration
config = MappingConfig(
    method='dtw',               # Mapping method
    max_lag=6,                  # Maximum lag (months)
    window_size=5,              # DTW window constraint
    normalize=True,             # Normalize data
    min_correlation=0.3,        # Minimum correlation threshold
    repartition_size=400,       # Number of Spark partitions
    cache_intermediate=True,    # Cache intermediate results
    broadcast_threshold=10*1024*1024  # Broadcast join threshold (bytes)
)

# Create engine with custom config
engine = GenericMappingEngine(spark, config)

# Map tables
result = engine.map_tables(
    table1=transactions,
    table2=revenue,
    key_col='customer_id',
    time_col='month',
    value1_col='amount',
    value2_col='rev',
    table1_name='Transactions',  # Optional display name
    table2_name='Revenue'        # Optional display name
)

# Get processing statistics
stats = engine.get_statistics()
print(stats)
```

### 3. Auto Method Selection

Let the engine automatically choose the best method for each entity:

```python
config = MappingConfig(
    method='auto',  # Automatically chooses correlation or DTW
    max_lag=6
)

engine = GenericMappingEngine(spark, config)
result = engine.map_tables(...)
```

## 🔧 Configuration Parameters

### MappingConfig Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `method` | str | 'auto' | Mapping method: 'correlation', 'dtw', or 'auto' |
| `min_correlation` | float | 0.3 | Minimum correlation threshold (0-1) |
| `max_lag` | int | 6 | Maximum time lag to consider |
| `window_size` | int | None | DTW window constraint (Sakoe-Chiba band) |
| `normalize` | bool | True | Normalize data before processing |
| `repartition_size` | int | 200 | Number of Spark partitions |
| `cache_intermediate` | bool | True | Cache intermediate results |
| `broadcast_threshold` | int | 10MB | Size threshold for broadcast joins |

## 📊 Output Schema

The mapping result includes:

| Column | Type | Description |
|--------|------|-------------|
| `cus_code` | String | Entity identifier (from key_col) |
| `time1` | String | Time value from table1 |
| `value1` | Double | Value from table1 |
| `time2` | String | Time value from table2 |
| `value2` | Double | Value from table2 |
| `lag_offset` | Integer | Time lag (correlation method only) |
| `method` | String | Mapping method used ('correlation' or 'dtw') |
| `correlation` | Double | Correlation score (correlation method only) |
| `dtw_cost` | Double | DTW cost (DTW method only) |
| `processed_at` | Timestamp | Processing timestamp |

## 🎓 Use Cases

### Use Case 1: Customer Transaction to Revenue Mapping

```python
# Map customer transactions to revenue
result = map_tables_simple(
    spark,
    customer_transactions,
    customer_revenue,
    key_col='customer_id',
    time_col='month',
    value1_col='transaction_amount',
    value2_col='revenue',
    method='correlation',
    max_lag=3
)

# Save results
result.write.format("delta").mode("overwrite") \
    .save("/mnt/data/customer_txn_revenue_mapping")
```

### Use Case 2: Product Sales to Profit Mapping

```python
# Map product sales to profit with complex patterns
config = MappingConfig(
    method='dtw',
    window_size=7,
    normalize=True
)

engine = GenericMappingEngine(spark, config)

result = engine.map_tables(
    product_sales,
    product_profit,
    key_col='product_id',
    time_col='week',
    value1_col='units_sold',
    value2_col='profit'
)
```

### Use Case 3: Marketing Spend to Conversion Mapping

```python
# Automatically choose best method per campaign
result = map_tables_simple(
    spark,
    marketing_spend,
    conversions,
    key_col='campaign_id',
    time_col='date',
    value1_col='spend',
    value2_col='conversions',
    method='auto'
)
```

## ⚡ Performance Optimization

### Cluster Size Guidelines

Adjust `repartition_size` based on cluster:

- **Small cluster** (< 10 nodes): 100-200 partitions
- **Medium cluster** (10-50 nodes): 200-400 partitions
- **Large cluster** (> 50 nodes): 400+ partitions

### Method Selection

- **Correlation**: Faster, best for simple linear relationships
  - Use when: Patterns are predictable and linear
  - Performance: ~1000-5000 records/second
  
- **DTW**: More accurate, best for complex patterns
  - Use when: Patterns are non-linear or irregular
  - Performance: ~100-500 records/second (more computationally intensive)
  
- **Auto**: Balanced approach
  - Use when: Mixed pattern complexity
  - Automatically uses correlation for simple patterns, DTW for complex ones

### Optimization Tips

1. **Enable Caching**: Set `cache_intermediate=True` for iterative workloads
2. **Adjust Partitioning**: Match `repartition_size` to cluster cores
3. **Broadcast Small Tables**: Increase `broadcast_threshold` for larger dimension tables
4. **Use Correlation First**: Try correlation method first, fall back to DTW if needed

## 🧪 Testing

Run the test suite:

```python
%run ./test_mapping_engine
```

The test suite includes:
- Simple interface tests
- DTW method tests
- Auto method selection tests
- Performance benchmarks
- Data validation tests
- Edge case handling

## 📈 Monitoring

Get processing statistics:

```python
engine = GenericMappingEngine(spark, config)
result = engine.map_tables(...)

# Get statistics
stats = engine.get_statistics()
print(f"Total mappings: {stats['total_mappings']:,}")
print(f"Processing time: {stats['processing_time_seconds']:.2f}s")
print(f"Throughput: {stats['throughput']:.0f} records/sec")
```

## 🔒 Data Validation

The engine automatically validates:

1. **Schema Validation**: Ensures required columns exist
2. **Data Quality Checks**: 
   - Null value detection
   - Negative value detection
   - Data type validation
3. **Warnings**: Alerts for high null percentages (>10%) or negative values (>5%)

## 🛠️ Troubleshooting

### Common Issues

**Issue**: "Missing required columns"
- **Solution**: Ensure your tables have the columns specified in `key_col`, `time_col`, `value1_col`, `value2_col`

**Issue**: Slow performance
- **Solution**: 
  - Increase `repartition_size`
  - Use correlation method instead of DTW
  - Enable caching with `cache_intermediate=True`

**Issue**: Out of memory
- **Solution**:
  - Decrease `repartition_size`
  - Disable caching with `cache_intermediate=False`
  - Process data in smaller batches

**Issue**: Low correlation scores
- **Solution**:
  - Try DTW method instead
  - Increase `max_lag`
  - Check data quality and alignment

## 📝 Examples

See `test_mapping_engine.py` for comprehensive examples including:
- Basic correlation mapping
- DTW mapping with custom configuration
- Auto method selection
- Performance testing
- Custom column names
- Edge cases

## 🤝 Contributing

To extend the mapping engine:

1. Add new mapping methods in the `GenericMappingEngine` class
2. Create corresponding UDF factories
3. Update configuration options in `MappingConfig`
4. Add tests in `test_mapping_engine.py`

## 📄 License

This code is part of the mapping-timeseries project.

## 👥 Author

**dangphdh**  
For questions or issues, please contact the repository owner.

---

## 🎯 Quick Reference

```python
# Import
%run ./mapping_engine

# Simple usage
result = map_tables_simple(
    spark, table1, table2,
    key_col='id', time_col='date',
    value1_col='val1', value2_col='val2'
)

# Advanced usage
config = MappingConfig(method='dtw', max_lag=6)
engine = GenericMappingEngine(spark, config)
result = engine.map_tables(table1, table2, ...)
stats = engine.get_statistics()

# Save results
result.write.format("delta").save("/path/to/output")
```
