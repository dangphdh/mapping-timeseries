# Quick Reference - Generic Mapping Engine

## 🚀 Quick Start

### 1. Import
```python
%run ./mapping_engine
```

### 2. Basic Usage (5 lines)
```python
result = map_tables_simple(
    spark, table1, table2,
    key_col='customer_id', time_col='month',
    value1_col='amount', value2_col='revenue'
)
```

## 📚 Common Patterns

### Pattern 1: Correlation with Custom Lag
```python
result = map_tables_simple(
    spark, transactions, revenue,
    key_col='customer_id', time_col='month',
    value1_col='txn_amount', value2_col='revenue',
    method='correlation',
    max_lag=3  # Consider 0-3 months lag
)
```

### Pattern 2: DTW for Complex Patterns
```python
config = MappingConfig(
    method='dtw',
    window_size=5,      # Constraint window
    normalize=True
)
engine = GenericMappingEngine(spark, config)
result = engine.map_tables(
    sales, profit,
    key_col='product_id', time_col='week',
    value1_col='units', value2_col='profit'
)
```

### Pattern 3: Auto Selection
```python
result = map_tables_simple(
    spark, table1, table2,
    key_col='id', time_col='date',
    value1_col='val1', value2_col='val2',
    method='auto'  # Let engine decide
)
```

### Pattern 4: High Performance Setup
```python
config = MappingConfig(
    method='correlation',
    max_lag=6,
    repartition_size=400,      # Match cluster cores
    cache_intermediate=True,    # Enable caching
    broadcast_threshold=20*1024*1024  # 20MB
)
engine = GenericMappingEngine(spark, config)
result = engine.map_tables(...)
```

## 🎛️ Configuration Presets

### Fast & Simple
```python
config = MappingConfig(
    method='correlation',
    max_lag=3,
    repartition_size=100,
    cache_intermediate=False
)
```

### Accurate & Complex
```python
config = MappingConfig(
    method='dtw',
    window_size=7,
    normalize=True,
    repartition_size=400
)
```

### Balanced (Auto)
```python
config = MappingConfig(
    method='auto',
    max_lag=6,
    min_correlation=0.3,
    repartition_size=200
)
```

## 📊 Output Schema

| Column | Type | Description |
|--------|------|-------------|
| cus_code | string | Entity ID |
| time1 | string | Time from table1 |
| value1 | double | Value from table1 |
| time2 | string | Time from table2 |
| value2 | double | Value from table2 |
| lag_offset | int | Lag in time units (correlation) |
| method | string | Method used |
| correlation | double | Correlation score (if correlation) |
| dtw_cost | double | DTW cost (if DTW) |
| processed_at | timestamp | Processing time |

## 🔍 Common Operations

### Check Results
```python
# Count mappings
result.count()

# Preview
display(result.limit(10))

# Summary stats
result.groupBy('method').agg(
    count('*').alias('count'),
    avg('correlation').alias('avg_corr')
).show()
```

### Save Results
```python
# Save as Delta
result.write.format("delta").mode("overwrite") \
    .save("/path/to/output")

# Save as table
result.write.format("delta").mode("overwrite") \
    .saveAsTable("database.table_name")

# Partition by entity
result.write.format("delta").mode("overwrite") \
    .partitionBy("cus_code") \
    .save("/path/to/output")
```

### Get Statistics
```python
stats = engine.get_statistics()
print(f"Total: {stats['total_mappings']:,}")
print(f"Time: {stats['processing_time_seconds']:.2f}s")
print(f"Throughput: {stats['throughput']:.0f} rec/s")
```

## ⚡ Performance Tips

### Cluster Size
- Small (< 10 nodes): `repartition_size=100-200`
- Medium (10-50 nodes): `repartition_size=200-400`
- Large (> 50 nodes): `repartition_size=400+`

### Method Selection
- **Use Correlation when:**
  - Patterns are linear
  - Speed is important
  - Data has clear lag pattern
  
- **Use DTW when:**
  - Patterns are complex
  - Accuracy is critical
  - Non-linear relationships
  
- **Use Auto when:**
  - Mixed pattern complexity
  - Uncertain about patterns

### Memory Management
```python
# Enable caching for small datasets
config = MappingConfig(cache_intermediate=True)

# Disable caching for large datasets
config = MappingConfig(cache_intermediate=False)

# Adjust broadcast threshold
config = MappingConfig(
    broadcast_threshold=50*1024*1024  # 50MB
)
```

## 🐛 Troubleshooting

### Error: "Missing required columns"
```python
# Check your column names
table1.columns  # List all columns
table2.columns

# Use exact names from your tables
result = map_tables_simple(
    spark, table1, table2,
    key_col='actual_column_name',  # Use exact name
    ...
)
```

### Slow Performance
```python
# Increase partitions
config = MappingConfig(repartition_size=400)

# Use faster method
config = MappingConfig(method='correlation')

# Disable caching for very large data
config = MappingConfig(cache_intermediate=False)
```

### Low Correlation Scores
```python
# Increase max lag
config = MappingConfig(max_lag=12)

# Try DTW instead
config = MappingConfig(method='dtw')

# Or use auto selection
config = MappingConfig(method='auto')
```

### Out of Memory
```python
# Process in batches
customers = table1.select('customer_id').distinct()
batch_size = 100

for i in range(0, customers.count(), batch_size):
    batch = customers.limit(batch_size).offset(i)
    batch_table1 = table1.join(batch, 'customer_id')
    batch_table2 = table2.join(batch, 'customer_id')
    
    result = map_tables_simple(
        spark, batch_table1, batch_table2, ...
    )
    
    # Save batch results
    result.write.mode('append').save(...)
```

## 📖 Example Workflows

### Workflow 1: Explore → Test → Production
```python
# 1. Explore with sample
sample1 = table1.sample(0.1)  # 10% sample
sample2 = table2.sample(0.1)

result_test = map_tables_simple(
    spark, sample1, sample2,
    key_col='id', time_col='month',
    value1_col='val1', value2_col='val2',
    method='auto'
)

# 2. Review results
display(result_test)

# 3. Run on full data with optimized config
config = MappingConfig(
    method='correlation',  # Based on test
    max_lag=3,
    repartition_size=400,
    cache_intermediate=True
)

result_prod = map_tables_simple(...)
result_prod.write.format("delta").save(...)
```

### Workflow 2: Compare Methods
```python
# Try both methods
result_corr = map_tables_simple(
    spark, table1, table2, ...,
    method='correlation'
)

result_dtw = map_tables_simple(
    spark, table1, table2, ...,
    method='dtw'
)

# Compare
print("Correlation mappings:", result_corr.count())
print("DTW mappings:", result_dtw.count())

# Choose best
final_result = result_corr  # or result_dtw
```

## 🔗 Links

- **Tutorial**: `databricks_intro_tutorial.py` - **Start here!** Comprehensive Databricks tutorial
- Main Documentation: `README.md`
- Architecture: `ARCHITECTURE.md`
- Tests: `test_mapping_engine.py`
- Examples: `example_usage.py`

## 💡 Tips

1. **Start simple**: Use `map_tables_simple()` first
2. **Test with samples**: Use `.sample()` for quick testing
3. **Monitor performance**: Check `engine.get_statistics()`
4. **Save incrementally**: Write results in batches for large data
5. **Use Delta format**: Better performance than Parquet for updates

---

**Need Help?** See `databricks_intro_tutorial.py` for a complete tutorial, `README.md` for detailed documentation, or `example_usage.py` for quick examples.
