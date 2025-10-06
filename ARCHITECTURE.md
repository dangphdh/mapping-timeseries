# Architecture Overview - Generic Mapping Engine

## 📐 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User Interface                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Quick Interface:           Advanced Interface:                      │
│  map_tables_simple()        GenericMappingEngine class              │
│                                                                       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Configuration Layer                             │
│                      MappingConfig class                             │
│  • Method selection (correlation/dtw/auto)                          │
│  • Performance parameters (partitioning, caching)                   │
│  • Algorithm parameters (max_lag, window_size, etc.)               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Validation Layer                                │
│                      DataValidator class                             │
│  • Schema validation                                                │
│  • Data quality checks                                              │
│  • Null/negative value detection                                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Preparation Layer                            │
│  • Column normalization                                             │
│  • Spark optimization (repartitioning, caching)                     │
│  • Table merging                                                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Mapping Engine Core                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐      │
│  │ Correlation   │    │     DTW       │    │     Auto      │      │
│  │   Method      │    │   Method      │    │   Selection   │      │
│  │               │    │               │    │               │      │
│  │ • Fast        │    │ • Accurate    │    │ • Adaptive    │      │
│  │ • Linear      │    │ • Non-linear  │    │ • Best of     │      │
│  │ • Lag-based   │    │ • Path-based  │    │   both        │      │
│  └───────┬───────┘    └───────┬───────┘    └───────┬───────┘      │
│          │                    │                    │                │
│          └────────────────────┼────────────────────┘                │
│                               │                                     │
└───────────────────────────────┼─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Spark Optimization Layer                          │
│  • Pandas UDF for distributed processing                           │
│  • Broadcast joins for small tables                                │
│  • Partition-level parallelism                                     │
│  • Adaptive query execution                                         │
│  • Arrow-based data transfer                                        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Output Layer                                 │
│  • Standardized schema                                              │
│  • Metadata enrichment (method, correlation, cost)                 │
│  • Processing statistics                                            │
│  • Delta/Parquet export ready                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## 🔄 Processing Flow

### 1. Input Phase
```
User Tables (table1, table2)
     │
     ├─> Schema Validation
     ├─> Data Quality Checks
     └─> Column Mapping
```

### 2. Preparation Phase
```
Validated Tables
     │
     ├─> Normalize column names
     ├─> Repartition for parallel processing
     ├─> Cache if configured
     └─> Merge on key + time columns
```

### 3. Analysis Phase
```
Merged Data
     │
     ├─> Group by entity (key_col)
     │
     ├─> For each entity:
     │   │
     │   ├─> Correlation Method:
     │   │   ├─> Compute correlation for each lag
     │   │   ├─> Select optimal lag
     │   │   └─> Apply lag and join tables
     │   │
     │   ├─> DTW Method:
     │   │   ├─> Build cost matrix
     │   │   ├─> Find optimal path
     │   │   └─> Extract alignments
     │   │
     │   └─> Auto Method:
     │       ├─> Compute correlation
     │       ├─> If high correlation: use correlation
     │       └─> If low correlation: use DTW
     │
     └─> Combine results
```

### 4. Output Phase
```
Mapping Results
     │
     ├─> Add metadata (method, timestamps)
     ├─> Collect statistics
     └─> Return DataFrame
```

## 🧩 Component Details

### Core Components

| Component | File | Purpose | Key Features |
|-----------|------|---------|--------------|
| **MappingConfig** | mapping_engine.py | Configuration management | Parameter validation, defaults |
| **DataValidator** | mapping_engine.py | Input validation | Schema checks, quality metrics |
| **GenericMappingEngine** | mapping_engine.py | Main engine | Orchestration, method selection |
| **Correlation UDF** | mapping_engine.py | Correlation analysis | Pandas UDF, lag optimization |
| **DTW UDF** | mapping_engine.py | DTW analysis | Pandas UDF, path finding |

### Helper Functions

| Function | Purpose | Optimization |
|----------|---------|--------------|
| `normalize_series()` | Z-score normalization | Vectorized numpy |
| `compute_correlation_with_lags()` | Find optimal lag | Scipy correlation |
| `dtw_distance()` | Compute DTW path | Window constraint |
| `map_tables_simple()` | Simplified interface | Default config |

## 🚀 Performance Features

### Spark Optimizations

1. **Partitioning**
   - Automatic repartitioning by key column
   - Configurable partition count
   - Coalesce partitions after processing

2. **Caching**
   - Optional intermediate result caching
   - Automatic cache cleanup
   - Memory-aware caching

3. **Broadcast Joins**
   - Small table broadcasting
   - Configurable threshold
   - Dimension table optimization

4. **Parallel Processing**
   - Pandas UDF for distributed computation
   - Per-entity parallelism
   - Arrow-based data transfer

5. **Adaptive Execution**
   - Dynamic partition coalescing
   - Runtime query optimization
   - Skew handling

## 📊 Scalability

### Performance Characteristics

| Dataset Size | Method | Expected Throughput | Memory Usage |
|--------------|--------|-------------------|--------------|
| Small (< 1M rows) | Correlation | 5,000-10,000 rec/s | Low |
| Medium (1M-10M) | Correlation | 2,000-5,000 rec/s | Medium |
| Large (> 10M) | Correlation | 1,000-3,000 rec/s | Medium |
| Small (< 1M rows) | DTW | 500-1,000 rec/s | Medium |
| Medium (1M-10M) | DTW | 200-500 rec/s | High |
| Large (> 10M) | DTW | 100-300 rec/s | High |

### Scaling Recommendations

- **< 1M rows**: Default settings (200 partitions)
- **1M-10M rows**: 400 partitions, enable caching
- **10M-100M rows**: 800+ partitions, selective caching
- **> 100M rows**: Process in batches, use correlation method

## 🔐 Data Quality

### Validation Checks

1. **Schema Validation**
   - Required column presence
   - Data type compatibility
   - Column name mapping

2. **Quality Metrics**
   - Null value percentage
   - Negative value detection
   - Row count validation

3. **Warnings**
   - High null rate (> 10%)
   - High negative rate (> 5%)
   - Low match rate

## 📦 File Structure

```
mapping-timeseries/
├── mapping_engine.py          (31KB) - Main engine implementation
├── test_mapping_engine.py     (8.6KB) - Comprehensive tests
├── example_usage.py           (5.2KB) - Quick start example
├── README.md                  (9.9KB) - Documentation
├── .gitignore                 - Exclude build artifacts
├── 01_setup.oy               - Original setup (reference)
├── 02_corr_mapping.p         - Original correlation notebook
├── 03_dtw_mapping.py         - Original DTW notebook
├── 04_comparision.py         - Original comparison notebook
└── 05_prod_dev.py            - Original production notebook
```

## 🎯 Use Case Examples

### 1. Customer Transaction → Revenue
```python
# Map customer transactions to revenue with 1-3 month lag
result = map_tables_simple(
    spark, transactions, revenue,
    key_col='customer_id', time_col='month',
    value1_col='txn_amount', value2_col='revenue',
    method='correlation', max_lag=3
)
```

### 2. Product Sales → Profit (Complex Pattern)
```python
# Use DTW for non-linear sales-to-profit relationship
config = MappingConfig(method='dtw', window_size=7)
engine = GenericMappingEngine(spark, config)
result = engine.map_tables(
    sales, profit,
    key_col='product_id', time_col='week',
    value1_col='units_sold', value2_col='profit'
)
```

### 3. Marketing Spend → Conversions (Mixed)
```python
# Auto-select method based on pattern complexity
result = map_tables_simple(
    spark, marketing_spend, conversions,
    key_col='campaign_id', time_col='date',
    value1_col='spend', value2_col='conversions',
    method='auto'
)
```

## 🔧 Customization Points

### Extending the Engine

1. **Add New Mapping Methods**
   - Implement method in `GenericMappingEngine`
   - Create corresponding UDF factory
   - Update config options

2. **Custom Validation Rules**
   - Extend `DataValidator` class
   - Add validation methods
   - Update validation pipeline

3. **Performance Tuning**
   - Adjust `MappingConfig` parameters
   - Modify partitioning strategy
   - Configure caching policy

---

**Created by:** dangphdh  
**Last Updated:** 2025-01-06  
**Version:** 1.0.0
