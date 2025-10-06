# Databricks notebook source
# MAGIC %md
# MAGIC # Generic Mapping Engine for Any 2 Tables
# MAGIC 
# MAGIC **Author:** dangphdh  
# MAGIC **Purpose:** Reusable, optimized mapping function for any 2 tables on Databricks
# MAGIC 
# MAGIC ## Features:
# MAGIC - Generic interface for mapping any 2 tables
# MAGIC - Support for both Correlation and DTW methods
# MAGIC - Spark-optimized (partitioning, caching, broadcast joins)
# MAGIC - Flexible configuration
# MAGIC - Data validation and quality checks
# MAGIC - Production-ready error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lag, lead, row_number, rank, dense_rank,
    avg, sum as _sum, count, stddev, min as _min, max as _max,
    udf, pandas_udf, PandasUDFType, broadcast,
    struct, collect_list, array, explode, lit,
    monotonically_increasing_id, when, countDistinct,
    add_months, to_date, date_format, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, ArrayType, FloatType, LongType
)

import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from scipy.spatial.distance import euclidean
from typing import Iterator, List, Tuple, Dict, Optional
from datetime import datetime
import json

print("✅ Libraries Imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration Class

# COMMAND ----------

class MappingConfig:
    """
    Configuration for mapping operations
    
    Attributes:
        method: Mapping method - 'correlation', 'dtw', or 'auto'
        min_correlation: Minimum correlation threshold for correlation method
        max_lag: Maximum lag to consider (in months)
        window_size: Window size for DTW
        normalize: Whether to normalize data
        repartition_size: Number of partitions for Spark optimization
        cache_intermediate: Whether to cache intermediate results
        broadcast_threshold: Size threshold for broadcast joins (in bytes)
    """
    
    def __init__(
        self,
        method: str = 'auto',
        min_correlation: float = 0.3,
        max_lag: int = 6,
        window_size: Optional[int] = None,
        normalize: bool = True,
        repartition_size: int = 200,
        cache_intermediate: bool = True,
        broadcast_threshold: int = 10 * 1024 * 1024  # 10MB
    ):
        self.method = method
        self.min_correlation = min_correlation
        self.max_lag = max_lag
        self.window_size = window_size
        self.normalize = normalize
        self.repartition_size = repartition_size
        self.cache_intermediate = cache_intermediate
        self.broadcast_threshold = broadcast_threshold
        
        # Validate
        if method not in ['correlation', 'dtw', 'auto']:
            raise ValueError("method must be 'correlation', 'dtw', or 'auto'")
        if not 0 <= min_correlation <= 1:
            raise ValueError("min_correlation must be between 0 and 1")
        if max_lag < 1:
            raise ValueError("max_lag must be at least 1")
    
    def __repr__(self):
        return f"MappingConfig(method={self.method}, min_correlation={self.min_correlation}, max_lag={self.max_lag})"

print("✅ MappingConfig Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Utility Functions

# COMMAND ----------

def normalize_series(arr: np.ndarray) -> np.ndarray:
    """Normalize time series using z-score normalization"""
    mean = arr.mean()
    std = arr.std()
    if std == 0 or np.isnan(std):
        return arr - mean
    return (arr - mean) / std

def compute_correlation_with_lags(txn: np.ndarray, rev: np.ndarray, max_lag: int) -> Tuple[int, float]:
    """
    Compute correlation for different lags and return optimal lag
    
    Args:
        txn: Transaction amounts array
        rev: Revenue amounts array
        max_lag: Maximum lag to consider
    
    Returns:
        (optimal_lag, best_correlation)
    """
    if len(txn) < 2 or len(rev) < 2:
        return 0, 0.0
    
    best_lag = 0
    best_corr = -1
    
    for lag in range(max_lag + 1):
        if len(txn) - lag < 2 or len(rev) - lag < 2:
            continue
        
        # Align series with lag
        txn_lagged = txn[:-lag] if lag > 0 else txn
        rev_aligned = rev[lag:]
        
        # Ensure same length
        min_len = min(len(txn_lagged), len(rev_aligned))
        if min_len < 2:
            continue
        
        txn_lagged = txn_lagged[:min_len]
        rev_aligned = rev_aligned[:min_len]
        
        # Compute correlation
        try:
            corr, _ = pearsonr(txn_lagged, rev_aligned)
            if not np.isnan(corr) and corr > best_corr:
                best_corr = corr
                best_lag = lag
        except:
            continue
    
    return best_lag, best_corr

def dtw_distance(txn: np.ndarray, rev: np.ndarray, window: Optional[int] = None, 
                 normalize: bool = True) -> Tuple[float, List[Tuple[int, int]]]:
    """
    Compute DTW distance and optimal path
    
    Args:
        txn: Transaction time series
        rev: Revenue time series
        window: Sakoe-Chiba window constraint
        normalize: Whether to normalize data
    
    Returns:
        (total_distance, path)
    """
    if normalize:
        txn = normalize_series(txn)
        rev = normalize_series(rev)
    
    n, m = len(txn), len(rev)
    
    # Initialize DTW matrix
    dtw_matrix = np.full((n + 1, m + 1), np.inf)
    dtw_matrix[0, 0] = 0
    
    # Fill DTW matrix with window constraint
    for i in range(1, n + 1):
        start_j = max(1, i - window) if window else 1
        end_j = min(m + 1, i + window + 1) if window else m + 1
        
        for j in range(start_j, end_j):
            cost = abs(txn[i-1] - rev[j-1])
            dtw_matrix[i, j] = cost + min(
                dtw_matrix[i-1, j],      # insertion
                dtw_matrix[i, j-1],      # deletion
                dtw_matrix[i-1, j-1]     # match
            )
    
    # Backtrack to find path
    path = []
    i, j = n, m
    while i > 0 and j > 0:
        path.append((i-1, j-1))
        
        # Choose direction with minimum cost
        candidates = [
            (dtw_matrix[i-1, j], i-1, j),
            (dtw_matrix[i, j-1], i, j-1),
            (dtw_matrix[i-1, j-1], i-1, j-1)
        ]
        _, i, j = min(candidates, key=lambda x: x[0])
    
    path.reverse()
    total_distance = dtw_matrix[n, m]
    
    return total_distance, path

print("✅ Utility Functions Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pandas UDF for Correlation Analysis

# COMMAND ----------

def create_correlation_udf(max_lag: int = 6):
    """
    Factory function to create correlation UDF with configurable max_lag
    """
    
    correlation_result_schema = StructType([
        StructField("cus_code", StringType(), False),
        StructField("optimal_lag", IntegerType(), False),
        StructField("correlation", DoubleType(), False),
        StructField("num_months", IntegerType(), False),
        StructField("txn_mean", DoubleType(), False),
        StructField("rev_mean", DoubleType(), False),
    ])
    
    @pandas_udf(correlation_result_schema, PandasUDFType.GROUPED_MAP)
    def compute_correlation_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Compute correlation for a customer across different lags
        """
        cus_code = pdf['cus_code'].iloc[0]
        
        # Sort by month
        pdf = pdf.sort_values('month')
        
        txn = pdf['txn_amount'].values
        rev = pdf['revenue'].values
        
        # Compute optimal lag and correlation
        optimal_lag, best_corr = compute_correlation_with_lags(txn, rev, max_lag)
        
        return pd.DataFrame([{
            'cus_code': cus_code,
            'optimal_lag': int(optimal_lag),
            'correlation': float(best_corr),
            'num_months': len(pdf),
            'txn_mean': float(txn.mean()),
            'rev_mean': float(rev.mean())
        }])
    
    return compute_correlation_udf

print("✅ Correlation UDF Factory Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pandas UDF for DTW Analysis

# COMMAND ----------

def create_dtw_udf(window_size: Optional[int] = None, normalize: bool = True):
    """
    Factory function to create DTW UDF with configurable parameters
    """
    
    dtw_result_schema = StructType([
        StructField("cus_code", StringType(), False),
        StructField("total_cost", DoubleType(), False),
        StructField("num_mappings", IntegerType(), False),
        StructField("path", ArrayType(StructType([
            StructField("txn_idx", IntegerType(), False),
            StructField("rev_idx", IntegerType(), False),
            StructField("txn_month", StringType(), False),
            StructField("rev_month", StringType(), False),
            StructField("txn_amount", DoubleType(), False),
            StructField("revenue", DoubleType(), False)
        ])), False)
    ])
    
    @pandas_udf(dtw_result_schema, PandasUDFType.GROUPED_MAP)
    def compute_dtw_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Compute DTW alignment for a customer
        """
        cus_code = pdf['cus_code'].iloc[0]
        
        # Sort by month
        pdf = pdf.sort_values('month')
        
        txn = pdf['txn_amount'].values
        rev = pdf['revenue'].values
        months = pdf['month'].values
        
        # Compute DTW
        try:
            total_cost, path = dtw_distance(txn, rev, window=window_size, normalize=normalize)
        except:
            # Handle errors gracefully
            path = [(i, i) for i in range(min(len(txn), len(rev)))]
            total_cost = 0.0
        
        # Convert path to detailed mapping
        path_details = []
        for txn_idx, rev_idx in path:
            path_details.append({
                'txn_idx': int(txn_idx),
                'rev_idx': int(rev_idx),
                'txn_month': str(months[txn_idx]),
                'rev_month': str(months[rev_idx]),
                'txn_amount': float(txn[txn_idx]),
                'revenue': float(rev[rev_idx])
            })
        
        return pd.DataFrame([{
            'cus_code': cus_code,
            'total_cost': float(total_cost),
            'num_mappings': len(path),
            'path': path_details
        }])
    
    return compute_dtw_udf

print("✅ DTW UDF Factory Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Validator

# COMMAND ----------

class DataValidator:
    """
    Validate input tables for mapping
    """
    
    def __init__(self, spark):
        self.spark = spark
    
    def validate_schema(self, df: DataFrame, required_columns: List[str], 
                       table_name: str) -> bool:
        """
        Validate that DataFrame has required columns
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            table_name: Name for logging
        
        Returns:
            True if valid, raises ValueError otherwise
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"{table_name} missing required columns: {missing}")
        
        print(f"✅ {table_name} schema validated")
        return True
    
    def validate_data_quality(self, df: DataFrame, value_col: str, 
                             table_name: str) -> Dict:
        """
        Check data quality metrics
        
        Args:
            df: DataFrame to validate
            value_col: Column name for value checks
            table_name: Name for logging
        
        Returns:
            Dictionary with quality metrics
        """
        print(f"📊 Validating {table_name} data quality...")
        
        total_rows = df.count()
        null_count = df.filter(col(value_col).isNull()).count()
        negative_count = df.filter(col(value_col) < 0).count()
        
        metrics = {
            'total_rows': total_rows,
            'null_count': null_count,
            'null_percentage': (null_count / total_rows * 100) if total_rows > 0 else 0,
            'negative_count': negative_count,
            'negative_percentage': (negative_count / total_rows * 100) if total_rows > 0 else 0
        }
        
        # Print summary
        print(f"   Total rows: {total_rows:,}")
        print(f"   Null values: {null_count:,} ({metrics['null_percentage']:.2f}%)")
        print(f"   Negative values: {negative_count:,} ({metrics['negative_percentage']:.2f}%)")
        
        # Warnings
        if metrics['null_percentage'] > 10:
            print(f"   ⚠️  WARNING: High null percentage in {table_name}")
        if metrics['negative_percentage'] > 5:
            print(f"   ⚠️  WARNING: High negative values in {table_name}")
        
        return metrics

print("✅ DataValidator Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Main Mapping Engine

# COMMAND ----------

class GenericMappingEngine:
    """
    Generic mapping engine for any 2 tables on Databricks
    
    This class provides a reusable, optimized interface for mapping
    two time-series tables using correlation or DTW methods.
    """
    
    def __init__(self, spark, config: Optional[MappingConfig] = None):
        """
        Initialize mapping engine
        
        Args:
            spark: SparkSession
            config: MappingConfig instance (uses defaults if None)
        """
        self.spark = spark
        self.config = config or MappingConfig()
        self.validator = DataValidator(spark)
        self.stats = {}
        
        # Configure Spark for optimization
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark settings for optimal performance"""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(self.config.broadcast_threshold))
        
        print("✅ Spark optimizations configured")
    
    def map_tables(
        self,
        table1: DataFrame,
        table2: DataFrame,
        key_col: str,
        time_col: str,
        value1_col: str,
        value2_col: str,
        table1_name: str = "table1",
        table2_name: str = "table2"
    ) -> DataFrame:
        """
        Map two tables using configured method
        
        Args:
            table1: First DataFrame (e.g., transactions)
            table2: Second DataFrame (e.g., revenue)
            key_col: Column name for grouping key (e.g., 'cus_code', 'customer_id')
            time_col: Column name for time dimension (e.g., 'month', 'date')
            value1_col: Column name for value in table1 (e.g., 'txn_amount')
            value2_col: Column name for value in table2 (e.g., 'revenue')
            table1_name: Display name for table1
            table2_name: Display name for table2
        
        Returns:
            DataFrame with mapped results
        
        Examples:
            >>> config = MappingConfig(method='correlation', max_lag=3)
            >>> engine = GenericMappingEngine(spark, config)
            >>> result = engine.map_tables(
            ...     transactions, revenue,
            ...     key_col='customer_id',
            ...     time_col='month',
            ...     value1_col='amount',
            ...     value2_col='rev'
            ... )
        """
        print("\n" + "="*80)
        print(f"GENERIC MAPPING ENGINE: {table1_name} → {table2_name}")
        print("="*80)
        print(f"Configuration: {self.config}")
        
        start_time = datetime.now()
        
        # Step 1: Validate schemas
        print("\n📋 Step 1: Validating Schemas...")
        self.validator.validate_schema(table1, [key_col, time_col, value1_col], table1_name)
        self.validator.validate_schema(table2, [key_col, time_col, value2_col], table2_name)
        
        # Step 2: Validate data quality
        print("\n📊 Step 2: Checking Data Quality...")
        metrics1 = self.validator.validate_data_quality(table1, value1_col, table1_name)
        metrics2 = self.validator.validate_data_quality(table2, value2_col, table2_name)
        
        # Step 3: Prepare data
        print("\n🔧 Step 3: Preparing Data...")
        
        # Normalize column names for internal processing
        table1_prep = table1.select(
            col(key_col).alias('cus_code'),
            col(time_col).alias('month'),
            col(value1_col).alias('txn_amount')
        )
        
        table2_prep = table2.select(
            col(key_col).alias('cus_code'),
            col(time_col).alias('month'),
            col(value2_col).alias('revenue')
        )
        
        # Optimize partitioning
        table1_prep = table1_prep.repartition(self.config.repartition_size, 'cus_code')
        table2_prep = table2_prep.repartition(self.config.repartition_size, 'cus_code')
        
        if self.config.cache_intermediate:
            table1_prep.cache()
            table2_prep.cache()
            print("   ✅ Intermediate results cached")
        
        # Step 4: Merge tables
        print("\n🔗 Step 4: Merging Tables...")
        merged = table1_prep.alias('t1').join(
            table2_prep.alias('t2'),
            (col('t1.cus_code') == col('t2.cus_code')) &
            (col('t1.month') == col('t2.month')),
            'inner'
        ).select(
            col('t1.cus_code'),
            col('t1.month'),
            col('t1.txn_amount'),
            col('t2.revenue')
        )
        
        merged_count = merged.count()
        print(f"   Merged records: {merged_count:,}")
        
        # Step 5: Apply mapping method
        print(f"\n🎯 Step 5: Applying {self.config.method.upper()} Mapping...")
        
        if self.config.method == 'correlation':
            result = self._apply_correlation_mapping(merged, table1_prep, table2_prep)
        elif self.config.method == 'dtw':
            result = self._apply_dtw_mapping(merged)
        else:  # auto
            result = self._apply_auto_mapping(merged, table1_prep, table2_prep)
        
        # Step 6: Add metadata
        result = result.withColumn('processed_at', lit(datetime.now()))
        
        # Step 7: Statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result_count = result.count()
        
        print("\n" + "="*80)
        print("MAPPING COMPLETED")
        print("="*80)
        print(f"✅ Total mappings: {result_count:,}")
        print(f"✅ Processing time: {duration:.2f} seconds")
        print(f"✅ Throughput: {result_count/duration:.0f} records/second")
        print("="*80 + "\n")
        
        self.stats = {
            'total_mappings': result_count,
            'processing_time_seconds': duration,
            'throughput': result_count/duration,
            'input_table1_rows': metrics1['total_rows'],
            'input_table2_rows': metrics2['total_rows'],
            'merged_rows': merged_count
        }
        
        return result
    
    def _apply_correlation_mapping(self, merged: DataFrame, 
                                   table1: DataFrame, table2: DataFrame) -> DataFrame:
        """Apply correlation-based mapping"""
        print("   Computing correlations for all entities...")
        
        # Create correlation UDF
        correlation_udf = create_correlation_udf(max_lag=self.config.max_lag)
        
        # Apply UDF
        correlations = merged.groupBy('cus_code').apply(correlation_udf)
        
        # Filter by minimum correlation
        valid_correlations = correlations.filter(
            col('correlation') >= self.config.min_correlation
        )
        
        valid_count = valid_correlations.count()
        total_count = correlations.count()
        
        print(f"   Valid correlations: {valid_count:,} / {total_count:,} " +
              f"({valid_count/total_count*100:.1f}%)")
        
        # Join back with original data to create mappings
        table1_with_lag = table1.join(
            broadcast(valid_correlations.select('cus_code', 'optimal_lag')),
            'cus_code',
            'inner'
        )
        
        # Apply lag and join with table2
        mappings = table1_with_lag.alias('t').join(
            table2.alias('r'),
            (col('t.cus_code') == col('r.cus_code')) &
            (date_format(add_months(to_date(col('t.month')), col('t.optimal_lag')), 'yyyy-MM') 
             == col('r.month')),
            'inner'
        ).select(
            col('t.cus_code'),
            col('t.month').alias('time1'),
            col('t.txn_amount').alias('value1'),
            col('r.month').alias('time2'),
            col('r.revenue').alias('value2'),
            col('t.optimal_lag').alias('lag_offset'),
            lit('correlation').alias('method')
        )
        
        # Join with correlation scores
        mappings = mappings.join(
            broadcast(valid_correlations.select('cus_code', 'correlation')),
            'cus_code',
            'left'
        )
        
        return mappings
    
    def _apply_dtw_mapping(self, merged: DataFrame) -> DataFrame:
        """Apply DTW-based mapping"""
        print("   Computing DTW alignments for all entities...")
        
        # Create DTW UDF
        dtw_udf = create_dtw_udf(
            window_size=self.config.window_size,
            normalize=self.config.normalize
        )
        
        # Apply UDF
        dtw_results = merged.groupBy('cus_code').apply(dtw_udf)
        
        # Explode path to get individual mappings
        mappings = dtw_results.select(
            col('cus_code'),
            col('total_cost'),
            explode(col('path')).alias('mapping')
        ).select(
            col('cus_code'),
            col('mapping.txn_month').alias('time1'),
            col('mapping.txn_amount').alias('value1'),
            col('mapping.rev_month').alias('time2'),
            col('mapping.revenue').alias('value2'),
            lit(None).cast('int').alias('lag_offset'),
            lit('dtw').alias('method'),
            col('total_cost').alias('dtw_cost')
        )
        
        return mappings
    
    def _apply_auto_mapping(self, merged: DataFrame, 
                           table1: DataFrame, table2: DataFrame) -> DataFrame:
        """
        Automatically choose best method per entity
        
        Uses correlation for simple patterns, DTW for complex ones
        """
        print("   Auto-selecting optimal method per entity...")
        
        # Compute both methods
        correlation_udf = create_correlation_udf(max_lag=self.config.max_lag)
        correlations = merged.groupBy('cus_code').apply(correlation_udf)
        
        # Segment entities
        simple_entities = correlations.filter(
            col('correlation') >= 0.7  # High correlation = simple pattern
        )
        
        complex_entities = correlations.filter(
            col('correlation') < 0.7  # Low correlation = complex pattern
        )
        
        simple_count = simple_entities.count()
        complex_count = complex_entities.count()
        
        print(f"   Simple patterns (correlation): {simple_count:,}")
        print(f"   Complex patterns (DTW): {complex_count:,}")
        
        results = []
        
        # Process simple patterns with correlation
        if simple_count > 0:
            simple_merged = merged.join(
                simple_entities.select('cus_code'),
                'cus_code',
                'inner'
            )
            simple_result = self._apply_correlation_mapping(
                simple_merged, table1, table2
            )
            results.append(simple_result)
        
        # Process complex patterns with DTW
        if complex_count > 0:
            complex_merged = merged.join(
                complex_entities.select('cus_code'),
                'cus_code',
                'inner'
            )
            complex_result = self._apply_dtw_mapping(complex_merged)
            results.append(complex_result)
        
        # Union results
        if len(results) == 0:
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], schema=StructType([
                StructField('cus_code', StringType()),
                StructField('time1', StringType()),
                StructField('value1', DoubleType()),
                StructField('time2', StringType()),
                StructField('value2', DoubleType()),
                StructField('lag_offset', IntegerType()),
                StructField('method', StringType()),
            ]))
        elif len(results) == 1:
            return results[0]
        else:
            return results[0].unionByName(results[1], allowMissingColumns=True)
    
    def get_statistics(self) -> Dict:
        """Get processing statistics"""
        return self.stats

print("✅ GenericMappingEngine Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Convenience Functions

# COMMAND ----------

def map_tables_simple(
    spark,
    table1: DataFrame,
    table2: DataFrame,
    key_col: str,
    time_col: str,
    value1_col: str,
    value2_col: str,
    method: str = 'auto',
    max_lag: int = 6
) -> DataFrame:
    """
    Simplified interface for quick mapping
    
    Args:
        spark: SparkSession
        table1: First DataFrame
        table2: Second DataFrame
        key_col: Grouping key column name
        time_col: Time dimension column name
        value1_col: Value column in table1
        value2_col: Value column in table2
        method: 'correlation', 'dtw', or 'auto'
        max_lag: Maximum lag to consider
    
    Returns:
        Mapped DataFrame
    
    Example:
        >>> result = map_tables_simple(
        ...     spark, transactions, revenue,
        ...     key_col='customer_id',
        ...     time_col='month',
        ...     value1_col='amount',
        ...     value2_col='rev'
        ... )
    """
    config = MappingConfig(method=method, max_lag=max_lag)
    engine = GenericMappingEngine(spark, config)
    
    return engine.map_tables(
        table1, table2,
        key_col, time_col,
        value1_col, value2_col
    )

print("✅ Convenience Functions Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Example Usage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Basic Correlation Mapping
# MAGIC 
# MAGIC ```python
# MAGIC # Load your tables
# MAGIC transactions = spark.table("database.transactions")
# MAGIC revenue = spark.table("database.revenue")
# MAGIC 
# MAGIC # Simple usage
# MAGIC result = map_tables_simple(
# MAGIC     spark,
# MAGIC     transactions,
# MAGIC     revenue,
# MAGIC     key_col='customer_id',
# MAGIC     time_col='month',
# MAGIC     value1_col='txn_amount',
# MAGIC     value2_col='revenue',
# MAGIC     method='correlation',
# MAGIC     max_lag=3
# MAGIC )
# MAGIC 
# MAGIC # View results
# MAGIC display(result)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: DTW Mapping with Custom Configuration
# MAGIC 
# MAGIC ```python
# MAGIC # Custom configuration
# MAGIC config = MappingConfig(
# MAGIC     method='dtw',
# MAGIC     window_size=5,
# MAGIC     normalize=True,
# MAGIC     repartition_size=400,
# MAGIC     cache_intermediate=True
# MAGIC )
# MAGIC 
# MAGIC # Create engine
# MAGIC engine = GenericMappingEngine(spark, config)
# MAGIC 
# MAGIC # Map tables
# MAGIC result = engine.map_tables(
# MAGIC     table1=transactions,
# MAGIC     table2=revenue,
# MAGIC     key_col='customer_id',
# MAGIC     time_col='month',
# MAGIC     value1_col='amount',
# MAGIC     value2_col='rev',
# MAGIC     table1_name='Transactions',
# MAGIC     table2_name='Revenue'
# MAGIC )
# MAGIC 
# MAGIC # Get statistics
# MAGIC stats = engine.get_statistics()
# MAGIC print(stats)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Auto Method Selection
# MAGIC 
# MAGIC ```python
# MAGIC # Let the engine automatically choose the best method per entity
# MAGIC config = MappingConfig(
# MAGIC     method='auto',  # Automatically selects correlation or DTW
# MAGIC     max_lag=6
# MAGIC )
# MAGIC 
# MAGIC engine = GenericMappingEngine(spark, config)
# MAGIC 
# MAGIC result = engine.map_tables(
# MAGIC     sales_data,
# MAGIC     profit_data,
# MAGIC     key_col='product_id',
# MAGIC     time_col='week',
# MAGIC     value1_col='units_sold',
# MAGIC     value2_col='profit'
# MAGIC )
# MAGIC 
# MAGIC # Save results
# MAGIC result.write.format("delta").mode("overwrite").save("/path/to/output")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Production Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization Tips:
# MAGIC 
# MAGIC 1. **Partitioning**: Adjust `repartition_size` based on your cluster size
# MAGIC    - Small cluster (< 10 nodes): 100-200 partitions
# MAGIC    - Medium cluster (10-50 nodes): 200-400 partitions
# MAGIC    - Large cluster (> 50 nodes): 400+ partitions
# MAGIC 
# MAGIC 2. **Caching**: Enable `cache_intermediate` for iterative workloads
# MAGIC 
# MAGIC 3. **Broadcast Joins**: Adjust `broadcast_threshold` based on dimension table sizes
# MAGIC 
# MAGIC 4. **Method Selection**:
# MAGIC    - Use 'correlation' for simple, linear relationships (faster)
# MAGIC    - Use 'dtw' for complex, non-linear patterns (more accurate)
# MAGIC    - Use 'auto' to let the engine decide (balanced)
# MAGIC 
# MAGIC 5. **Monitoring**: Check statistics with `engine.get_statistics()`

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                    GENERIC MAPPING ENGINE READY                              ║
║                                                                              ║
║  ✅ Supports correlation and DTW mapping methods                             ║
║  ✅ Optimized for Spark with partitioning and caching                        ║
║  ✅ Flexible configuration for any use case                                  ║
║  ✅ Built-in data validation and quality checks                              ║
║  ✅ Production-ready with comprehensive error handling                       ║
║                                                                              ║
║  Use map_tables_simple() for quick mappings                                  ║
║  Use GenericMappingEngine for advanced control                              ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
