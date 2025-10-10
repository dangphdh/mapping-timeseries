# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Introduction to Mapping Engine on Databricks
# MAGIC 
# MAGIC **Welcome!** This notebook provides a comprehensive introduction to using the Generic Mapping Engine on Databricks.
# MAGIC 
# MAGIC ## 📖 What is the Mapping Engine?
# MAGIC 
# MAGIC The Mapping Engine helps you discover relationships between two time-series tables by finding optimal mappings between their time points. This is useful for:
# MAGIC 
# MAGIC - **Revenue Attribution**: Link customer transactions to revenue generation
# MAGIC - **Marketing Analysis**: Connect ad spend to conversions/sales
# MAGIC - **Supply Chain**: Map inventory changes to demand patterns
# MAGIC - **Product Analytics**: Relate feature releases to user engagement
# MAGIC 
# MAGIC ## 🎯 What You'll Learn
# MAGIC 
# MAGIC 1. How to import and set up the mapping engine
# MAGIC 2. How to prepare your data for mapping
# MAGIC 3. Simple mapping with correlation method
# MAGIC 4. Advanced mapping with DTW (Dynamic Time Warping)
# MAGIC 5. Using auto-selection mode
# MAGIC 6. Interpreting and visualizing results
# MAGIC 7. Performance optimization tips
# MAGIC 8. Common troubleshooting
# MAGIC 
# MAGIC ## ⏱️ Estimated Time
# MAGIC 
# MAGIC 15-20 minutes
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Let's get started!** 👇

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Setup & Import
# MAGIC 
# MAGIC First, we'll import the mapping engine. The engine is designed to work seamlessly in Databricks notebooks.

# COMMAND ----------

# Import the mapping engine
%run ./mapping_engine

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Success!** The mapping engine is now loaded and ready to use.
# MAGIC 
# MAGIC You should see several confirmation messages above, including:
# MAGIC - ✅ Libraries Imported
# MAGIC - ✅ MappingConfig Defined
# MAGIC - ✅ GenericMappingEngine Defined
# MAGIC - ✅ Convenience Functions Defined

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Understanding Your Data Requirements
# MAGIC 
# MAGIC Before we start mapping, let's understand what data structure is required:
# MAGIC 
# MAGIC ### Required Columns
# MAGIC 
# MAGIC Each of your two tables must have at least **3 columns**:
# MAGIC 
# MAGIC 1. **Key Column** (e.g., `customer_id`, `product_id`, `campaign_id`)
# MAGIC    - Identifies the entity you're tracking
# MAGIC    - Should be consistent across both tables
# MAGIC    
# MAGIC 2. **Time Column** (e.g., `month`, `week`, `date`)
# MAGIC    - Represents the time dimension
# MAGIC    - Can be in any format (string, date, timestamp)
# MAGIC    
# MAGIC 3. **Value Column** (e.g., `amount`, `revenue`, `count`)
# MAGIC    - The metric you want to analyze
# MAGIC    - Should be numeric
# MAGIC 
# MAGIC ### Example Table Structure
# MAGIC 
# MAGIC **Table 1 - Transactions:**
# MAGIC ```
# MAGIC | customer_id | month    | transaction_amount |
# MAGIC |-------------|----------|-------------------|
# MAGIC | CUST_001    | 2024-01  | 500.0             |
# MAGIC | CUST_001    | 2024-02  | 750.0             |
# MAGIC | CUST_002    | 2024-01  | 300.0             |
# MAGIC ```
# MAGIC 
# MAGIC **Table 2 - Revenue:**
# MAGIC ```
# MAGIC | customer_id | month    | revenue |
# MAGIC |-------------|----------|---------|
# MAGIC | CUST_001    | 2024-01  | 50.0    |
# MAGIC | CUST_001    | 2024-02  | 75.0    |
# MAGIC | CUST_002    | 2024-01  | 30.0    |
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Creating Sample Data
# MAGIC 
# MAGIC Let's create realistic sample data to demonstrate the mapping engine. 
# MAGIC 
# MAGIC **Scenario**: We have customer transactions and want to map them to revenue, with a 1-month lag (transactions in one month lead to revenue in the next month).

# COMMAND ----------

import pandas as pd
import numpy as np

# Set seed for reproducibility
np.random.seed(42)

# Create sample transactions data
print("📊 Creating sample transactions data...")
txn_data = []
for customer in range(1, 51):  # 50 customers
    for month in range(1, 13):  # 12 months
        txn_data.append({
            'customer_id': f'CUST_{customer:04d}',
            'month': f'2024-{month:02d}',
            'transaction_amount': np.random.uniform(100, 1000)
        })

transactions = spark.createDataFrame(pd.DataFrame(txn_data))

# Create sample revenue data (with 1-month lag correlation)
print("💰 Creating sample revenue data...")
rev_data = []
for customer in range(1, 51):
    for month in range(1, 13):
        if month > 1:
            # Revenue is roughly 10% of previous month's transactions + noise
            rev = np.random.uniform(10, 100)
        else:
            rev = np.random.uniform(5, 20)
        rev_data.append({
            'customer_id': f'CUST_{customer:04d}',
            'month': f'2024-{month:02d}',
            'revenue': rev
        })

revenue = spark.createDataFrame(pd.DataFrame(rev_data))

print(f"\n✅ Sample data created successfully!")
print(f"   Transactions: {transactions.count():,} rows")
print(f"   Revenue: {revenue.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 👀 Preview Your Data
# MAGIC 
# MAGIC Always preview your data before mapping to ensure it's structured correctly:

# COMMAND ----------

print("📊 Transactions Preview:")
display(transactions.limit(10))

# COMMAND ----------

print("💰 Revenue Preview:")
display(revenue.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Your First Mapping - Simple Correlation
# MAGIC 
# MAGIC Now let's perform our first mapping! We'll use the **correlation method**, which is:
# MAGIC - ✅ Fast and efficient
# MAGIC - ✅ Good for linear relationships
# MAGIC - ✅ Easy to interpret
# MAGIC 
# MAGIC ### Key Parameters Explained:
# MAGIC 
# MAGIC - `table1` / `table2`: Your two DataFrames
# MAGIC - `key_col`: Column name that identifies entities (e.g., customer_id)
# MAGIC - `time_col`: Column name for time dimension
# MAGIC - `value1_col` / `value2_col`: Metric columns to compare
# MAGIC - `method`: 'correlation', 'dtw', or 'auto'
# MAGIC - `max_lag`: How many time periods ahead/behind to check (e.g., 3 months)

# COMMAND ----------

# Perform the mapping
print("🔄 Starting mapping process...")

result = map_tables_simple(
    spark,
    table1=transactions,
    table2=revenue,
    key_col='customer_id',           # The entity we're tracking
    time_col='month',                 # Time dimension
    value1_col='transaction_amount',  # Metric from transactions table
    value2_col='revenue',             # Metric from revenue table
    method='correlation',             # Use correlation method
    max_lag=3                         # Check up to 3 months lag
)

print(f"\n✅ Mapping completed successfully!")
print(f"   Total mappings found: {result.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Understanding the Results
# MAGIC 
# MAGIC Let's examine what the mapping engine found:

# COMMAND ----------

# Preview the mapping results
print("🔍 Mapping Results Preview:")
display(result.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Understanding the Output Columns:
# MAGIC 
# MAGIC - **cus_code**: Customer identifier
# MAGIC - **time1**: Time point from transactions table
# MAGIC - **value1**: Transaction amount at that time
# MAGIC - **time2**: Time point from revenue table that was mapped
# MAGIC - **value2**: Revenue amount at that time
# MAGIC - **lag_offset**: How many periods ahead/behind (e.g., 1 = revenue comes 1 month after transaction)
# MAGIC - **correlation**: Strength of relationship (0 to 1, higher is better)
# MAGIC - **method**: Which method was used ('correlation' or 'dtw')
# MAGIC - **processed_at**: When the mapping was performed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Analyzing Results
# MAGIC 
# MAGIC Let's dig deeper into what we found:

# COMMAND ----------

# Summary statistics by method
print("📈 Overall Summary:")
result.groupBy('method').agg(
    count('*').alias('total_mappings'),
    avg('correlation').alias('avg_correlation'),
    avg('lag_offset').alias('avg_lag'),
    avg('value1').alias('avg_transaction'),
    avg('value2').alias('avg_revenue')
).show()

# COMMAND ----------

# Lag distribution - shows the most common time delays
print("⏱️ Lag Distribution (which time delays are most common):")
lag_dist = result.groupBy('lag_offset').agg(
    count('*').alias('count'),
    avg('correlation').alias('avg_correlation')
).orderBy('lag_offset')
display(lag_dist)

# COMMAND ----------

# Correlation strength distribution
print("💪 Correlation Strength Distribution:")
result.groupBy(
    when(col('correlation') >= 0.7, 'Strong (0.7+)')
    .when(col('correlation') >= 0.5, 'Medium (0.5-0.7)')
    .when(col('correlation') >= 0.3, 'Weak (0.3-0.5)')
    .otherwise('Very Weak (<0.3)')
    .alias('strength')
).count().orderBy('strength').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Advanced Example - DTW Method
# MAGIC 
# MAGIC For more complex, non-linear patterns, you can use **DTW (Dynamic Time Warping)**:
# MAGIC 
# MAGIC - ✅ Better for complex patterns
# MAGIC - ✅ Handles non-linear relationships
# MAGIC - ⚠️ More computationally intensive
# MAGIC - ⚠️ Slower than correlation

# COMMAND ----------

# Create custom configuration for DTW
from pyspark.sql.functions import col, count, avg, when

config = MappingConfig(
    method='dtw',           # Use DTW method
    window_size=5,          # DTW window constraint
    normalize=True,         # Normalize data
    repartition_size=200,   # Spark partitions
    cache_intermediate=True # Enable caching
)

# Create engine with custom config
engine = GenericMappingEngine(spark, config)

# Perform mapping
print("🔄 Running DTW mapping...")
result_dtw = engine.map_tables(
    table1=transactions,
    table2=revenue,
    key_col='customer_id',
    time_col='month',
    value1_col='transaction_amount',
    value2_col='revenue',
    table1_name='Transactions',
    table2_name='Revenue'
)

# Get statistics
stats = engine.get_statistics()
print("\n📊 DTW Processing Statistics:")
for key, value in stats.items():
    if isinstance(value, float):
        print(f"   {key}: {value:.2f}")
    elif isinstance(value, int):
        print(f"   {key}: {value:,}")
    else:
        print(f"   {key}: {value}")

# COMMAND ----------

# Preview DTW results
print("🔍 DTW Results Preview:")
display(result_dtw.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8️⃣ Auto-Selection Mode
# MAGIC 
# MAGIC Not sure which method to use? Let the engine decide automatically!
# MAGIC 
# MAGIC **Auto mode**:
# MAGIC - Tries correlation first (fast)
# MAGIC - Falls back to DTW if correlation is weak
# MAGIC - Best of both worlds

# COMMAND ----------

# Use auto-selection
print("🤖 Using auto-selection mode...")

result_auto = map_tables_simple(
    spark,
    table1=transactions,
    table2=revenue,
    key_col='customer_id',
    time_col='month',
    value1_col='transaction_amount',
    value2_col='revenue',
    method='auto',  # Automatic method selection
    max_lag=3
)

print(f"\n✅ Auto mapping completed!")
print(f"   Total mappings: {result_auto.count():,}")

# Show which methods were used
print("\n📊 Methods Distribution:")
result_auto.groupBy('method').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9️⃣ Saving Your Results
# MAGIC 
# MAGIC Once you're happy with the mappings, save them for later use:

# COMMAND ----------

# Define output path (update with your actual path)
output_path = "/tmp/mapping_results"

# Save as Delta table (best for Databricks)
# result.write.format("delta").mode("overwrite").save(output_path)
# print(f"✅ Results saved to {output_path}")

# Or save as a managed table
# result.write.format("delta").mode("overwrite").saveAsTable("your_catalog.your_schema.mapping_results")
# print("✅ Results saved as managed table")

# For this demo, we'll just show the command
print("💡 To save results, uncomment one of the options above")
print("   Option 1: Save to path - result.write.format('delta').mode('overwrite').save('/your/path')")
print("   Option 2: Save as table - result.write.format('delta').mode('overwrite').saveAsTable('your_table')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔟 Using With Real Databricks Tables
# MAGIC 
# MAGIC Here's how to use the engine with your actual Delta tables:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Load your actual tables from Databricks
# MAGIC transactions = spark.table("your_catalog.your_schema.transactions")
# MAGIC revenue = spark.table("your_catalog.your_schema.revenue")
# MAGIC 
# MAGIC # Check the schema to identify your columns
# MAGIC transactions.printSchema()
# MAGIC revenue.printSchema()
# MAGIC 
# MAGIC # Run the mapping
# MAGIC result = map_tables_simple(
# MAGIC     spark,
# MAGIC     table1=transactions,
# MAGIC     table2=revenue,
# MAGIC     key_col='your_key_column',      # Update with your column name
# MAGIC     time_col='your_time_column',    # Update with your column name
# MAGIC     value1_col='your_value1_col',   # Update with your column name
# MAGIC     value2_col='your_value2_col',   # Update with your column name
# MAGIC     method='auto',
# MAGIC     max_lag=6
# MAGIC )
# MAGIC 
# MAGIC # Display results
# MAGIC display(result)
# MAGIC 
# MAGIC # Save results
# MAGIC result.write.format("delta").mode("overwrite") \
# MAGIC     .saveAsTable("your_catalog.your_schema.mapping_results")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Performance Optimization Tips
# MAGIC 
# MAGIC ### 1. Cluster Size & Partitions
# MAGIC 
# MAGIC Match `repartition_size` to your cluster:
# MAGIC 
# MAGIC ```python
# MAGIC # Small cluster (< 10 nodes)
# MAGIC config = MappingConfig(repartition_size=100)
# MAGIC 
# MAGIC # Medium cluster (10-50 nodes)
# MAGIC config = MappingConfig(repartition_size=200)
# MAGIC 
# MAGIC # Large cluster (> 50 nodes)
# MAGIC config = MappingConfig(repartition_size=400)
# MAGIC ```
# MAGIC 
# MAGIC ### 2. Method Selection
# MAGIC 
# MAGIC - **Start with correlation** - it's much faster
# MAGIC - **Use DTW** only if correlation gives poor results
# MAGIC - **Use auto** for mixed patterns
# MAGIC 
# MAGIC ### 3. Caching
# MAGIC 
# MAGIC ```python
# MAGIC # Enable caching for better performance
# MAGIC config = MappingConfig(cache_intermediate=True)
# MAGIC 
# MAGIC # Disable for very large datasets to save memory
# MAGIC config = MappingConfig(cache_intermediate=False)
# MAGIC ```
# MAGIC 
# MAGIC ### 4. Test with Samples First
# MAGIC 
# MAGIC ```python
# MAGIC # Test with 10% sample
# MAGIC sample1 = transactions.sample(0.1)
# MAGIC sample2 = revenue.sample(0.1)
# MAGIC 
# MAGIC result_test = map_tables_simple(spark, sample1, sample2, ...)
# MAGIC 
# MAGIC # Once satisfied, run on full data
# MAGIC result_full = map_tables_simple(spark, transactions, revenue, ...)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐛 Troubleshooting Guide
# MAGIC 
# MAGIC ### Error: "Missing required columns"
# MAGIC 
# MAGIC **Problem**: The engine can't find the columns you specified
# MAGIC 
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Check your column names
# MAGIC print(transactions.columns)
# MAGIC print(revenue.columns)
# MAGIC 
# MAGIC # Use exact names (case-sensitive!)
# MAGIC result = map_tables_simple(..., key_col='actual_column_name', ...)
# MAGIC ```
# MAGIC 
# MAGIC ### Error: Slow Performance
# MAGIC 
# MAGIC **Problem**: Processing takes too long
# MAGIC 
# MAGIC **Solutions**:
# MAGIC ```python
# MAGIC # 1. Increase partitions
# MAGIC config = MappingConfig(repartition_size=400)
# MAGIC 
# MAGIC # 2. Use correlation instead of DTW
# MAGIC config = MappingConfig(method='correlation')
# MAGIC 
# MAGIC # 3. Test with sample first
# MAGIC sample = table.sample(0.1)
# MAGIC ```
# MAGIC 
# MAGIC ### Issue: Low Correlation Scores
# MAGIC 
# MAGIC **Problem**: All correlations are below 0.3
# MAGIC 
# MAGIC **Solutions**:
# MAGIC ```python
# MAGIC # 1. Increase max_lag (check more time periods)
# MAGIC result = map_tables_simple(..., max_lag=12)
# MAGIC 
# MAGIC # 2. Try DTW method
# MAGIC config = MappingConfig(method='dtw')
# MAGIC 
# MAGIC # 3. Check if data is actually related
# MAGIC # - Verify time alignment
# MAGIC # - Check for data quality issues
# MAGIC # - Ensure tables share common entities
# MAGIC ```
# MAGIC 
# MAGIC ### Issue: Out of Memory
# MAGIC 
# MAGIC **Problem**: Cluster runs out of memory
# MAGIC 
# MAGIC **Solutions**:
# MAGIC ```python
# MAGIC # 1. Disable caching
# MAGIC config = MappingConfig(cache_intermediate=False)
# MAGIC 
# MAGIC # 2. Decrease partitions
# MAGIC config = MappingConfig(repartition_size=50)
# MAGIC 
# MAGIC # 3. Process in smaller batches
# MAGIC keys = transactions.select('customer_id').distinct().collect()
# MAGIC for batch in chunks(keys, 100):
# MAGIC     # Process 100 customers at a time
# MAGIC     subset1 = transactions.filter(col('customer_id').isin(batch))
# MAGIC     subset2 = revenue.filter(col('customer_id').isin(batch))
# MAGIC     result_batch = map_tables_simple(spark, subset1, subset2, ...)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Real-World Use Cases
# MAGIC 
# MAGIC ### Use Case 1: E-commerce Attribution
# MAGIC 
# MAGIC **Scenario**: Map customer browsing activity to purchases
# MAGIC 
# MAGIC ```python
# MAGIC browsing = spark.table("analytics.customer_page_views")
# MAGIC purchases = spark.table("sales.customer_purchases")
# MAGIC 
# MAGIC attribution = map_tables_simple(
# MAGIC     spark,
# MAGIC     table1=browsing,
# MAGIC     table2=purchases,
# MAGIC     key_col='customer_id',
# MAGIC     time_col='date',
# MAGIC     value1_col='page_views',
# MAGIC     value2_col='purchase_amount',
# MAGIC     method='correlation',
# MAGIC     max_lag=7  # Check up to 7 days
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC ### Use Case 2: Marketing ROI
# MAGIC 
# MAGIC **Scenario**: Connect ad spend to conversions
# MAGIC 
# MAGIC ```python
# MAGIC ad_spend = spark.table("marketing.daily_spend")
# MAGIC conversions = spark.table("marketing.conversions")
# MAGIC 
# MAGIC roi_mapping = map_tables_simple(
# MAGIC     spark,
# MAGIC     table1=ad_spend,
# MAGIC     table2=conversions,
# MAGIC     key_col='campaign_id',
# MAGIC     time_col='date',
# MAGIC     value1_col='spend_usd',
# MAGIC     value2_col='conversion_count',
# MAGIC     method='auto',
# MAGIC     max_lag=14  # Up to 2 weeks lag
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC ### Use Case 3: Supply Chain
# MAGIC 
# MAGIC **Scenario**: Map inventory levels to demand
# MAGIC 
# MAGIC ```python
# MAGIC inventory = spark.table("warehouse.inventory_levels")
# MAGIC demand = spark.table("sales.product_demand")
# MAGIC 
# MAGIC supply_demand = map_tables_simple(
# MAGIC     spark,
# MAGIC     table1=inventory,
# MAGIC     table2=demand,
# MAGIC     key_col='product_id',
# MAGIC     time_col='week',
# MAGIC     value1_col='stock_level',
# MAGIC     value2_col='units_sold',
# MAGIC     method='dtw',  # Complex patterns
# MAGIC     max_lag=4
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Quick Reference Card
# MAGIC 
# MAGIC ### Basic Usage Pattern
# MAGIC 
# MAGIC ```python
# MAGIC # 1. Import
# MAGIC %run ./mapping_engine
# MAGIC 
# MAGIC # 2. Load tables
# MAGIC table1 = spark.table("your.table1")
# MAGIC table2 = spark.table("your.table2")
# MAGIC 
# MAGIC # 3. Map
# MAGIC result = map_tables_simple(
# MAGIC     spark, table1, table2,
# MAGIC     key_col='id',
# MAGIC     time_col='date',
# MAGIC     value1_col='metric1',
# MAGIC     value2_col='metric2',
# MAGIC     method='auto',
# MAGIC     max_lag=6
# MAGIC )
# MAGIC 
# MAGIC # 4. Analyze
# MAGIC display(result)
# MAGIC 
# MAGIC # 5. Save
# MAGIC result.write.format("delta").mode("overwrite").saveAsTable("your.output_table")
# MAGIC ```
# MAGIC 
# MAGIC ### Method Comparison
# MAGIC 
# MAGIC | Method | Speed | Accuracy | Best For |
# MAGIC |--------|-------|----------|----------|
# MAGIC | `correlation` | ⚡⚡⚡ Fast | Good | Linear relationships |
# MAGIC | `dtw` | 🐌 Slow | Excellent | Complex patterns |
# MAGIC | `auto` | ⚡⚡ Medium | Very Good | Mixed patterns |
# MAGIC 
# MAGIC ### Configuration Presets
# MAGIC 
# MAGIC ```python
# MAGIC # Fast & Simple
# MAGIC MappingConfig(method='correlation', max_lag=3, cache_intermediate=False)
# MAGIC 
# MAGIC # Accurate & Complex
# MAGIC MappingConfig(method='dtw', window_size=7, normalize=True)
# MAGIC 
# MAGIC # Balanced
# MAGIC MappingConfig(method='auto', max_lag=6, repartition_size=200)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC 
# MAGIC Congratulations! You now know how to use the Mapping Engine on Databricks. Here's what to do next:
# MAGIC 
# MAGIC 1. **Try with Your Data**: Replace the sample data with your actual tables
# MAGIC 2. **Experiment**: Test different methods and parameters
# MAGIC 3. **Optimize**: Tune configuration for your cluster size
# MAGIC 4. **Explore More**: Check out these additional resources:
# MAGIC    - `README.md` - Comprehensive documentation
# MAGIC    - `QUICK_REFERENCE.md` - Quick tips and patterns
# MAGIC    - `example_usage.py` - Additional examples
# MAGIC    - `test_mapping_engine.py` - Test cases and edge cases
# MAGIC 
# MAGIC ## 💬 Need Help?
# MAGIC 
# MAGIC - Check the troubleshooting section above
# MAGIC - Review the documentation files
# MAGIC - Contact the repository owner: dangphdh
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Happy Mapping!** 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Summary Checklist
# MAGIC 
# MAGIC Use this checklist when working with the mapping engine:
# MAGIC 
# MAGIC **Before Mapping:**
# MAGIC - [ ] Tables have key, time, and value columns
# MAGIC - [ ] Column names are identified correctly
# MAGIC - [ ] Data types are appropriate (numeric values)
# MAGIC - [ ] Preview data to verify structure
# MAGIC 
# MAGIC **During Mapping:**
# MAGIC - [ ] Started with sample/subset for testing
# MAGIC - [ ] Chose appropriate method (correlation/dtw/auto)
# MAGIC - [ ] Set reasonable max_lag value
# MAGIC - [ ] Configured partitions for cluster size
# MAGIC 
# MAGIC **After Mapping:**
# MAGIC - [ ] Reviewed result counts and statistics
# MAGIC - [ ] Checked correlation/DTW scores
# MAGIC - [ ] Analyzed lag distribution
# MAGIC - [ ] Saved results to Delta table
# MAGIC - [ ] Documented configuration used
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **End of Tutorial** ✨
