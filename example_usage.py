# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Start Example - Generic Mapping Engine
# MAGIC 
# MAGIC This notebook demonstrates how to quickly use the generic mapping engine
# MAGIC to map any two tables on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Import the Mapping Engine

# COMMAND ----------

# MAGIC %run ./mapping_engine

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Your Tables
# MAGIC 
# MAGIC Replace this with your actual tables from Databricks

# COMMAND ----------

# Example: Load from Delta tables
# transactions = spark.table("your_database.transactions")
# revenue = spark.table("your_database.revenue")

# For this demo, we'll create sample data
import pandas as pd
import numpy as np

np.random.seed(42)

# Create sample transactions
txn_data = []
for cus in range(1, 101):
    for month in range(1, 13):
        txn_data.append({
            'customer_id': f'CUST_{cus:04d}',
            'month': f'2024-{month:02d}',
            'transaction_amount': np.random.uniform(100, 1000)
        })

transactions = spark.createDataFrame(pd.DataFrame(txn_data))

# Create sample revenue (with 1-month lag)
rev_data = []
for cus in range(1, 101):
    for month in range(1, 13):
        if month > 1:
            # Revenue is roughly 10% of previous month's transactions
            rev = np.random.uniform(10, 100)
        else:
            rev = 0
        rev_data.append({
            'customer_id': f'CUST_{cus:04d}',
            'month': f'2024-{month:02d}',
            'revenue': rev
        })

revenue = spark.createDataFrame(pd.DataFrame(rev_data))

print("✅ Sample data created")
print(f"Transactions: {transactions.count():,} rows")
print(f"Revenue: {revenue.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Map the Tables
# MAGIC 
# MAGIC Use the simple interface to map your tables

# COMMAND ----------

# Map transactions to revenue
result = map_tables_simple(
    spark,
    table1=transactions,
    table2=revenue,
    key_col='customer_id',      # Column that identifies each entity (customer, product, etc.)
    time_col='month',            # Column representing time
    value1_col='transaction_amount',  # Value column in first table
    value2_col='revenue',        # Value column in second table
    method='correlation',        # Method: 'correlation', 'dtw', or 'auto'
    max_lag=3                    # Maximum time lag to consider (in months)
)

print("\n✅ Mapping completed!")
print(f"Total mappings: {result.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: View Results

# COMMAND ----------

# Show sample results
display(result.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Analyze Results

# COMMAND ----------

# Summary statistics
print("📊 Mapping Summary:")
result.groupBy('method').agg(
    count('*').alias('total_mappings'),
    avg('correlation').alias('avg_correlation'),
    avg('lag_offset').alias('avg_lag'),
    avg('value1').alias('avg_transaction'),
    avg('value2').alias('avg_revenue')
).show()

# Distribution of lag offsets
print("\n📊 Lag Distribution:")
result.groupBy('lag_offset').agg(
    count('*').alias('count')
).orderBy('lag_offset').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save Results (Optional)

# COMMAND ----------

# Save to Delta table
output_path = "/mnt/data/mapped_results"

# Uncomment to save
# result.write.format("delta").mode("overwrite").save(output_path)
# print(f"✅ Results saved to {output_path}")

# Or save as a table
# result.write.format("delta").mode("overwrite").saveAsTable("your_database.mapped_results")
# print("✅ Results saved as table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced: Using Custom Configuration

# COMMAND ----------

# For more control, use custom configuration
config = MappingConfig(
    method='auto',              # Automatically choose best method per customer
    max_lag=6,                  # Consider up to 6 months lag
    min_correlation=0.3,        # Minimum correlation threshold
    repartition_size=200,       # Number of Spark partitions
    cache_intermediate=True     # Cache for better performance
)

engine = GenericMappingEngine(spark, config)

# Map with custom config
result_advanced = engine.map_tables(
    table1=transactions,
    table2=revenue,
    key_col='customer_id',
    time_col='month',
    value1_col='transaction_amount',
    value2_col='revenue',
    table1_name='Transactions',
    table2_name='Revenue'
)

# Get processing statistics
stats = engine.get_statistics()
print("\n📈 Processing Statistics:")
for key, value in stats.items():
    if isinstance(value, float):
        print(f"   {key}: {value:.2f}")
    else:
        print(f"   {key}: {value:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Replace sample data with your actual tables
# MAGIC 2. Adjust configuration parameters based on your data
# MAGIC 3. Experiment with different methods ('correlation', 'dtw', 'auto')
# MAGIC 4. Save and use the mapping results in your downstream analysis
# MAGIC 
# MAGIC For more examples, see `test_mapping_engine.py`
# MAGIC 
# MAGIC For detailed documentation, see `README.md`
