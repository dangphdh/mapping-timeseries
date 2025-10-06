# Databricks notebook source
# MAGIC %md
# MAGIC # Correlation-Based Mapping
# MAGIC Thuật toán đơn giản dùng correlation để tìm lag tối ưu

# COMMAND ----------

# MAGIC %run ./01_setup_and_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define Correlation UDF

# COMMAND ----------

# Schema cho kết quả correlation
correlation_result_schema = StructType([
    StructField("cus_code", StringType(), False),
    StructField("optimal_lag", IntegerType(), False),
    StructField("correlation", DoubleType(), False),
    StructField("num_months", IntegerType(), False),
    StructField("txn_mean", DoubleType(), False),
    StructField("rev_mean", DoubleType(), False),
    StructField("all_lags", ArrayType(StructType([
        StructField("lag", IntegerType(), False),
        StructField("corr", DoubleType(), False),
        StructField("pvalue", DoubleType(), False)
    ])), False)
])

@pandas_udf(correlation_result_schema, PandasUDFType.GROUPED_MAP)
def compute_correlation_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas UDF để tính correlation với các lag khác nhau
    
    Input: DataFrame với columns [cus_code, month, txn_amount, revenue]
    Output: DataFrame với optimal lag và correlation
    """
    cus_code = pdf['cus_code'].iloc[0]
    
    # Sort by month
    pdf = pdf.sort_values('month')
    
    txn = pdf['txn_amount'].values
    rev = pdf['revenue'].values
    
    if len(txn) < 3:
        # Không đủ data
        return pd.DataFrame([{
            'cus_code': cus_code,
            'optimal_lag': 0,
            'correlation': 0.0,
            'num_months': len(txn),
            'txn_mean': float(txn.mean()),
            'rev_mean': float(rev.mean()),
            'all_lags': []
        }])
    
    max_lag = min(6, len(txn) - 2)  # Tối đa 6 tháng
    
    best_lag = 0
    best_corr = -2.0
    all_lags = []
    
    for lag in range(0, max_lag + 1):
        if lag == 0:
            # Cùng tháng
            if len(txn) > 1:
                corr, pval = pearsonr(txn, rev)
            else:
                corr, pval = 0.0, 1.0
        else:
            # Revenue shift về trước lag tháng
            shifted_rev = rev[lag:]
            matched_txn = txn[:-lag]
            
            if len(matched_txn) > 1:
                corr, pval = pearsonr(matched_txn, shifted_rev)
            else:
                corr, pval = 0.0, 1.0
        
        all_lags.append({
            'lag': int(lag),
            'corr': float(corr),
            'pvalue': float(pval)
        })
        
        if corr > best_corr:
            best_corr = corr
            best_lag = lag
    
    return pd.DataFrame([{
        'cus_code': cus_code,
        'optimal_lag': int(best_lag),
        'correlation': float(best_corr),
        'num_months': len(txn),
        'txn_mean': float(txn.mean()),
        'rev_mean': float(rev.mean()),
        'all_lags': all_lags
    }])

print("✅ Correlation UDF Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Run Correlation Analysis

# COMMAND ----------

def run_correlation_analysis(tbl1, tbl2, cache=True):
    """
    Chạy correlation analysis cho tất cả khách hàng
    
    Args:
        tbl1: Transaction DataFrame
        tbl2: Revenue DataFrame
        cache: Cache intermediate results
    
    Returns:
        Spark DataFrame với kết quả correlation
    """
    print("🔄 Running Correlation Analysis...")
    
    # Join txn và revenue
    merged = tbl1.alias("t1").join(
        tbl2.alias("t2"),
        (col("t1.cus_code") == col("t2.cus_code")) & 
        (col("t1.month") == col("t2.month")),
        "inner"
    ).select(
        col("t1.cus_code"),
        col("t1.month"),
        col("t1.txn_amount"),
        col("t2.revenue")
    )
    
    if cache:
        merged.cache()
    
    # Apply UDF
    result = merged.groupBy("cus_code").apply(compute_correlation_udf)
    
    if cache:
        result.cache()
        count = result.count()
        print(f"✅ Processed {count:,} customers")
    
    return result

# Run analysis
correlation_results = run_correlation_analysis(tbl1, tbl2, cache=True)

# Display results
display(correlation_results.orderBy(col("correlation").desc()).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Analyze Correlation Results

# COMMAND ----------

def analyze_correlation_results(correlation_results):
    """
    Phân tích kết quả correlation
    """
    print("="*60)
    print("CORRELATION ANALYSIS SUMMARY")
    print("="*60)
    
    # 1. Overall statistics
    stats = correlation_results.select(
        count("*").alias("total_customers"),
        avg("correlation").alias("avg_correlation"),
        stddev("correlation").alias("std_correlation"),
        _min("correlation").alias("min_correlation"),
        _max("correlation").alias("max_correlation"),
        avg("optimal_lag").alias("avg_lag"),
        avg("num_months").alias("avg_months")
    ).toPandas()
    
    print("\n1. OVERALL STATISTICS")
    print("-" * 60)
    print(stats.to_string(index=False))
    
    # 2. Lag distribution
    print("\n2. LAG DISTRIBUTION")
    print("-" * 60)
    lag_dist = correlation_results.groupBy("optimal_lag").agg(
        count("*").alias("num_customers")
    ).orderBy("optimal_lag").toPandas()
    
    print(lag_dist.to_string(index=False))
    
    # 3. Correlation segments
    print("\n3. CORRELATION QUALITY SEGMENTS")
    print("-" * 60)
    segments = correlation_results.select(
        when(col("correlation") >= 0.8, "High (≥0.8)")
        .when(col("correlation") >= 0.5, "Medium (0.5-0.8)")
        .when(col("correlation") >= 0.2, "Low (0.2-0.5)")
        .otherwise("Very Low (<0.2)")
        .alias("segment")
    ).groupBy("segment").agg(
        count("*").alias("num_customers")
    ).orderBy("segment").toPandas()
    
    print(segments.to_string(index=False))
    
    print("\n" + "="*60)

analyze_correlation_results(correlation_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Visualization

# COMMAND ----------

def visualize_correlation_results(correlation_results):
    """
    Visualize correlation analysis results
    """
    # Convert to Pandas
    pdf = correlation_results.toPandas()
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Correlation Distribution',
            'Optimal Lag Distribution',
            'Correlation vs Lag',
            'Top 20 Customers by Correlation'
        ),
        specs=[
            [{"type": "histogram"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "bar"}]
        ]
    )
    
    # 1. Correlation distribution
    fig.add_trace(
        go.Histogram(x=pdf['correlation'], nbinsx=30, name='Correlation'),
        row=1, col=1
    )
    
    # 2. Lag distribution
    lag_counts = pdf['optimal_lag'].value_counts().sort_index()
    fig.add_trace(
        go.Bar(x=lag_counts.index, y=lag_counts.values, name='Lag Count'),
        row=1, col=2
    )
    
    # 3. Correlation vs Lag scatter
    fig.add_trace(
        go.Scatter(
            x=pdf['optimal_lag'], 
            y=pdf['correlation'],
            mode='markers',
            marker=dict(size=5, opacity=0.5),
            name='Customers'
        ),
        row=2, col=1
    )
    
    # 4. Top 20 customers
    top20 = pdf.nlargest(20, 'correlation')
    fig.add_trace(
        go.Bar(
            x=top20['cus_code'],
            y=top20['correlation'],
            name='Top 20',
            marker_color='green'
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=800,
        showlegend=False,
        title_text="Correlation Analysis Results"
    )
    
    fig.update_xaxes(title_text="Correlation", row=1, col=1)
    fig.update_xaxes(title_text="Lag (months)", row=1, col=2)
    fig.update_xaxes(title_text="Optimal Lag", row=2, col=1)
    fig.update_xaxes(title_text="Customer Code", row=2, col=2, tickangle=45)
    
    fig.update_yaxes(title_text="Frequency", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Correlation", row=2, col=1)
    fig.update_yaxes(title_text="Correlation", row=2, col=2)
    
    fig.show()

visualize_correlation_results(correlation_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Generate Mapping Table

# COMMAND ----------

def generate_correlation_mapping(tbl1, tbl2, correlation_results, min_correlation=0.3):
    """
    Tạo bảng mapping dựa trên correlation results
    
    Args:
        tbl1: Transaction DataFrame
        tbl2: Revenue DataFrame
        correlation_results: Kết quả từ correlation analysis
        min_correlation: Ngưỡng correlation tối thiểu
    
    Returns:
        Spark DataFrame với mapping txn → revenue
    """
    print(f"🔄 Generating Mapping Table (min_correlation={min_correlation})...")
    
    # Filter customers với correlation đủ cao
    valid_customers = correlation_results.filter(
        col("correlation") >= min_correlation
    ).select("cus_code", "optimal_lag")
    
    print(f"   Valid customers: {valid_customers.count():,}")
    
    # Join với transaction data
    txn_with_lag = tbl1.join(valid_customers, "cus_code", "inner")
    
    # Tạo mapping: txn_month + lag = rev_month
    from pyspark.sql.functions import expr, add_months, to_date
    
    mapping = txn_with_lag.join(
        tbl2,
        (txn_with_lag.cus_code == tbl2.cus_code) &
        (add_months(to_date(txn_with_lag.month), txn_with_lag.optimal_lag) == to_date(tbl2.month)),
        "inner"
    ).select(
        txn_with_lag.cus_code,
        txn_with_lag.month.alias("txn_month"),
        txn_with_lag.txn_amount,
        tbl2.month.alias("rev_month"),
        tbl2.revenue,
        txn_with_lag.optimal_lag.alias("lag_months")
    )
    
    print(f"✅ Generated {mapping.count():,} mappings")
    
    return mapping

# Generate mapping
correlation_mapping = generate_correlation_mapping(
    tbl1, tbl2, correlation_results, min_correlation=0.3
)

display(correlation_mapping.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Export Results

# COMMAND ----------

# Save to Delta table
output_path = "/mnt/data/correlation_results"

correlation_results.write.format("delta").mode("overwrite").save(f"{output_path}/summary")
correlation_mapping.write.format("delta").mode("overwrite").save(f"{output_path}/mapping")

print(f"✅ Results saved to {output_path}")
