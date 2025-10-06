# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Time Warping (DTW) Implementation
# MAGIC Thuật toán DTW để xử lý pattern phức tạp

# COMMAND ----------

# MAGIC %run ./01_setup_and_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. DTW Core Functions

# COMMAND ----------

def dtw_distance(txn: np.ndarray, rev: np.ndarray, 
                 window: int = None, normalize: bool = True) -> Tuple[float, np.ndarray, List]:
    """
    Tính DTW distance và tìm optimal path
    
    Args:
        txn: Transaction time series
        rev: Revenue time series
        window: Window constraint (None = no constraint)
        normalize: Normalize series trước khi tính
    
    Returns:
        (total_cost, acc_cost_matrix, path)
    """
    # Normalize
    if normalize:
        txn = normalize_series(txn)
        rev = normalize_series(rev)
    
    n, m = len(txn), len(rev)
    
    # Distance matrix
    distance_matrix = np.zeros((n, m))
    for i in range(n):
        for j in range(m):
            distance_matrix[i, j] = abs(txn[i] - rev[j])
    
    # Accumulated cost matrix
    acc_cost = np.full((n + 1, m + 1), np.inf)
    acc_cost[0, 0] = 0
    
    # Dynamic programming với window constraint
    for i in range(1, n + 1):
        if window is None:
            j_start, j_end = 1, m + 1
        else:
            j_start = max(1, i - window)
            j_end = min(m + 1, i + window + 1)
        
        for j in range(j_start, j_end):
            cost = distance_matrix[i-1, j-1]
            acc_cost[i, j] = cost + min(
                acc_cost[i-1, j],      # vertical
                acc_cost[i, j-1],      # horizontal
                acc_cost[i-1, j-1]     # diagonal
            )
    
    # Backtracking
    path = []
    i, j = n, m
    
    while i > 0 and j > 0:
        path.append((i-1, j-1))
        
        candidates = [
            (i-1, j, acc_cost[i-1, j]),
            (i, j-1, acc_cost[i, j-1]),
            (i-1, j-1, acc_cost[i-1, j-1])
        ]
        
        i, j, _ = min(candidates, key=lambda x: x[2])
    
    path.reverse()
    
    total_cost = acc_cost[n, m]
    
    return total_cost, acc_cost, path

print("✅ DTW Core Functions Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. DTW Pandas UDF

# COMMAND ----------

# Schema cho kết quả DTW
dtw_result_schema = StructType([
    StructField("cus_code", StringType(), False),
    StructField("total_cost", DoubleType(), False),
    StructField("num_mappings", IntegerType(), False),
    StructField("num_months_txn", IntegerType(), False),
    StructField("num_months_rev", IntegerType(), False),
    StructField("avg_distance", DoubleType(), False),
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
    Pandas UDF để tính DTW
    
    Input: DataFrame với columns [cus_code, month, txn_amount, revenue]
    Output: DataFrame với DTW results
    """
    cus_code = pdf['cus_code'].iloc[0]
    
    # Sort by month
    pdf = pdf.sort_values('month')
    
    txn = pdf['txn_amount'].values
    rev = pdf['revenue'].values
    months = pdf['month'].values
    
    if len(txn) < 2 or len(rev) < 2:
        # Không đủ data
        return pd.DataFrame([{
            'cus_code': cus_code,
            'total_cost': 0.0,
            'num_mappings': 0,
            'num_months_txn': len(txn),
            'num_months_rev': len(rev),
            'avg_distance': 0.0,
            'path': []
        }])
    
    # Compute DTW
    window = min(3, len(txn) - 1)  # Window = 3 tháng
    total_cost, _, path = dtw_distance(txn, rev, window=window, normalize=True)
    
    # Build path details
    path_details = []
    total_distance = 0.0
    
    for txn_idx, rev_idx in path:
        distance = abs(txn[txn_idx] - rev[rev_idx])
        total_distance += distance
        
        path_details.append({
            'txn_idx': int(txn_idx),
            'rev_idx': int(rev_idx),
            'txn_month': str(months[txn_idx]),
            'rev_month': str(months[rev_idx]),
            'txn_amount': float(txn[txn_idx]),
            'revenue': float(rev[rev_idx])
        })
    
    avg_distance = total_distance / len(path) if len(path) > 0 else 0.0
    
    return pd.DataFrame([{
        'cus_code': cus_code,
        'total_cost': float(total_cost),
        'num_mappings': len(path),
        'num_months_txn': len(txn),
        'num_months_rev': len(rev),
        'avg_distance': float(avg_distance),
        'path': path_details
    }])

print("✅ DTW UDF Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Run DTW Analysis

# COMMAND ----------

def run_dtw_analysis(tbl1, tbl2, cache=True):
    """
    Chạy DTW analysis cho tất cả khách hàng
    
    Args:
        tbl1: Transaction DataFrame
        tbl2: Revenue DataFrame
        cache: Cache intermediate results
    
    Returns:
        Spark DataFrame với kết quả DTW
    """
    print("🔄 Running DTW Analysis...")
    print("   ⚠️  DTW is computationally intensive, this may take a while...")
    
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
    result = merged.groupBy("cus_code").apply(compute_dtw_udf)
    
    if cache:
        result.cache()
        count = result.count()
        print(f"✅ Processed {count:,} customers")
    
    return result

# Run analysis
dtw_results = run_dtw_analysis(tbl1, tbl2, cache=True)

# Display results
display(dtw_results.orderBy("total_cost").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Analyze DTW Results

# COMMAND ----------

def analyze_dtw_results(dtw_results):
    """
    Phân tích kết quả DTW
    """
    print("="*60)
    print("DTW ANALYSIS SUMMARY")
    print("="*60)
    
    # 1. Overall statistics
    stats = dtw_results.select(
        count("*").alias("total_customers"),
        avg("total_cost").alias("avg_total_cost"),
        stddev("total_cost").alias("std_total_cost"),
        avg("num_mappings").alias("avg_mappings"),
        avg("avg_distance").alias("avg_distance"),
        avg("num_months_txn").alias("avg_months_txn"),
        avg("num_months_rev").alias("avg_months_rev")
    ).toPandas()
    
    print("\n1. OVERALL STATISTICS")
    print("-" * 60)
    print(stats.to_string(index=False))
    
    # 2. Mapping ratio distribution
    print("\n2. MAPPING CHARACTERISTICS")
    print("-" * 60)
    
    mapping_stats = dtw_results.select(
        (col("num_mappings") / col("num_months_txn")).alias("mapping_ratio")
    ).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").toPandas()
    
    print("Mapping Ratio (num_mappings / num_months):")
    print(mapping_stats.to_string(index=False))
    
    # 3. Cost segments
    print("\n3. COST QUALITY SEGMENTS")
    print("-" * 60)
    
    # Determine quartiles
    quartiles = dtw_results.approxQuantile("total_cost", [0.25, 0.5, 0.75], 0.01)
    
    segments = dtw_results.select(
        when(col("total_cost") <= quartiles[0], "Excellent (Q1)")
        .when(col("total_cost") <= quartiles[1], "Good (Q2)")
        .when(col("total_cost") <= quartiles[2], "Fair (Q3)")
        .otherwise("Poor (Q4)")
        .alias("segment")
    ).groupBy("segment").agg(
        count("*").alias("num_customers")
    ).orderBy("segment").toPandas()
    
    print(segments.to_string(index=False))
    
    print("\n" + "="*60)

analyze_dtw_results(dtw_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Extract DTW Mappings

# COMMAND ----------

def extract_dtw_mappings(dtw_results, max_cost_percentile=75):
    """
    Extract chi tiết mappings từ DTW results
    
    Args:
        dtw_results: DTW results DataFrame
        max_cost_percentile: Lọc customers có cost <= percentile này
    
    Returns:
        Spark DataFrame với chi tiết mappings
    """
    print(f"🔄 Extracting DTW Mappings (max_cost_percentile={max_cost_percentile})...")
    
    # Determine cost threshold
    cost_threshold = dtw_results.approxQuantile("total_cost", [max_cost_percentile/100], 0.01)[0]
    print(f"   Cost threshold: {cost_threshold:.2f}")
    
    # Filter valid customers
    valid_results = dtw_results.filter(col("total_cost") <= cost_threshold)
    
    print(f"   Valid customers: {valid_results.count():,}")
    
    # Explode path array
    mappings = valid_results.select(
        col("cus_code"),
        col("total_cost"),
        explode(col("path")).alias("mapping")
    ).select(
        col("cus_code"),
        col("total_cost"),
        col("mapping.txn_idx"),
        col("mapping.rev_idx"),
        col("mapping.txn_month"),
        col("mapping.rev_month"),
        col("mapping.txn_amount"),
        col("mapping.revenue")
    )
    
    print(f"✅ Extracted {mappings.count():,} mappings")
    
    return mappings

# Extract mappings
dtw_mappings = extract_dtw_mappings(dtw_results, max_cost_percentile=75)

display(dtw_mappings.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Visualization

# COMMAND ----------

def visualize_dtw_example(tbl1, tbl2, dtw_results, cus_code):
    """
    Visualize DTW result cho 1 khách hàng cụ thể
    """
    # Get customer data
    txn_pdf = tbl1.filter(col("cus_code") == cus_code).orderBy("month").toPandas()
    rev_pdf = tbl2.filter(col("cus_code") == cus_code).orderBy("month").toPandas()
    dtw_pdf = dtw_results.filter(col("cus_code") == cus_code).toPandas()
    
    if len(dtw_pdf) == 0:
        print(f"No DTW result for customer {cus_code}")
        return
    
    # Extract path
    path = dtw_pdf.iloc[0]['path']
    
    # Create visualization
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Time Series Comparison',
            'DTW Path',
            'Distance Matrix',
            'Mapping Details'
        ),
        specs=[
            [{"secondary_y": True}, {"type": "scatter"}],
            [{"type": "heatmap"}, {"type": "table"}]
        ]
    )
    
    # 1. Time series
    months = list(range(len(txn_pdf)))
    
    fig.add_trace(
        go.Scatter(x=months, y=txn_pdf['txn_amount'], 
                  mode='lines+markers', name='Transaction'),
        row=1, col=1, secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=months, y=rev_pdf['revenue'], 
                  mode='lines+markers', name='Revenue', 
                  marker=dict(symbol='square')),
        row=1, col=1, secondary_y=True
    )
    
    # 2. DTW Path
    txn_indices = [p['txn_idx'] for p in path]
    rev_indices = [p['rev_idx'] for p in path]
    
    fig.add_trace(
        go.Scatter(x=rev_indices, y=txn_indices,
                  mode='lines+markers',
                  name='DTW Path',
                  marker=dict(size=8)),
        row=1, col=2
    )
    
    # 3. Distance Matrix
    txn = txn_pdf['txn_amount'].values
    rev = rev_pdf['revenue'].values
    dist_matrix = compute_distance_matrix(txn, rev, normalize=True)
    
    fig.add_trace(
        go.Heatmap(z=dist_matrix, colorscale='RdYlGn_r'),
        row=2, col=1
    )
    
    # 4. Mapping table
    mapping_df = pd.DataFrame(path[:10])  # Top 10
    
    fig.add_trace(
        go.Table(
            header=dict(values=['Txn Month', 'Rev Month', 'Txn Amount', 'Revenue']),
            cells=dict(values=[
                mapping_df['txn_month'],
                mapping_df['rev_month'],
                mapping_df['txn_amount'].round(2),
                mapping_df['revenue'].round(2)
            ])
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(height=900, title_text=f"DTW Analysis - Customer {cus_code}")
    fig.update_xaxes(title_text="Month", row=1, col=1)
    fig.update_yaxes(title_text="Transaction", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Revenue", row=1, col=1, secondary_y=True)
    fig.update_xaxes(title_text="Revenue Index", row=1, col=2)
    fig.update_yaxes(title_text="Transaction Index", row=1, col=2)
    
    fig.show()

# Visualize ví dụ
sample_customer = dtw_results.select("cus_code").limit(1).collect()[0][0]
visualize_dtw_example(tbl1, tbl2, dtw_results, sample_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Export Results

# COMMAND ----------

# Save to Delta table
output_path = "/mnt/data/dtw_results"

dtw_results.write.format("delta").mode("overwrite").save(f"{output_path}/summary")
dtw_mappings.write.format("delta").mode("overwrite").save(f"{output_path}/mapping")

print(f"✅ DTW Results saved to {output_path}")
