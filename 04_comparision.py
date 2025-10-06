# Databricks notebook source
# MAGIC %md
# MAGIC # Comparison: Correlation vs DTW
# MAGIC So sánh 2 phương pháp và đưa ra recommendations

# COMMAND ----------

# MAGIC %run ./01_setup_and_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Results

# COMMAND ----------

# Load correlation results
correlation_results = spark.read.format("delta").load("/mnt/data/correlation_results/summary")
correlation_mapping = spark.read.format("delta").load("/mnt/data/correlation_results/mapping")

# Load DTW results
dtw_results = spark.read.format("delta").load("/mnt/data/dtw_results/summary")
dtw_mappings = spark.read.format("delta").load("/mnt/data/dtw_results/mapping")

print("✅ Results Loaded")
print(f"   Correlation: {correlation_results.count():,} customers")
print(f"   DTW: {dtw_results.count():,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Compare Methods

# COMMAND ----------

def compare_methods(correlation_results, dtw_results):
    """
    So sánh correlation và DTW
    """
    print("="*70)
    print(" "*20 + "METHOD COMPARISON")
    print("="*70)
    
    # Join results
    comparison = correlation_results.alias("corr").join(
        dtw_results.alias("dtw"),
        col("corr.cus_code") == col("dtw.cus_code"),
        "inner"
    ).select(
        col("corr.cus_code"),
        col("corr.correlation"),
        col("corr.optimal_lag"),
        col("dtw.total_cost"),
        col("dtw.num_mappings"),
        col("dtw.avg_distance")
    )
    
    comparison.cache()
    
    # Overall comparison
    stats = comparison.select(
        count("*").alias("total_customers"),
        avg("correlation").alias("avg_correlation"),
        stddev("correlation").alias("std_correlation"),
        avg("total_cost").alias("avg_dtw_cost"),
        stddev("total_cost").alias("std_dtw_cost"),
        avg("optimal_lag").alias("avg_lag"),
        avg("num_mappings").alias("avg_dtw_mappings")
    ).toPandas()
    
    print("\nOVERALL STATISTICS")
    print("-" * 70)
    print(stats.to_string(index=False))
    
    # Correlation between methods
    pdf = comparison.toPandas()
    
    from scipy.stats import pearsonr, spearmanr
    
    # Correlation between correlation score and DTW cost (should be negative)
    if len(pdf) > 1:
        pearson_corr, pearson_p = pearsonr(pdf['correlation'], pdf['total_cost'])
        spearman_corr, spearman_p = spearmanr(pdf['correlation'], pdf['total_cost'])
        
        print("\nMETHOD AGREEMENT")
        print("-" * 70)
        print(f"Pearson correlation (Corr vs DTW cost): {pearson_corr:.3f} (p={pearson_p:.4f})")
        print(f"Spearman correlation (Corr vs DTW cost): {spearman_corr:.3f} (p={spearman_p:.4f})")
        print("\nInterpretation:")
        print("  - Negative correlation expected (high correlation = low DTW cost)")
        print(f"  - {'✅ Methods agree' if pearson_corr < -0.3 else '⚠️  Methods show different patterns'}")
    
    # Segment analysis
    print("\nSEGMENT ANALYSIS")
    print("-" * 70)
    
    segments = comparison.select(
        when(col("correlation") >= 0.7, "High Correlation")
        .when(col("correlation") >= 0.4, "Medium Correlation")
        .otherwise("Low Correlation")
        .alias("corr_segment"),
        
        when(col("total_cost") <= pdf['total_cost'].quantile(0.33), "Low DTW Cost")
        .when(col("total_cost") <= pdf['total_cost'].quantile(0.67), "Medium DTW Cost")
        .otherwise("High DTW Cost")
        .alias("dtw_segment")
    ).groupBy("corr_segment", "dtw_segment").agg(
        count("*").alias("num_customers")
    ).orderBy("corr_segment", "dtw_segment").toPandas()
    
    print(segments.to_string(index=False))
    
    print("\n" + "="*70)
    
    return comparison

comparison_df = compare_methods(correlation_results, dtw_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Visualize Comparison

# COMMAND ----------

def visualize_comparison(comparison_df):
    """
    Visualize so sánh giữa 2 phương pháp
    """
    pdf = comparison_df.toPandas()
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Correlation vs DTW Cost',
            'Method Agreement Matrix',
            'Optimal Lag Distribution',
            'Performance Metrics'
        )
    )
    
    # 1. Scatter plot
    fig.add_trace(
        go.Scatter(
            x=pdf['correlation'],
            y=pdf['total_cost'],
            mode='markers',
            marker=dict(
                size=5,
                color=pdf['optimal_lag'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title="Lag")
            ),
            text=pdf['cus_code'],
            name='Customers'
        ),
        row=1, col=1
    )
    
    # 2. 2D histogram (agreement matrix)
    fig.add_trace(
        go.Histogram2d(
            x=pdf['correlation'],
            y=pdf['total_cost'],
            colorscale='Blues',
            nbinsx=20,
            nbinsy=20
        ),
        row=1, col=2
    )
    
    # 3. Lag distribution by correlation quality
    pdf['corr_quality'] = pd.cut(
        pdf['correlation'],
        bins=[0, 0.3, 0.6, 1.0],
        labels=['Low', 'Medium', 'High']
    )
    
    for quality in ['Low', 'Medium', 'High']:
        data = pdf[pdf['corr_quality'] == quality]['optimal_lag']
        fig.add_trace(
            go.Histogram(x=data, name=quality, opacity=0.7),
            row=2, col=1
        )
    
    # 4. Performance comparison table
    summary = pd.DataFrame({
        'Metric': ['Avg Correlation', 'Avg DTW Cost', 'Avg Lag', 'Avg Mappings'],
        'Value': [
            f"{pdf['correlation'].mean():.3f}",
            f"{pdf['total_cost'].mean():.2f}",
            f"{pdf['optimal_lag'].mean():.2f}",
            f"{pdf['num_mappings'].mean():.1f}"
        ],
        'Std Dev': [
            f"{pdf['correlation'].std():.3f}",
            f"{pdf['total_cost'].std():.2f}",
            f"{pdf['optimal_lag'].std():.2f}",
            f"{pdf['num_mappings'].std():.1f}"
        ]
    })
    
    fig.add_trace(
        go.Table(
            header=dict(values=list(summary.columns)),
            cells=dict(values=[summary[col] for col in summary.columns])
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(height=900, title_text="Correlation vs DTW Comparison")
    fig.update_xaxes(title_text="Correlation", row=1, col=1)
    fig.update_yaxes(title_text="DTW Cost", row=1, col=1)
    fig.update_xaxes(title_text="Optimal Lag", row=2, col=1)
    
    fig.show()

visualize_comparison(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Generate Recommendations

# COMMAND ----------

def generate_recommendations(comparison_df, tbl1):
    """
    Tạo recommendations cho mỗi khách hàng
    """
    print("🔄 Generating Recommendations...")
    
    # Join với pattern type từ original data
    with_pattern = comparison_df.join(
        tbl1.select("cus_code", "pattern_type").distinct(),
        "cus_code",
        "inner"
    )
    
    # Define recommendation logic
    recommendations = with_pattern.select(
        col("cus_code"),
        col("correlation"),
        col("optimal_lag"),
        col("total_cost"),
        col("pattern_type"),
        
        # Recommended method
        when(
            (col("correlation") >= 0.7) & (col("optimal_lag") <= 2),
            "Correlation"
        ).when(
            (col("correlation") >= 0.5) & (col("total_cost") <= 10),
            "Either (prefer Correlation for simplicity)"
        ).when(
            col("total_cost") <= 15,
            "DTW"
        ).otherwise(
            "Complex - Manual Review"
        ).alias("recommended_method"),
        
        # Confidence
        when(
            (col("correlation") >= 0.8) | (col("total_cost") <= 5),
            "High"
        ).when(
            (col("correlation") >= 0.5) | (col("total_cost") <= 15),
            "Medium"
        ).otherwise(
            "Low"
        ).alias("confidence"),
        
        # Reason
        when(
            col("correlation") >= 0.7,
            "High correlation with simple lag pattern"
        ).when(
            col("total_cost") <= 10,
            "Low DTW cost indicates good alignment"
        ).when(
            (col("correlation") < 0.3) & (col("total_cost") > 20),
            "Both methods show poor fit - investigate data quality"
        ).otherwise(
            "Moderate fit - consider business context"
        ).alias("reason")
    )
    
    recommendations.cache()
    
    # Summary
    print("\n" + "="*70)
    print(" "*20 + "RECOMMENDATIONS SUMMARY")
    print("="*70)
    
    method_dist = recommendations.groupBy("recommended_method", "confidence").agg(
        count("*").alias("num_customers")
    ).orderBy("recommended_method", "confidence").toPandas()
    
    print("\nRECOMMENDED METHOD DISTRIBUTION")
    print("-" * 70)
    print(method_dist.to_string(index=False))
    
    # Pattern-based analysis
    pattern_analysis = recommendations.groupBy("pattern_type", "recommended_method").agg(
        count("*").alias("num_customers"),
        avg("correlation").alias("avg_correlation"),
        avg("total_cost").alias("avg_cost")
    ).orderBy("pattern_type", "recommended_method").toPandas()
    
    print("\nPATTERN-BASED ANALYSIS")
    print("-" * 70)
    print(pattern_analysis.to_string(index=False))
    
    print("\n" + "="*70)
    
    return recommendations

recommendations = generate_recommendations(comparison_df, tbl1)

display(recommendations.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Final Report

# COMMAND ----------

def generate_final_report(correlation_results, dtw_results, recommendations):
    """
    Tạo báo cáo tổng kết
    """
    report = []
    
    report.append("="*80)
    report.append(" "*20 + "TRANSACTION-REVENUE MAPPING")
    report.append(" "*25 + "FINAL REPORT")
    report.append(" "*20 + f"Generated: 2025-10-06 by dangphdh")
    report.append("="*80)
    
    # Executive Summary
    report.append("\n📊 EXECUTIVE SUMMARY")
    report.append("-" * 80)
    
    total_customers = recommendations.count()
    high_conf = recommendations.filter(col("confidence") == "High").count()
    medium_conf = recommendations.filter(col("confidence") == "Medium").count()
    low_conf = recommendations.filter(col("confidence") == "Low").count()
    
    report.append(f"\nTotal Customers Analyzed: {total_customers:,}")
    report.append(f"  • High Confidence: {high_conf:,} ({high_conf/total_customers*100:.1f}%)")
    report.append(f"  • Medium Confidence: {medium_conf:,} ({medium_conf/total_customers*100:.1f}%)")
    report.append(f"  • Low Confidence: {low_conf:,} ({low_conf/total_customers*100:.1f}%)")
    
    # Method recommendations
    report.append("\n🎯 METHOD RECOMMENDATIONS")
    report.append("-" * 80)
    
    method_dist = recommendations.groupBy("recommended_method").agg(
        count("*").alias("count")
    ).toPandas()
    
    for _, row in method_dist.iterrows():
        pct = row['count'] / total_customers * 100
        report.append(f"  • {row['recommended_method']}: {row['count']:,} customers ({pct:.1f}%)")
    
    # Quality metrics
    report.append("\n📈 QUALITY METRICS")
    report.append("-" * 80)
    
    corr_stats = correlation_results.select(
        avg("correlation").alias("avg_corr"),
        stddev("correlation").alias("std_corr")
    ).collect()[0]
    
    dtw_stats = dtw_results.select(
        avg("total_cost").alias("avg_cost"),
        stddev("total_cost").alias("std_cost")
    ).collect()[0]
    
    report.append(f"\nCorrelation Method:")
    report.append(f"  • Average Correlation: {corr_stats['avg_corr']:.3f} ± {corr_stats['std_corr']:.3f}")
    report.append(f"\nDTW Method:")
    report.append(f"  • Average Cost: {dtw_stats['avg_cost']:.2f} ± {dtw_stats['std_cost']:.2f}")
    
    # Recommendations
    report.append("\n💡 KEY RECOMMENDATIONS")
    report.append("-" * 80)
    
    report.append("\n1. FOR HIGH CONFIDENCE CUSTOMERS:")
    report.append("   → Use recommended method directly")
    report.append("   → Automate mapping process")
    report.append("   → Monitor monthly for drift")
    
    report.append("\n2. FOR MEDIUM CONFIDENCE CUSTOMERS:")
    report.append("   → Use recommended method with validation")
    report.append("   → Sample check 10% of mappings monthly")
    report.append("   → Consider hybrid approach")
    
    report.append("\n3. FOR LOW CONFIDENCE CUSTOMERS:")
    report.append("   → Manual review required")
    report.append("   → Investigate data quality issues")
    report.append("   → Consider additional features (product, segment, etc.)")
    
    report.append("\n4. IMPLEMENTATION STRATEGY:")
    report.append("   → Phase 1: Deploy for High Confidence customers")
    report.append("   → Phase 2: Validate and deploy for Medium Confidence")
    report.append("   → Phase 3: Address Low Confidence cases")
    
    report.append("\n" + "="*80)
    
    # Print report
    print("\n".join(report))
    
    # Save report
    report_text = "\n".join(report)
    dbutils.fs.put("/mnt/data/mapping_final_report.txt", report_text, overwrite=True)
    print(f"\n✅ Report saved to /mnt/data/mapping_final_report.txt")

generate_final_report(correlation_results, dtw_results, recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Export Final Recommendations

# COMMAND ----------

# Save recommendations
output_path = "/mnt/data/final_recommendations"

recommendations.write.format("delta").mode("overwrite").save(output_path)

# Create view for easy querying
recommendations.createOrReplaceTempView("recommendations")

spark.sql("""
    CREATE OR REPLACE TABLE default.txn_rev_mapping_recommendations
    USING DELTA
    LOCATION '/mnt/data/final_recommendations'
""")

print(f"✅ Final recommendations saved")
print(f"   Location: {output_path}")
print(f"   Table: default.txn_rev_mapping_recommendations")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query example
# MAGIC SELECT 
# MAGIC   recommended_method,
# MAGIC   confidence,
# MAGIC   COUNT(*) as num_customers,
# MAGIC   AVG(correlation) as avg_correlation,
# MAGIC   AVG(total_cost) as avg_dtw_cost
# MAGIC FROM recommendations
# MAGIC GROUP BY recommended_method, confidence
# MAGIC ORDER BY recommended_method, confidence
