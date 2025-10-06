# Databricks notebook source
# MAGIC %md
# MAGIC # Production Deployment - Transaction-Revenue Mapping
# MAGIC 
# MAGIC **Author:** dangphdh  
# MAGIC **Date:** 2025-10-06  
# MAGIC **Purpose:** Production pipeline để map transaction → revenue cho dữ liệu mới

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

# MAGIC %run ./01_setup_and_utils

# COMMAND ----------

from pyspark.sql import DataFrame
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta
import json

# Production Config
class Config:
    """Production configuration"""
    
    # Data paths
    INPUT_TXN_PATH = "/mnt/prod/transactions"
    INPUT_REV_PATH = "/mnt/prod/revenue"
    OUTPUT_PATH = "/mnt/prod/mapping_results"
    CHECKPOINT_PATH = "/mnt/prod/checkpoints"
    MODEL_PATH = "/mnt/data/final_recommendations"
    
    # Processing parameters
    CORRELATION_MIN_THRESHOLD = 0.3
    DTW_MAX_COST_PERCENTILE = 75
    WINDOW_SIZE = 3
    MAX_LAG = 6
    
    # Quality thresholds
    HIGH_CONFIDENCE_CORR = 0.7
    MEDIUM_CONFIDENCE_CORR = 0.5
    LOW_DTW_COST = 10
    MEDIUM_DTW_COST = 15
    
    # Monitoring
    ENABLE_MONITORING = True
    ALERT_EMAIL = "dangphdh@company.com"
    
    # Performance
    REPARTITION_SIZE = 200
    CACHE_INTERMEDIATE = True

print("✅ Configuration Loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Validation Layer

# COMMAND ----------

class DataValidator:
    """Validate input data quality"""
    
    def __init__(self, spark):
        self.spark = spark
        self.validation_results = {}
        
    def validate_schema(self, df: DataFrame, expected_columns: list, 
                       df_name: str) -> bool:
        """Validate DataFrame schema"""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        
        if not expected_set.issubset(actual_columns):
            missing = expected_set - actual_columns
            print(f"❌ {df_name}: Missing columns {missing}")
            return False
        
        print(f"✅ {df_name}: Schema valid")
        return True
    
    def validate_data_quality(self, df: DataFrame, df_name: str) -> Dict:
        """Validate data quality"""
        print(f"\n📊 Validating {df_name}...")
        
        results = {
            'total_rows': df.count(),
            'null_checks': {},
            'duplicates': 0,
            'date_range': {},
            'status': 'PASSED'
        }
        
        # 1. Null checks
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / results['total_rows'] * 100) if results['total_rows'] > 0 else 0
            results['null_checks'][col_name] = {
                'count': null_count,
                'percentage': null_pct
            }
            
            if null_pct > 10:
                print(f"⚠️  {col_name}: {null_pct:.2f}% nulls (high)")
                results['status'] = 'WARNING'
        
        # 2. Duplicates
        if 'cus_code' in df.columns and 'month' in df.columns:
            total = df.count()
            distinct = df.select('cus_code', 'month').distinct().count()
            results['duplicates'] = total - distinct
            
            if results['duplicates'] > 0:
                print(f"⚠️  Found {results['duplicates']} duplicate records")
                results['status'] = 'WARNING'
        
        # 3. Date range
        if 'month' in df.columns:
            date_stats = df.select(
                _min('month').alias('min_date'),
                _max('month').alias('max_date'),
                countDistinct('month').alias('distinct_months')
            ).collect()[0]
            
            results['date_range'] = {
                'min': str(date_stats['min_date']),
                'max': str(date_stats['max_date']),
                'distinct_months': date_stats['distinct_months']
            }
        
        # 4. Value checks
        if 'txn_amount' in df.columns:
            amount_stats = df.select(
                _min('txn_amount').alias('min_amt'),
                _max('txn_amount').alias('max_amt'),
                avg('txn_amount').alias('avg_amt')
            ).collect()[0]
            
            if amount_stats['min_amt'] < 0:
                print(f"❌ Negative transaction amounts found")
                results['status'] = 'FAILED'
        
        if 'revenue' in df.columns:
            rev_stats = df.select(
                _min('revenue').alias('min_rev'),
                _max('revenue').alias('max_rev')
            ).collect()[0]
            
            if rev_stats['min_rev'] < 0:
                print(f"❌ Negative revenue found")
                results['status'] = 'FAILED'
        
        print(f"Status: {results['status']}")
        
        self.validation_results[df_name] = results
        return results
    
    def get_validation_report(self) -> str:
        """Generate validation report"""
        report = []
        report.append("="*80)
        report.append("DATA VALIDATION REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("="*80)
        
        for df_name, results in self.validation_results.items():
            report.append(f"\n{df_name.upper()}")
            report.append("-"*80)
            report.append(f"Total Rows: {results['total_rows']:,}")
            report.append(f"Status: {results['status']}")
            
            if results['duplicates'] > 0:
                report.append(f"Duplicates: {results['duplicates']:,}")
            
            if results['date_range']:
                report.append(f"Date Range: {results['date_range']['min']} to {results['date_range']['max']}")
            
            # Null summary
            high_nulls = [k for k, v in results['null_checks'].items() if v['percentage'] > 5]
            if high_nulls:
                report.append(f"Columns with >5% nulls: {', '.join(high_nulls)}")
        
        report.append("\n" + "="*80)
        
        return "\n".join(report)

print("✅ DataValidator Class Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mapping Engine

# COMMAND ----------

class MappingEngine:
    """Core mapping engine"""
    
    def __init__(self, spark, config: Config):
        self.spark = spark
        self.config = config
        self.recommendations = None
        self.stats = {}
        
    def load_recommendations(self):
        """Load pre-trained recommendations"""
        print(f"🔄 Loading recommendations from {self.config.MODEL_PATH}...")
        
        try:
            self.recommendations = self.spark.read.format("delta").load(self.config.MODEL_PATH)
            count = self.recommendations.count()
            print(f"✅ Loaded {count:,} customer recommendations")
            
            # Cache for reuse
            self.recommendations.cache()
            
        except Exception as e:
            print(f"❌ Failed to load recommendations: {e}")
            raise
    
    def apply_correlation_mapping(self, tbl1: DataFrame, tbl2: DataFrame, 
                                  customers: DataFrame) -> DataFrame:
        """
        Apply correlation-based mapping
        
        Args:
            tbl1: Transaction data
            tbl2: Revenue data
            customers: Customers to use correlation method
        
        Returns:
            Mapped DataFrame
        """
        print("🔄 Applying Correlation Mapping...")
        
        # Filter customers
        cus_list = customers.select('cus_code', 'optimal_lag').cache()
        
        # Join transactions with lag info
        txn_with_lag = tbl1.join(cus_list, 'cus_code', 'inner')
        
        # Apply lag and join with revenue
        from pyspark.sql.functions import add_months, to_date, date_format
        
        mapped = txn_with_lag.alias('t').join(
            tbl2.alias('r'),
            (col('t.cus_code') == col('r.cus_code')) &
            (date_format(add_months(to_date(col('t.month')), col('t.optimal_lag')), 'yyyy-MM') 
             == col('r.month')),
            'inner'
        ).select(
            col('t.cus_code'),
            col('t.month').alias('txn_month'),
            col('t.txn_amount'),
            col('r.month').alias('rev_month'),
            col('r.revenue'),
            col('t.optimal_lag').alias('lag_months'),
            lit('correlation').alias('mapping_method'),
            lit(datetime.now()).alias('processed_at')
        )
        
        count = mapped.count()
        print(f"✅ Correlation mapping: {count:,} records")
        
        self.stats['correlation_mappings'] = count
        
        return mapped
    
    def apply_dtw_mapping(self, tbl1: DataFrame, tbl2: DataFrame,
                         customers: DataFrame) -> DataFrame:
        """
        Apply DTW-based mapping
        
        Args:
            tbl1: Transaction data
            tbl2: Revenue data
            customers: Customers to use DTW method
        
        Returns:
            Mapped DataFrame
        """
        print("🔄 Applying DTW Mapping...")
        
        # Filter customers
        cus_list = customers.select('cus_code').cache()
        
        # Merge txn and revenue
        merged = tbl1.join(cus_list, 'cus_code', 'inner').alias('t1').join(
            tbl2.alias('t2'),
            (col('t1.cus_code') == col('t2.cus_code')) &
            (col('t1.month') == col('t2.month')),
            'inner'
        ).select(
            col('t1.cus_code'),
            col('t1.month'),
            col('t1.txn_amount'),
            col('t2.revenue')
        )
        
        # Apply DTW UDF
        dtw_results = merged.groupBy('cus_code').apply(compute_dtw_udf)
        
        # Extract mappings
        mapped = dtw_results.select(
            col('cus_code'),
            explode(col('path')).alias('mapping')
        ).select(
            col('cus_code'),
            col('mapping.txn_month'),
            col('mapping.txn_amount'),
            col('mapping.rev_month'),
            col('mapping.revenue'),
            lit(None).cast('int').alias('lag_months'),
            lit('dtw').alias('mapping_method'),
            lit(datetime.now()).alias('processed_at')
        )
        
        count = mapped.count()
        print(f"✅ DTW mapping: {count:,} records")
        
        self.stats['dtw_mappings'] = count
        
        return mapped
    
    def process(self, tbl1: DataFrame, tbl2: DataFrame) -> DataFrame:
        """
        Main processing pipeline
        
        Args:
            tbl1: Transaction data
            tbl2: Revenue data
        
        Returns:
            Final mapped results
        """
        print("\n" + "="*80)
        print("STARTING MAPPING PIPELINE")
        print("="*80)
        
        start_time = datetime.now()
        
        # Load recommendations if not loaded
        if self.recommendations is None:
            self.load_recommendations()
        
        # Repartition for performance
        tbl1 = tbl1.repartition(self.config.REPARTITION_SIZE, 'cus_code')
        tbl2 = tbl2.repartition(self.config.REPARTITION_SIZE, 'cus_code')
        
        if self.config.CACHE_INTERMEDIATE:
            tbl1.cache()
            tbl2.cache()
        
        # Get unique customers in input data
        input_customers = tbl1.select('cus_code').distinct()
        
        # Join with recommendations
        customers_with_rec = input_customers.join(
            self.recommendations,
            'cus_code',
            'left'
        )
        
        # Segment customers by recommended method
        correlation_customers = customers_with_rec.filter(
            col('recommended_method').isin(['Correlation', 'Either (prefer Correlation for simplicity)'])
        )
        
        dtw_customers = customers_with_rec.filter(
            col('recommended_method') == 'DTW'
        )
        
        no_rec_customers = customers_with_rec.filter(
            col('recommended_method').isNull()
        )
        
        # Stats
        total_customers = input_customers.count()
        corr_count = correlation_customers.count()
        dtw_count = dtw_customers.count()
        no_rec_count = no_rec_customers.count()
        
        print(f"\n📊 Customer Segmentation:")
        print(f"   Total: {total_customers:,}")
        print(f"   Correlation: {corr_count:,} ({corr_count/total_customers*100:.1f}%)")
        print(f"   DTW: {dtw_count:,} ({dtw_count/total_customers*100:.1f}%)")
        print(f"   No Recommendation: {no_rec_count:,} ({no_rec_count/total_customers*100:.1f}%)")
        
        self.stats['total_customers'] = total_customers
        self.stats['correlation_customers'] = corr_count
        self.stats['dtw_customers'] = dtw_count
        self.stats['no_rec_customers'] = no_rec_count
        
        # Process each segment
        results = []
        
        if corr_count > 0:
            corr_mapped = self.apply_correlation_mapping(tbl1, tbl2, correlation_customers)
            results.append(corr_mapped)
        
        if dtw_count > 0:
            dtw_mapped = self.apply_dtw_mapping(tbl1, tbl2, dtw_customers)
            results.append(dtw_mapped)
        
        # Handle customers without recommendations (use default correlation)
        if no_rec_count > 0:
            print("⚠️  Processing customers without recommendations (using default correlation)...")
            default_customers = no_rec_customers.select('cus_code').withColumn('optimal_lag', lit(1))
            default_mapped = self.apply_correlation_mapping(tbl1, tbl2, default_customers)
            results.append(default_mapped)
            self.stats['default_mappings'] = default_mapped.count()
        
        # Combine results
        if len(results) > 0:
            final_result = results[0]
            for df in results[1:]:
                final_result = final_result.union(df)
        else:
            # Empty result
            final_result = self.spark.createDataFrame([], schema=results[0].schema if results else None)
        
        # Add metadata
        final_result = final_result.withColumn('pipeline_run_id', lit(start_time.strftime('%Y%m%d_%H%M%S')))
        
        # Stats
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        total_mappings = final_result.count()
        self.stats['total_mappings'] = total_mappings
        self.stats['duration_seconds'] = duration
        self.stats['start_time'] = str(start_time)
        self.stats['end_time'] = str(end_time)
        
        print(f"\n✅ Pipeline completed in {duration:.2f} seconds")
        print(f"   Total mappings: {total_mappings:,}")
        
        if total_customers > 0:
            coverage = (total_mappings / total_customers) * 100
            print(f"   Coverage: {coverage:.1f}%")
            self.stats['coverage_pct'] = coverage
        
        return final_result
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return self.stats

print("✅ MappingEngine Class Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality Assurance Layer

# COMMAND ----------

class QualityChecker:
    """Quality checks for mapping results"""
    
    def __init__(self, spark):
        self.spark = spark
        self.issues = []
        
    def check_mapping_quality(self, mapped_df: DataFrame, 
                             tbl1: DataFrame, tbl2: DataFrame) -> Dict:
        """
        Comprehensive quality checks
        
        Returns:
            Dictionary with check results
        """
        print("\n" + "="*80)
        print("QUALITY ASSURANCE CHECKS")
        print("="*80)
        
        results = {
            'passed': True,
            'checks': {}
        }
        
        # 1. Coverage check
        print("\n1. COVERAGE CHECK")
        print("-"*80)
        
        total_txn = tbl1.select('cus_code').distinct().count()
        mapped_cus = mapped_df.select('cus_code').distinct().count()
        coverage = (mapped_cus / total_txn * 100) if total_txn > 0 else 0
        
        results['checks']['coverage'] = {
            'total_customers': total_txn,
            'mapped_customers': mapped_cus,
            'coverage_pct': coverage,
            'status': 'PASS' if coverage >= 70 else 'FAIL'
        }
        
        print(f"Total customers: {total_txn:,}")
        print(f"Mapped customers: {mapped_cus:,}")
        print(f"Coverage: {coverage:.1f}%")
        print(f"Status: {results['checks']['coverage']['status']}")
        
        if coverage < 70:
            self.issues.append(f"Low coverage: {coverage:.1f}%")
            results['passed'] = False
        
        # 2. Duplicate check
        print("\n2. DUPLICATE CHECK")
        print("-"*80)
        
        total_mappings = mapped_df.count()
        distinct_mappings = mapped_df.select('cus_code', 'txn_month', 'rev_month').distinct().count()
        duplicates = total_mappings - distinct_mappings
        
        results['checks']['duplicates'] = {
            'total_mappings': total_mappings,
            'distinct_mappings': distinct_mappings,
            'duplicates': duplicates,
            'status': 'PASS' if duplicates == 0 else 'WARNING'
        }
        
        print(f"Total mappings: {total_mappings:,}")
        print(f"Duplicates: {duplicates:,}")
        print(f"Status: {results['checks']['duplicates']['status']}")
        
        if duplicates > 0:
            self.issues.append(f"Found {duplicates:,} duplicate mappings")
        
        # 3. Lag reasonableness check
        print("\n3. LAG REASONABLENESS CHECK")
        print("-"*80)
        
        lag_stats = mapped_df.filter(col('lag_months').isNotNull()).select(
            _min('lag_months').alias('min_lag'),
            _max('lag_months').alias('max_lag'),
            avg('lag_months').alias('avg_lag')
        ).collect()
        
        if len(lag_stats) > 0:
            lag_stat = lag_stats[0]
            
            results['checks']['lag'] = {
                'min_lag': int(lag_stat['min_lag']) if lag_stat['min_lag'] else None,
                'max_lag': int(lag_stat['max_lag']) if lag_stat['max_lag'] else None,
                'avg_lag': float(lag_stat['avg_lag']) if lag_stat['avg_lag'] else None,
                'status': 'PASS' if (lag_stat['max_lag'] or 0) <= 12 else 'WARNING'
            }
            
            print(f"Min lag: {results['checks']['lag']['min_lag']} months")
            print(f"Max lag: {results['checks']['lag']['max_lag']} months")
            print(f"Avg lag: {results['checks']['lag']['avg_lag']:.2f} months")
            print(f"Status: {results['checks']['lag']['status']}")
            
            if (lag_stat['max_lag'] or 0) > 12:
                self.issues.append(f"Unusual lag detected: {lag_stat['max_lag']} months")
        
        # 4. Revenue/Transaction ratio check
        print("\n4. REVENUE/TRANSACTION RATIO CHECK")
        print("-"*80)
        
        ratio_stats = mapped_df.select(
            (col('revenue') / col('txn_amount')).alias('ratio')
        ).filter(col('txn_amount') > 0).select(
            _min('ratio').alias('min_ratio'),
            _max('ratio').alias('max_ratio'),
            avg('ratio').alias('avg_ratio'),
            stddev('ratio').alias('std_ratio')
        ).collect()[0]
        
        results['checks']['ratio'] = {
            'min_ratio': float(ratio_stats['min_ratio']),
            'max_ratio': float(ratio_stats['max_ratio']),
            'avg_ratio': float(ratio_stats['avg_ratio']),
            'std_ratio': float(ratio_stats['std_ratio']) if ratio_stats['std_ratio'] else 0,
            'status': 'PASS' if ratio_stats['max_ratio'] <= 1 else 'WARNING'
        }
        
        print(f"Min ratio: {results['checks']['ratio']['min_ratio']:.4f}")
        print(f"Max ratio: {results['checks']['ratio']['max_ratio']:.4f}")
        print(f"Avg ratio: {results['checks']['ratio']['avg_ratio']:.4f}")
        print(f"Status: {results['checks']['ratio']['status']}")
        
        if ratio_stats['max_ratio'] > 1:
            self.issues.append(f"Revenue > Transaction detected (max ratio: {ratio_stats['max_ratio']:.2f})")
        
        # 5. Temporal consistency check
        print("\n5. TEMPORAL CONSISTENCY CHECK")
        print("-"*80)
        
        # Check if rev_month >= txn_month
        temporal_violations = mapped_df.filter(
            to_date(col('rev_month')) < to_date(col('txn_month'))
        ).count()
        
        results['checks']['temporal'] = {
            'violations': temporal_violations,
            'status': 'PASS' if temporal_violations == 0 else 'FAIL'
        }
        
        print(f"Temporal violations: {temporal_violations:,}")
        print(f"Status: {results['checks']['temporal']['status']}")
        
        if temporal_violations > 0:
            self.issues.append(f"Found {temporal_violations:,} temporal violations (revenue before transaction)")
            results['passed'] = False
        
        # Summary
        print("\n" + "="*80)
        print(f"OVERALL STATUS: {'✅ PASSED' if results['passed'] else '❌ FAILED'}")
        
        if len(self.issues) > 0:
            print(f"\nIssues found ({len(self.issues)}):")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")
        
        print("="*80)
        
        return results
    
    def generate_quality_report(self, results: Dict) -> str:
        """Generate quality report"""
        report = []
        report.append("="*80)
        report.append("QUALITY ASSURANCE REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("="*80)
        
        report.append(f"\nOVERALL STATUS: {results['passed']}")
        
        for check_name, check_results in results['checks'].items():
            report.append(f"\n{check_name.upper()}")
            report.append("-"*80)
            for k, v in check_results.items():
                report.append(f"  {k}: {v}")
        
        if len(self.issues) > 0:
            report.append("\nISSUES")
            report.append("-"*80)
            for i, issue in enumerate(self.issues, 1):
                report.append(f"  {i}. {issue}")
        
        report.append("\n" + "="*80)
        
        return "\n".join(report)

print("✅ QualityChecker Class Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitoring & Alerting

# COMMAND ----------

class Monitor:
    """Monitoring and alerting"""
    
    def __init__(self, config: Config):
        self.config = config
        self.metrics = {}
        
    def log_metrics(self, metrics: Dict):
        """Log metrics to monitoring system"""
        self.metrics.update(metrics)
        
        # Log to Databricks
        print("\n📊 METRICS LOGGED:")
        for key, value in metrics.items():
            print(f"   {key}: {value}")
        
        # In production, send to monitoring system (e.g., CloudWatch, Datadog)
        # Example:
        # cloudwatch.put_metric_data(
        #     Namespace='MappingPipeline',
        #     MetricData=[{'MetricName': k, 'Value': v} for k, v in metrics.items()]
        # )
    
    def send_alert(self, subject: str, message: str, severity: str = 'INFO'):
        """Send alert notification"""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'severity': severity,
            'subject': subject,
            'message': message
        }
        
        print(f"\n🚨 ALERT [{severity}]: {subject}")
        print(f"   {message}")
        
        if self.config.ENABLE_MONITORING:
            # In production, send email/slack notification
            # Example:
            # sns.publish(
            #     TopicArn='arn:aws:sns:...',
            #     Subject=subject,
            #     Message=message
            # )
            pass
    
    def check_sla(self, stats: Dict):
        """Check if SLA is met"""
        print("\n📈 SLA CHECKS:")
        
        sla_met = True
        
        # 1. Coverage SLA: >= 80%
        coverage = stats.get('coverage_pct', 0)
        if coverage < 80:
            self.send_alert(
                subject="SLA VIOLATION: Low Coverage",
                message=f"Coverage is {coverage:.1f}%, below 80% threshold",
                severity="ERROR"
            )
            sla_met = False
        else:
            print(f"   ✅ Coverage: {coverage:.1f}% (>= 80%)")
        
        # 2. Processing time SLA: <= 30 minutes
        duration = stats.get('duration_seconds', 0)
        if duration > 1800:  # 30 minutes
            self.send_alert(
                subject="SLA VIOLATION: Long Processing Time",
                message=f"Processing took {duration/60:.1f} minutes, exceeding 30 min threshold",
                severity="WARNING"
            )
            sla_met = False
        else:
            print(f"   ✅ Duration: {duration/60:.1f} min (<= 30 min)")
        
        # 3. Data quality SLA
        total_mappings = stats.get('total_mappings', 0)
        if total_mappings == 0:
            self.send_alert(
                subject="SLA VIOLATION: No Mappings Generated",
                message="Pipeline produced zero mappings",
                severity="CRITICAL"
            )
            sla_met = False
        else:
            print(f"   ✅ Mappings: {total_mappings:,} (> 0)")
        
        return sla_met

print("✅ Monitor Class Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Main Production Pipeline

# COMMAND ----------

class ProductionPipeline:
    """
    Main production pipeline orchestrator
    """
    
    def __init__(self, spark, config: Config):
        self.spark = spark
        self.config = config
        self.validator = DataValidator(spark)
        self.engine = MappingEngine(spark, config)
        self.qa = QualityChecker(spark)
        self.monitor = Monitor(config)
        
    def run(self, txn_path: str = None, rev_path: str = None, 
            output_path: str = None, mode: str = 'batch') -> bool:
        """
        Run production pipeline
        
        Args:
            txn_path: Transaction data path (default: Config.INPUT_TXN_PATH)
            rev_path: Revenue data path (default: Config.INPUT_REV_PATH)
            output_path: Output path (default: Config.OUTPUT_PATH)
            mode: 'batch' or 'incremental'
        
        Returns:
            Success status
        """
        print("\n" + "="*80)
        print(" "*25 + "PRODUCTION PIPELINE")
        print(" "*20 + f"Mode: {mode.upper()}")
        print(" "*15 + f"Run ID: {datetime.now().strftime('%Y%m%d_%H%M%S')}")
        print(" "*15 + f"User: {dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}")
        print("="*80)
        
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        success = False
        
        try:
            # Set paths
            txn_path = txn_path or self.config.INPUT_TXN_PATH
            rev_path = rev_path or self.config.INPUT_REV_PATH
            output_path = output_path or self.config.OUTPUT_PATH
            
            # STEP 1: Load Data
            print("\n" + "="*80)
            print("STEP 1: LOAD DATA")
            print("="*80)
            
            tbl1 = self.spark.read.format("delta").load(txn_path)
            tbl2 = self.spark.read.format("delta").load(rev_path)
            
            print(f"✅ Loaded transaction data: {tbl1.count():,} records")
            print(f"✅ Loaded revenue data: {tbl2.count():,} records")
            
            # STEP 2: Validate Data
            print("\n" + "="*80)
            print("STEP 2: VALIDATE DATA")
            print("="*80)
            
            # Schema validation
            txn_valid = self.validator.validate_schema(
                tbl1, ['cus_code', 'month', 'txn_amount'], 'Transaction'
            )
            rev_valid = self.validator.validate_schema(
                tbl2, ['cus_code', 'month', 'revenue'], 'Revenue'
            )
            
            if not (txn_valid and rev_valid):
                raise ValueError("Schema validation failed")
            
            # Quality validation
            txn_quality = self.validator.validate_data_quality(tbl1, 'Transaction')
            rev_quality = self.validator.validate_data_quality(tbl2, 'Revenue')
            
            if txn_quality['status'] == 'FAILED' or rev_quality['status'] == 'FAILED':
                raise ValueError("Data quality validation failed")
            
            # STEP 3: Process Mapping
            print("\n" + "="*80)
            print("STEP 3: PROCESS MAPPING")
            print("="*80)
            
            mapped_df = self.engine.process(tbl1, tbl2)
            
            # STEP 4: Quality Assurance
            print("\n" + "="*80)
            print("STEP 4: QUALITY ASSURANCE")
            print("="*80)
            
            qa_results = self.qa.check_mapping_quality(mapped_df, tbl1, tbl2)
            
            if not qa_results['passed']:
                self.monitor.send_alert(
                    subject="Quality Check Failed",
                    message="Mapping results failed quality checks",
                    severity="ERROR"
                )
                # Continue but log warning
                print("⚠️  Quality checks failed, but continuing...")
            
            # STEP 5: Save Results
            print("\n" + "="*80)
            print("STEP 5: SAVE RESULTS")
            print("="*80)
            
            # Partition by date for better query performance
            mapped_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("txn_month") \
                .option("overwriteSchema", "true") \
                .save(f"{output_path}/mappings_{run_id}")
            
            print(f"✅ Results saved to {output_path}/mappings_{run_id}")
            
            # Create/update table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS prod.txn_rev_mappings
                USING DELTA
                LOCATION '{output_path}/mappings_{run_id}'
            """)
            
            print("✅ Table prod.txn_rev_mappings created/updated")
            
            # STEP 6: Save Metadata
            print("\n" + "="*80)
            print("STEP 6: SAVE METADATA")
            print("="*80)
            
            metadata = {
                'run_id': run_id,
                'timestamp': datetime.now().isoformat(),
                'input_txn_path': txn_path,
                'input_rev_path': rev_path,
                'output_path': output_path,
                'mode': mode,
                'stats': self.engine.get_stats(),
                'validation': self.validator.validation_results,
                'quality': qa_results
            }
            
            # Save as JSON
            metadata_json = json.dumps(metadata, indent=2, default=str)
            dbutils.fs.put(
                f"{output_path}/metadata_{run_id}.json",
                metadata_json,
                overwrite=True
            )
            
            print(f"✅ Metadata saved")
            
            # STEP 7: Monitoring & Alerting
            print("\n" + "="*80)
            print("STEP 7: MONITORING & ALERTING")
            print("="*80)
            
            # Log metrics
            self.monitor.log_metrics(self.engine.get_stats())
            
            # Check SLA
            sla_met = self.monitor.check_sla(self.engine.get_stats())
            
            # Success notification
            if qa_results['passed'] and sla_met:
                self.monitor.send_alert(
                    subject=f"Pipeline Success - {run_id}",
                    message=f"Successfully processed {self.engine.stats['total_mappings']:,} mappings",
                    severity="INFO"
                )
                success = True
            
            # STEP 8: Generate Reports
            print("\n" + "="*80)
            print("STEP 8: GENERATE REPORTS")
            print("="*80)
            
            # Validation report
            validation_report = self.validator.get_validation_report()
            dbutils.fs.put(
                f"{output_path}/validation_report_{run_id}.txt",
                validation_report,
                overwrite=True
            )
            print(f"✅ Validation report saved")
            
            # Quality report
            quality_report = self.qa.generate_quality_report(qa_results)
            dbutils.fs.put(
                f"{output_path}/quality_report_{run_id}.txt",
                quality_report,
                overwrite=True
            )
            print(f"✅ Quality report saved")
            
            # Final summary
            print("\n" + "="*80)
            print("PIPELINE SUMMARY")
            print("="*80)
            print(f"Run ID: {run_id}")
            print(f"Status: {'✅ SUCCESS' if success else '⚠️  COMPLETED WITH WARNINGS'}")
            print(f"Total Mappings: {self.engine.stats['total_mappings']:,}")
            print(f"Duration: {self.engine.stats['duration_seconds']:.2f} seconds")
            print(f"Coverage: {self.engine.stats.get('coverage_pct', 0):.1f}%")
            print("="*80)
            
            return success
            
        except Exception as e:
            print(f"\n❌ PIPELINE FAILED: {str(e)}")
            
            import traceback
            error_details = traceback.format_exc()
            
            self.monitor.send_alert(
                subject=f"Pipeline Failed - {run_id}",
                message=f"Error: {str(e)}\n\n{error_details}",
                severity="CRITICAL"
            )
            
            # Save error log
            dbutils.fs.put(
                f"{self.config.OUTPUT_PATH}/error_{run_id}.log",
                error_details,
                overwrite=True
            )
            
            return False

print("✅ ProductionPipeline Class Defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run Production Pipeline

# COMMAND ----------

# Initialize pipeline
config = Config()
pipeline = ProductionPipeline(spark, config)

# Run pipeline
success = pipeline.run(
    txn_path="/mnt/prod/transactions",
    rev_path="/mnt/prod/revenue",
    output_path="/mnt/prod/mapping_results",
    mode='batch'
)

if success:
    print("\n✅✅✅ PIPELINE COMPLETED SUCCESSFULLY ✅✅✅")
else:
    print("\n⚠️⚠️⚠️ PIPELINE COMPLETED WITH ISSUES ⚠️⚠️⚠️")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Query Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent mappings
# MAGIC SELECT 
# MAGIC   cus_code,
# MAGIC   txn_month,
# MAGIC   txn_amount,
# MAGIC   rev_month,
# MAGIC   revenue,
# MAGIC   lag_months,
# MAGIC   mapping_method,
# MAGIC   processed_at
# MAGIC FROM prod.txn_rev_mappings
# MAGIC ORDER BY processed_at DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary statistics by method
# MAGIC SELECT 
# MAGIC   mapping_method,
# MAGIC   COUNT(*) as num_mappings,
# MAGIC   COUNT(DISTINCT cus_code) as num_customers,
# MAGIC   AVG(revenue) as avg_revenue,
# MAGIC   AVG(txn_amount) as avg_txn,
# MAGIC   AVG(revenue / NULLIF(txn_amount, 0)) as avg_ratio,
# MAGIC   AVG(lag_months) as avg_lag
# MAGIC FROM prod.txn_rev_mappings
# MAGIC GROUP BY mapping_method

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Incremental Processing

# COMMAND ----------

def run_incremental_pipeline(start_date: str, end_date: str):
    """
    Run pipeline for specific date range (incremental mode)
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    print(f"🔄 Running incremental pipeline for {start_date} to {end_date}")
    
    # Filter data by date range
    tbl1_filtered = spark.read.format("delta").load(Config.INPUT_TXN_PATH) \
        .filter((col('month') >= start_date) & (col('month') <= end_date))
    
    tbl2_filtered = spark.read.format("delta").load(Config.INPUT_REV_PATH) \
        .filter((col('month') >= start_date) & (col('month') <= end_date))
    
    print(f"   Transactions: {tbl1_filtered.count():,}")
    print(f"   Revenue: {tbl2_filtered.count():,}")
    
    # Save filtered data to temp location
    temp_path = f"/tmp/mapping_incremental_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    tbl1_filtered.write.format("delta").mode("overwrite").save(f"{temp_path}/txn")
    tbl2_filtered.write.format("delta").mode("overwrite").save(f"{temp_path}/rev")
    
    # Run pipeline
    pipeline = ProductionPipeline(spark, Config())
    success = pipeline.run(
        txn_path=f"{temp_path}/txn",
        rev_path=f"{temp_path}/rev",
        output_path=Config.OUTPUT_PATH,
        mode='incremental'
    )
    
    # Cleanup temp
    dbutils.fs.rm(temp_path, recurse=True)
    
    return success

# Example: Process last month
from datetime import datetime, timedelta

last_month = (datetime.now().replace(day=1) - timedelta(days=1))
start_date = last_month.replace(day=1).strftime('%Y-%m-01')
end_date = last_month.strftime('%Y-%m-%d')

# run_incremental_pipeline(start_date, end_date)
print(f"To run incremental: run_incremental_pipeline('{start_date}', '{end_date}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Scheduling (Databricks Jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schedule Configuration
# MAGIC 
# MAGIC To schedule this notebook as a Databricks Job:
# MAGIC 
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "TxnRevMapping_Production",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "run_mapping_pipeline",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Production/05_production_deployment",
# MAGIC         "base_parameters": {
# MAGIC           "mode": "batch",
# MAGIC           "txn_path": "/mnt/prod/transactions",
# MAGIC           "rev_path": "/mnt/prod/revenue"
# MAGIC         }
# MAGIC       },
# MAGIC       "new_cluster": {
# MAGIC         "spark_version": "13.3.x-scala2.12",
# MAGIC         "node_type_id": "i3.xlarge",
# MAGIC         "num_workers": 4,
# MAGIC         "spark_conf": {
# MAGIC           "spark.sql.adaptive.enabled": "true"
# MAGIC         }
# MAGIC       },
# MAGIC       "timeout_seconds": 3600,
# MAGIC       "max_retries": 2,
# MAGIC       "email_notifications": {
# MAGIC         "on_failure": ["dangphdh@company.com"],
# MAGIC         "on_success": ["dangphdh@company.com"]
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 2 * * ?",
# MAGIC     "timezone_id": "UTC",
# MAGIC     "pause_status": "UNPAUSED"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC **Schedule:** Daily at 2:00 AM UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Rollback Mechanism

# COMMAND ----------

def rollback_to_previous_version(table_name: str = "prod.txn_rev_mappings"):
    """
    Rollback table to previous version using Delta Time Travel
    
    Args:
        table_name: Table name to rollback
    """
    print(f"🔄 Rolling back {table_name} to previous version...")
    
    # Get version history
    history = spark.sql(f"DESCRIBE HISTORY {table_name}").limit(5)
    display(history)
    
    # Get previous version
    versions = spark.sql(f"DESCRIBE HISTORY {table_name}").select("version").limit(2).collect()
    
    if len(versions) < 2:
        print("❌ No previous version available")
        return False
    
    previous_version = versions[1]["version"]
    
    print(f"Rolling back to version {previous_version}...")
    
    # Restore
    spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {previous_version}")
    
    print(f"✅ Rollback completed to version {previous_version}")
    return True

# Example usage (commented out for safety):
# rollback_to_previous_version()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Performance Optimization Tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Optimization Checklist
# MAGIC 
# MAGIC #### 1. **Data Skew Handling**
# MAGIC ```python
# MAGIC # Add salt to skewed keys
# MAGIC df = df.withColumn("salt", (rand() * 10).cast("int"))
# MAGIC df = df.repartition(col("cus_code"), col("salt"))
# MAGIC ```
# MAGIC 
# MAGIC #### 2. **Caching Strategy**
# MAGIC ```python
# MAGIC # Cache frequently accessed data
# MAGIC recommendations.cache()
# MAGIC recommendations.count()  # Trigger cache
# MAGIC ```
# MAGIC 
# MAGIC #### 3. **Adaptive Query Execution (AQE)**
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC ```
# MAGIC 
# MAGIC #### 4. **Z-Ordering for Delta Tables**
# MAGIC ```sql
# MAGIC OPTIMIZE prod.txn_rev_mappings
# MAGIC ZORDER BY (cus_code, txn_month)
# MAGIC ```
# MAGIC 
# MAGIC #### 5. **Broadcast Joins**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC df.join(broadcast(small_df), "key")
# MAGIC ```
# MAGIC 
# MAGIC #### 6. **Partition Pruning**
# MAGIC ```python
# MAGIC # Partition by month for time-range queries
# MAGIC df.write.partitionBy("txn_month").save(path)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Monitoring Dashboard Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring view
# MAGIC CREATE OR REPLACE VIEW prod.mapping_monitoring AS
# MAGIC SELECT 
# MAGIC   DATE(processed_at) as process_date,
# MAGIC   mapping_method,
# MAGIC   COUNT(*) as total_mappings,
# MAGIC   COUNT(DISTINCT cus_code) as unique_customers,
# MAGIC   AVG(revenue) as avg_revenue,
# MAGIC   AVG(txn_amount) as avg_transaction,
# MAGIC   AVG(revenue / NULLIF(txn_amount, 0)) as avg_conversion_rate,
# MAGIC   MIN(processed_at) as first_processed,
# MAGIC   MAX(processed_at) as last_processed
# MAGIC FROM prod.txn_rev_mappings
# MAGIC GROUP BY DATE(processed_at), mapping_method

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View monitoring dashboard
# MAGIC SELECT * FROM prod.mapping_monitoring
# MAGIC ORDER BY process_date DESC, mapping_method

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎉 PRODUCTION PIPELINE COMPLETE
# MAGIC 
# MAGIC ## Next Steps:
# MAGIC 
# MAGIC 1. **Test Pipeline**: Run with sample data
# MAGIC 2. **Configure Scheduling**: Set up Databricks Jobs
# MAGIC 3. **Setup Monitoring**: Configure alerts and dashboards
# MAGIC 4. **Document**: Create runbook and operational procedures
# MAGIC 5. **Deploy**: Move to production environment
# MAGIC 
# MAGIC ## Support:
# MAGIC - **Owner:** dangphdh
# MAGIC - **Email:** dangphdh@company.com
# MAGIC - **Documentation:** Confluence/Wiki link
# MAGIC - **On-call:** PagerDuty rotation
# MAGIC 
# MAGIC ---
