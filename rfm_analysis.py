"""
RFM Segmentation Analysis
=========================

Standalone Python script to perform RFM customer segmentation
based on the processed ETL data.
"""

import warnings
from datetime import timedelta

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

warnings.filterwarnings('ignore')

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)

def main():
    print("="*80)
    print("RFM SEGMENTATION ANALYSIS")
    print("="*80)

    # Load processed fact table
    fact_df = pd.read_csv('data/processed/dwh/fact_order_items.csv')
    customers_df = pd.read_csv('data/processed/staging/raw_customers.csv')

    print(f"\nFact table shape: {fact_df.shape}")
    print(f"Customers table shape: {customers_df.shape}")

    # Convert timestamp to datetime
    fact_df['order_purchase_timestamp'] = pd.to_datetime(fact_df['order_purchase_timestamp'])
    print("\n✓ Data loaded and prepared")

    # ============================================================================
    # 1. RFM METRICS CALCULATION
    # ============================================================================

    print("\n" + "="*60)
    print("1. RFM METRICS CALCULATION")
    print("="*60)

    # Filter to delivered orders only
    delivered_orders = fact_df[fact_df['is_delivered'] == 1].copy()

    # Calculate RFM metrics
    current_date = delivered_orders['order_purchase_timestamp'].max() + timedelta(days=1)

    rfm = delivered_orders.groupby('customer_id').agg({
        'order_purchase_timestamp': [
            ('recency_days', lambda x: (current_date - x.max()).days),
            ('first_order_date', 'min'),
            ('last_order_date', 'max')
        ],
        'order_id': 'nunique',  # Frequency
        'total_value': 'sum'    # Monetary
    }).round(2)

    # Flatten column names
    rfm.columns = ['recency_days', 'first_order_date', 'last_order_date', 'frequency', 'monetary']
    rfm = rfm.reset_index()

    print(f"✓ RFM metrics calculated for {len(rfm)} customers")
    print(f"  - Recency range: {rfm['recency_days'].min()} - {rfm['recency_days'].max()} days")
    print(f"  - Frequency range: {rfm['frequency'].min()} - {rfm['frequency'].max()} orders")
    print(f"  - Monetary range: R$ {rfm['monetary'].min():.2f} - R$ {rfm['monetary'].max():.2f}")

    # ============================================================================
    # 2. RFM SCORING
    # ============================================================================

    print("\n" + "="*60)
    print("2. RFM SCORING")
    print("="*60)

    # Create quintiles for scoring (1-5 scale)
    # Note: For recency, lower is better (more recent)
    # Handle cases where there's not enough variation
    try:
        rfm['r_score'] = pd.qcut(rfm['recency_days'], 5, labels=[5, 4, 3, 2, 1], duplicates='drop').astype(int)
    except ValueError:
        # If not enough unique values, use ranking
        rfm['r_score'] = pd.cut(rfm['recency_days'].rank(method='dense', pct=True),
                               bins=5, labels=[5, 4, 3, 2, 1]).astype(int)

    try:
        rfm['f_score'] = pd.qcut(rfm['frequency'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop').astype(int)
    except ValueError:
        # Most customers have frequency=1, so assign score based on whether >1
        rfm['f_score'] = (rfm['frequency'] > 1).astype(int) + 1  # 1 for freq=1, 2 for freq>1

    try:
        rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop').astype(int)
    except ValueError:
        # If not enough variation, use ranking
        rfm['m_score'] = pd.cut(rfm['monetary'].rank(method='dense', pct=True),
                               bins=5, labels=[1, 2, 3, 4, 5]).astype(int)

    # Combine scores
    rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)

    print("✓ RFM scores assigned using quintiles")
    print(f"  - R scores: {rfm['r_score'].value_counts().sort_index().to_dict()}")
    print(f"  - F scores: {rfm['f_score'].value_counts().sort_index().to_dict()}")
    print(f"  - M scores: {rfm['m_score'].value_counts().sort_index().to_dict()}")

    # ============================================================================
    # 3. CUSTOMER SEGMENTATION
    # ============================================================================

    print("\n" + "="*60)
    print("3. CUSTOMER SEGMENTATION")
    print("="*60)

    def assign_segment(row):
        r, f, m = row['r_score'], row['f_score'], row['m_score']

        # Champions: High R, F, M
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        # Loyal Customers: High F, good R/M
        elif f >= 4 and (r >= 3 or m >= 3):
            return 'Loyal Customers'
        # Potential Loyalists: Recent customers, growing frequency
        elif r >= 4 and f >= 2:
            return 'Potential Loyalists'
        # New Customers: High R, low F
        elif r >= 4 and f == 1:
            return 'New Customers'
        # At Risk: High value but not recent
        elif (f >= 3 or m >= 3) and r <= 2:
            return 'At Risk'
        # Lost: Low R, F, M
        else:
            return 'Lost'

    rfm['segment'] = rfm.apply(assign_segment, axis=1)

    # Add customer demographics
    rfm = rfm.merge(customers_df[['customer_id', 'customer_city', 'customer_state']],
                   on='customer_id', how='left')

    segment_counts = rfm['segment'].value_counts()
    print("✓ Customer segments assigned:")
    for segment, count in segment_counts.items():
        pct = count / len(rfm) * 100
        print(f"  - {segment}: {count} customers ({pct:.1f}%)")

    # ============================================================================
    # 4. SEGMENT ANALYSIS
    # ============================================================================

    print("\n" + "="*60)
    print("4. SEGMENT ANALYSIS")
    print("="*60)

    # Calculate segment statistics
    segment_stats = rfm.groupby('segment').agg({
        'recency_days': ['mean', 'median'],
        'frequency': ['mean', 'median'],
        'monetary': ['mean', 'median', 'sum'],
        'customer_id': 'count'
    }).round(2)

    segment_stats.columns = ['recency_mean', 'recency_median', 'freq_mean', 'freq_median',
                           'monetary_mean', 'monetary_median', 'total_revenue', 'customer_count']
    segment_stats = segment_stats.reset_index()

    # Calculate percentages
    segment_stats['pct_customers'] = (segment_stats['customer_count'] / segment_stats['customer_count'].sum() * 100).round(1)
    segment_stats['pct_revenue'] = (segment_stats['total_revenue'] / segment_stats['total_revenue'].sum() * 100).round(1)

    print("✓ Segment statistics calculated:")
    print(segment_stats.to_string(index=False))

    # ============================================================================
    # 5. KEY BUSINESS INSIGHTS
    # ============================================================================

    print("\n" + "="*80)
    print("KEY BUSINESS INSIGHTS")
    print("="*80)

    # 1. Revenue concentration
    top_segment = segment_stats.loc[segment_stats['pct_revenue'].idxmax()]
    print("\n1. REVENUE CONCENTRATION (Pareto Analysis)")
    print(f"   Top segment ({top_segment['segment']}) has:")
    print(f"   - {top_segment['pct_customers']}% of customers")
    print(f"   - {top_segment['pct_revenue']}% of revenue")
    print("   💡 Action: Prioritize retention of high-value segments")

    # 2. At-risk customers
    at_risk = rfm[rfm['segment'] == 'At Risk']
    if len(at_risk) > 0:
        at_risk_revenue = at_risk['monetary'].sum()
        print("\n2. AT-RISK SEGMENT")
        print(f"   - {len(at_risk)} customers ({len(at_risk)/len(rfm)*100:.1f}% of base)")
        print(f"   - Potential revenue at stake: R$ {at_risk_revenue:,.2f}")
        print("   💡 Action: Launch win-back campaign with special offers")

    # 3. New customers
    new_customers = rfm[rfm['segment'] == 'New Customers']
    if len(new_customers) > 0:
        print("\n3. NEW CUSTOMER ONBOARDING")
        print(f"   - {len(new_customers)} customers ({len(new_customers)/len(rfm)*100:.1f}% of base)")
        print(f"   - Average CLV: R$ {new_customers['monetary'].mean():.2f}")
        print("   💡 Action: Implement onboarding program to drive repeat purchase")

    # 4. Loyalty metrics
    repeat_pct = (len(rfm[rfm['frequency'] > 1]) / len(rfm) * 100)
    print("\n4. LOYALTY METRICS")
    print(f"   - Repeat purchase rate: {repeat_pct:.1f}%")
    print(f"   - Average customer value: R$ {rfm['monetary'].mean():.2f}")
    print(f"   💡 Action: Target is >30% repeat rate; current status: {'✓ GOOD' if repeat_pct > 30 else '✗ NEEDS IMPROVEMENT'}")

    # ============================================================================
    # 6. EXPORT RESULTS
    # ============================================================================

    # Save RFM results
    rfm_export = rfm[['customer_id', 'recency_days', 'frequency', 'monetary',
                       'r_score', 'f_score', 'm_score', 'rfm_score', 'segment',
                       'customer_city', 'customer_state']]
    rfm_export.to_csv('data/processed/rfm_segmentation.csv', index=False)

    print("\n✓ RFM segmentation exported to data/processed/rfm_segmentation.csv")
    print(f"   Total customers segmented: {len(rfm_export)}")

    # Save segment statistics
    segment_stats.to_csv('data/processed/rfm_segment_stats.csv', index=False)
    print("✓ Segment statistics exported to data/processed/rfm_segment_stats.csv")

    print("\n" + "="*80)
    print("RFM SEGMENTATION COMPLETED SUCCESSFULLY")
    print("="*80)

if __name__ == '__main__':
    main()
