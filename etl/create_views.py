import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")


VIEWS_SQL = [

    # 1. Template Adoption Rate
    """
    CREATE OR REPLACE VIEW kpi_adoption_rate AS
    SELECT
        COUNT(*) FILTER (WHERE template_id IS NOT NULL)::float
        / NULLIF(COUNT(*), 0) AS adoption_rate
    FROM pipelines;
    """,

    # 2. Template Usage by Template
    """
    CREATE OR REPLACE VIEW kpi_template_usage AS
    SELECT
        template_id,
        COUNT(*) AS pipeline_count
    FROM pipelines
    WHERE template_id IS NOT NULL
    GROUP BY template_id;
    """,

    # 3. Pipeline Trend (Daily)
    """
    CREATE OR REPLACE VIEW kpi_pipeline_trend AS
    SELECT
        DATE(created_at) AS date,
        COUNT(*) FILTER (WHERE template_id IS NOT NULL) AS template_pipelines,
        COUNT(*) AS total_pipelines
    FROM pipelines
    GROUP BY DATE(created_at)
    ORDER BY date;
    """,

    # 4. First 3 Runs Failure Rate
    """
    CREATE OR REPLACE VIEW kpi_first_3_failures AS
    WITH ranked_runs AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY execution_date) AS run_order
        FROM pipeline_runs r
    )
    SELECT
        p.pipeline_id,
        COUNT(*) FILTER (WHERE r.status != 'SUCCESS')::float / 3 AS failure_rate
    FROM pipelines p
    JOIN ranked_runs r ON p.pipeline_id = r.pipeline_id
    WHERE p.template_id IS NOT NULL
      AND r.run_order <= 3
    GROUP BY p.pipeline_id;
    """,

    # 5. Last Run Status
    """
    CREATE OR REPLACE VIEW kpi_last_run_status AS
    SELECT
        p.pipeline_id,
        r.status
    FROM pipelines p
    JOIN LATERAL (
        SELECT status
        FROM pipeline_runs
        WHERE pipeline_id = p.pipeline_id
        ORDER BY execution_date DESC
        LIMIT 1
    ) r ON true
    WHERE p.template_id IS NOT NULL;
    """,

    # 6. Active Pipeline Rate (Last 30 days)
    """
    CREATE OR REPLACE VIEW kpi_active_pipeline_rate AS
    SELECT
        COUNT(DISTINCT pipeline_id) FILTER (
            WHERE execution_date::timestamp > NOW() - INTERVAL '30 days'
        )::float
        / NULLIF(COUNT(DISTINCT pipeline_id), 0) AS active_rate
    FROM pipeline_runs
    WHERE execution_date IS NOT NULL;
    """,

    # 7. Run Frequency
    """
    CREATE OR REPLACE VIEW kpi_run_frequency AS
    SELECT
        pipeline_id,
        COUNT(*) AS total_runs,
        COUNT(*)::float / GREATEST(
            EXTRACT(
                DAY FROM (
                    MAX(execution_date::timestamp) - MIN(execution_date::timestamp)
                )
            ),
            1
        ) AS runs_per_day
    FROM pipeline_runs
    WHERE execution_date IS NOT NULL
    GROUP BY pipeline_id;
    """
]


def create_views():
    engine = create_engine(DATABASE_URL)

    with engine.connect() as connection:
        for i, view_sql in enumerate(VIEWS_SQL, start=1):
            try:
                connection.execute(text(view_sql))
                connection.commit()
                print(f"✅ View {i} created successfully")
            except Exception as e:
                print(f"❌ Error creating view {i}: {e}")

    print("🎯 All views processed.")


if __name__ == "__main__":
    create_views()