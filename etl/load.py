from sqlalchemy import create_engine
import os

engine = create_engine(os.getenv("DATABASE_URL"))

def load_data(pipelines_df, runs_df):
    pipelines_df.to_sql("pipelines", engine, if_exists="replace", index=False)
    runs_df.to_sql("pipeline_runs", engine, if_exists="replace", index=False)