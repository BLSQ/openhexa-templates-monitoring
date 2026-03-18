from extract import extract_data
from load import load_data
from create_views import create_views


def main():
    pipelines_df, runs_df = extract_data()
    load_data(pipelines_df, runs_df)
    create_views()

if __name__ == "__main__":
    main()