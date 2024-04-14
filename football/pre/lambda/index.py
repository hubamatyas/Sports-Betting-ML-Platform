import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, update, insert
from sqlalchemy.exc import IntegrityError
from pre_football_lambda import LeagueDataClient, GenDataFrame

def create_db_engine():
    db_username = 'your_username'
    db_password = 'your_password'
    db_host = 'your_host'
    db_port = 'your_port'
    db_name = 'your_db_name'
    return create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

def generate_dataframe(client, league_name, country, years):
    return GenDataFrame(client, league_name, country, years).main()

def get_rows_with_changed_status(new_df, current_db):
    new_df.set_index('id', inplace=True)
    current_db.set_index('id', inplace=True)
    changed_status_ids = new_df.index[new_df['status'] != current_db.loc[new_df.index, 'status']]
    return new_df.loc[changed_status_ids]

def update_or_insert_row(engine, table_name, row):
    table = Table(table_name, MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        update_stmt = update(table).where(table.c.id == row['id']).values(row.to_dict())
        result = connection.execute(update_stmt)
        if result.rowcount == 0:
            try:
                connection.execute(insert(table).values(row.to_dict()))
                print(f"Inserted new row with id {row['id']}")
            except IntegrityError:
                print(f"Row with id {row['id']} could not be inserted.")
        elif result.rowcount == 1:
            print(f"Updated row with id {row['id']}")
        connection.commit()

def main():
    engine = create_db_engine()
    api_key = "your_api_key"
    client = LeagueDataClient(api_key)
    league_name = "Premier League"
    country = "England"
    years = ['2023']

    new_df = generate_dataframe(client, league_name, country, years)
    current_db = pd.read_sql_table('pre_football', engine)

    rows_to_update = get_rows_with_changed_status(new_df, current_db)
    rows_to_update.reset_index(inplace=True)

    # Convert specific columns to string if necessary
    for col in ['homeGoals', 'awayGoals', 'homeGoals_timings', 'awayGoals_timings']:
        rows_to_update[col] = rows_to_update[col].astype(str)

    for index, row in rows_to_update.iterrows():
        update_or_insert_row(engine, 'pre_football', row)

if __name__ == "__main__":
    main()
