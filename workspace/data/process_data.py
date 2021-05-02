import sys
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = pd.merge(messages, categories, on=["id"])
    
    return(df, categories)


def clean_data(df, categories):
    
    categories = categories['categories'].str.split(';').apply(pd.Series)
    
    row = categories.iloc[0].values.flatten().tolist()
    category_colnames = [i.split('-', 1)[0] for i in row]
    categories.columns = category_colnames
    
    for column in categories:
        categories[column] = categories[column].str.strip().str[-1]
        categories[column] = pd.to_numeric(categories[column])
    
    df = df.drop('categories', axis = 1)
    df = pd.concat([df, categories], axis=1)
    df = df.drop_duplicates()
    
    return(df)


def save_data(df, database_filename):
    
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('Table', engine, index=False)

    pass  


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df, categories = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df, categories)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()