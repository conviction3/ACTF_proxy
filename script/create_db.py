import sqlite3

if __name__ == '__main__':
    conn = sqlite3.connect('../data/result_data.db')
    print("open database successfully")
    c = conn.cursor()
    c.execute(
        '''CREATE TABLE seq_data
            (   seq     INTEGER PRIMARY KEY NOT NULL,
                number  INT             NOT NULL
            );
        '''
    )
    conn.commit()
    print("Table created successfully")