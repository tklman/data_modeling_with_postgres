import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import io

def to_buffer(df):
    """
    This function converts a pandas dataframe into a buffer that can be used by the psycopg2 copy_from function.
    :param df:
    :return:
    """
    s_buf = io.StringIO()
    df.to_csv(s_buf, index=False, sep='\t', header=None)
    s_buf.seek(0)
    return s_buf

def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the song file
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    cur.copy_from(file=to_buffer(song_data), table='sti_songs')
    cur.execute(song_table_insert)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    cur.copy_from(file=to_buffer(artist_data), table='sti_artists')
    cur.execute(artist_table_insert)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    At last the songplays table will be populated with song play data after a lookup of
    the song_id and artist_id.

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the song file
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    column_labels = ('datetime', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(
        [t.values.astype('str'), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]).transpose()
    time_df.columns = column_labels

    cur.copy_from(file=to_buffer(time_df), table='sti_time')
    cur.execute(time_table_insert)


    #for i, row in time_df.iterrows():
    #    cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    cur.copy_from(file=to_buffer(user_df), table='sti_users')
    cur.execute(user_table_insert)

    #for i, row in user_df.iterrows():
    #    cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        #cur.copy_from(file=to_buffer(songplay_data), table='songplays')


def process_data(cur, conn, filepath, func):
    """
    This procedure applies one of the two functions above on the JSON files
    found in the directory and sub directories following the filepath parameter.

    INPUTS:
    * cur the cursor variable
    * conn the connection variable
    * filepath the file path of the necessary files
    * func the function that needs to be applied
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()