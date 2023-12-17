class SqlQueries:
    songplay_table = {
        "create": ("""
                CREATE TABLE IF NOT EXISTS songplays (
                    playid varchar(32) NOT NULL,
                    start_time timestamp NOT NULL,
                    userid int4 NOT NULL,
                    "level" varchar(256),
                    songid varchar(256),
                    artistid varchar(256),
                    sessionid int4,
                    location varchar(256),
                    user_agent varchar(256),
                    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                );
        """),
        "truncate":("""
            TRUNCATE TABLE songplays;
        """),
        "insert": ("""
                INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
                SELECT
                        md5(events.sessionid || events.start_time) songplay_id,
                        events.start_time, 
                        events.userid, 
                        events.level, 
                        songs.song_id, 
                        songs.artist_id, 
                        events.sessionid, 
                        events.location, 
                        events.useragent
                        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                    FROM staging_events
                    WHERE page='NextSong') events
                    LEFT JOIN staging_songs songs
                    ON events.song = songs.title
                        AND events.artist = songs.artist_name
                        AND events.length = songs.duration
        """)
    }
    user_table = {
        "create": ("""
            CREATE TABLE IF NOT EXISTS users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );
        """),
        "truncate":("""
            TRUNCATE TABLE users;
        """
        ),
        "insert": ("""
            INSERT INTO users (userid, first_name, last_name, gender, level)
            SELECT distinct userid, firstname, lastname, gender, level
            FROM staging_events
            WHERE page='NextSong'
        """)
    }

    song_table = {
        "create": ("""
            CREATE TABLE IF NOT EXISTS songs (
                songid varchar(256) NOT NULL,
                title varchar(512),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );
        """),
        "truncate":("""
            TRUNCATE TABLE songs;
        """),
        "insert": ("""
            INSERT INTO songs (songid, title, artistid, year, duration)
            SELECT distinct song_id, title, artist_id, year, duration
            FROM staging_songs
        """)
    }
    artist_table = {
        "create": ("""
            CREATE TABLE IF NOT EXISTS artists (
                artistid varchar(256) NOT NULL,
                name varchar(512),
                location varchar(512),
                latitude numeric(18,0),
                longitude numeric(18,0)
            );
        """),
        "truncate":("""
            TRUNCATE TABLE artists;
        """),
        "insert": ("""
            INSERT INTO artists (artistid, name, location, latitude, longitude)
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM staging_songs
        """)
    }
    time_table = {
        "create": ("""
            CREATE TABLE IF NOT EXISTS "time" (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            ) ;
        """),
        "truncate":("""
            TRUNCATE TABLE "time";
        """),
        "insert": ("""
            INSERT INTO time (start_time, hour, day, week, month, year, weekday)
            SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
            FROM songplays
        """)
    }


    stage_songs_create = ("""
                           CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs int4,
                                artist_id varchar(256),
                                artist_name varchar(512),
                                artist_latitude numeric(18,0),
                                artist_longitude numeric(18,0),
                                artist_location varchar(512),
                                song_id varchar(256),
                                title varchar(512),
                                duration numeric(18,0),
                                "year" int4
                            );

                           """)
    stage_events_create = ("""
                           CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar(256),
                                auth varchar(256),
                                firstname varchar(256),
                                gender varchar(256),
                                iteminsession int4,
                                lastname varchar(256),
                                length numeric(18,0),
                                "level" varchar(256),
                                location varchar(256),
                                "method" varchar(256),
                                page varchar(256),
                                registration numeric(18,0),
                                sessionid int4,
                                song varchar(256),
                                status int4,
                                ts int8,
                                useragent varchar(256),
                                userid int4
                            );
                           """)
    stage_sql = """
                {create_stmt}
                TRUNCATE TABLE {table};
                COPY {table}
                FROM '{bucket}'
                ACCESS_KEY_ID '{{access}}'
                SECRET_ACCESS_KEY '{{secret}}'
                REGION '{region}'
                json
                '{reference}'
                """

    stage_events_sql = stage_sql.format(
        create_stmt = stage_events_create,
        table = "staging_events",
        bucket = "s3://airflow-bucket-enock/log-data/",
        region = "us-east-1",
        reference = "s3://airflow-bucket-enock/log-data-metadata/log_json_path.json"
    )
    stage_songs_sql = stage_sql.format(
        create_stmt = stage_songs_create,
        table = "staging_songs",
        bucket = "s3://airflow-bucket-enock/song-data/",
        region = "us-east-1",
        reference = "auto"
    )
    
    quality_checks = [
        {
            "query":"SELECT COUNT(*) FROM users where userid=NULL;",
            "check":0
        },
        {
            "query":"SELECT COUNT(*) FROM songs where songid=NULL;",
            "check":0
        },
        {
            "query":"SELECT COUNT(*) FROM artists where artistid=NULL;",
            "check":0
        },
        {
            "query":"SELECT COUNT(*) FROM time where start_time=NULL;",
            "check":0
        }      
    ]