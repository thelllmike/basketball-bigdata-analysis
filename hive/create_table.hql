CREATE TABLE basketball_pbp (
    EVENTID INT,
    EVENTNUM INT,
    GAME_ID INT,
    HOMEDESCRIPTION STRING,
    PCTIMESTRING STRING,
    PERIOD INT,
    PLAYER1_ID INT,
    PLAYER1_NAME STRING,
    PLAYER1_TEAM_ABBREVIATION STRING,
    PLAYER1_TEAM_CITY STRING,
    PLAYER1_TEAM_ID INT,
    PLAYER1_TEAM_NICKNAME STRING,
    PLAYER2_ID INT,
    PLAYER2_NAME STRING,
    PLAYER2_TEAM_ABBREVIATION STRING,
    PLAYER2_TEAM_CITY STRING,
    PLAYER2_TEAM_ID INT,
    PLAYER2_TEAM_NICKNAME STRING,
    SCORE STRING,
    SCOREMARGIN STRING,
    VISITORDESCRIPTION STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/input/basketball_pbp.csv' INTO TABLE basketball_pbp;