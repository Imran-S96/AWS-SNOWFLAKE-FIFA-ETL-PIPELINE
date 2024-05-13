CREATE FIFA TABLE

CREATE TABLE IF NOT EXISTS FIFA_PLAYER_INFO (
    Name VARCHAR(100) PRIMARY KEY,
    Nationality VARCHAR(50),
    Age INT,
    Rating FLOAT,
    Club VARCHAR(100), 
    Height_cm INT,
    Foot VARCHAR(10),
    Position VARCHAR(50),
    Value_euro FLOAT,
    Weekly_Wage_euro FLOAT,
    PAC INT,
    SHO INT,
    PAS INT,
    DRI INT,
    DEF INT,
    PHY INT
);