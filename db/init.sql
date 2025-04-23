-- 1. Administrators
CREATE TABLE IF NOT EXISTS administrators (
    id           SERIAL PRIMARY KEY,
    telegram_id  BIGINT    UNIQUE,
    username     VARCHAR(64)  NOT NULL UNIQUE,
    password     VARCHAR(128) NOT NULL,
    role         VARCHAR(16)  NOT NULL CHECK(role IN ('main','senior','admin')) DEFAULT 'admin',
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Registration codes (для /reg)
CREATE TABLE IF NOT EXISTS reg_codes (
    code        VARCHAR(64) PRIMARY KEY,
    role        VARCHAR(16) NOT NULL CHECK(role IN ('senior','admin')),
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Tournaments
CREATE TABLE IF NOT EXISTS tournaments (
    id              SERIAL PRIMARY KEY,
    admin_id        INTEGER REFERENCES administrators(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    tournament_type VARCHAR(32)  NOT NULL CHECK(tournament_type IN ('Beginner','Advanced')),
    status          VARCHAR(32)  NOT NULL CHECK(status IN ('registration','active','ended')) DEFAULT 'registration',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at     TIMESTAMP WITH TIME ZONE,
    data            JSONB
);

-- 4. Players
CREATE TABLE IF NOT EXISTS players (
    id            SERIAL PRIMARY KEY,
    tournament_id INTEGER REFERENCES tournaments(id) ON DELETE CASCADE,
    name          VARCHAR(128) NOT NULL,
    score         INTEGER DEFAULT 0,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Rounds
CREATE TABLE IF NOT EXISTS rounds (
    id            SERIAL PRIMARY KEY,
    tournament_id INTEGER REFERENCES tournaments(id) ON DELETE CASCADE,
    round_type    VARCHAR(16) NOT NULL CHECK(round_type IN ('simple','final')),
    data          JSONB       NOT NULL,
    status        VARCHAR(16) NOT NULL CHECK(status IN ('pending','done')) DEFAULT 'pending',
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 6. Matches
CREATE TABLE IF NOT EXISTS matches (
    id            SERIAL PRIMARY KEY,
    round_id      INTEGER REFERENCES rounds(id) ON DELETE CASCADE,
    table_number  INTEGER,
    player1_id    INTEGER REFERENCES players(id) ON DELETE SET NULL,
    player2_id    INTEGER REFERENCES players(id) ON DELETE SET NULL,
    result        JSONB,
    status        VARCHAR(16) DEFAULT 'scheduled',
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);