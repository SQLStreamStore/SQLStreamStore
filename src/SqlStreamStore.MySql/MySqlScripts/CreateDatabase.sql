CREATE TABLE IF NOT EXISTS Streams (
  Id               CHAR(42)             NOT NULL,
  IdInternal       INT                  NOT NULL AUTO_INCREMENT,
  IdOriginal       NVARCHAR(1000)       NOT NULL,
  Version          INT                  NOT NULL DEFAULT -1,
  Position         BIGINT               NOT NULL DEFAULT -1,
  PRIMARY KEY (IdInternal)
);

CREATE TABLE IF NOT EXISTS Messages (
  Position         BIGINT               NOT NULL AUTO_INCREMENT,
  StreamIdInternal INT                  NOT NULL,
  StreamVersion    INT                  NOT NULL,
  Id               BINARY (16)          NOT NULL,
  Created          BIGINT               NOT NULL,
  Type             NVARCHAR (128)       NOT NULL,
  JsonData         LONGTEXT             NOT NULL,
  JsonMetadata     LONGTEXT,
  PRIMARY KEY (Position),
  FOREIGN KEY (StreamIdInternal) REFERENCES Streams (IdInternal)
);