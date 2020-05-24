CREATE TABLE STREAMS
(
    ID                 CHAR(42)                NOT NULL,
    IDORIGINAL         NVARCHAR2(1000)         NOT NULL,
    IDINTERNAL         NUMBER(10) GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    VERSION            NUMBER(10) DEFAULT (-1) NOT NULL,
    POSITION           INT        DEFAULT (-1) NOT NULL,
    MAXAGE             NUMBER(10) DEFAULT (NULL),
    MAXCOUNT           NUMBER(10) DEFAULT (NULL),
    IDORIGINALREVERSED NVARCHAR2(1000)         NOT NULL,
    CONSTRAINT "STREAMS_PK" PRIMARY KEY (IDINTERNAL) ENABLE
);


CREATE UNIQUE INDEX IX_STREAMS_ID ON STREAMS (ID ASC);
CREATE INDEX IX_STREAMS_ID_ORIGINAL ON STREAMS (IDORIGINAL ASC, IDINTERNAL ASC);
CREATE INDEX IX_STREAMS_ID_ORIGINALREV ON STREAMS (IDORIGINALREVERSED ASC, IDINTERNAL ASC);

CREATE TABLE STREAMEVENTS
(
    STREAMIDINTERNAL NUMBER(10)     NOT NULL,
    STREAMVERSION    NUMBER(10)     NOT NULL,
    "POSITION"       INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    ID               VARCHAR2(40)   NOT NULL,
    CREATED          TIMESTAMP DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP),
    "TYPE"           NVARCHAR2(128) NOT NULL,
    JSONDATA         NCLOB          NOT NULL,
    JSONMETA         NCLOB          NULL,
    CONSTRAINT "STREAMEVENTS_PK" PRIMARY KEY ("POSITION") ENABLE,
    CONSTRAINT "STREAMEVENTS_FK_STREAMS" FOREIGN KEY (STREAMIDINTERNAL) REFERENCES STREAMS (IDINTERNAL)
);

CREATE UNIQUE INDEX IX_STREAMEVENTS_ID ON STREAMEVENTS (STREAMIDINTERNAL ASC, ID ASC);
CREATE INDEX IX_STREAMEVENTS_VERSION ON STREAMEVENTS (STREAMIDINTERNAL ASC, STREAMVERSION ASC);
CREATE INDEX IX_STREAMEVENTS_CREATED ON STREAMEVENTS (STREAMIDINTERNAL ASC, CREATED ASC);

CREATE OR REPLACE TYPE STREAMNEWMESSAGE AS OBJECT
(
    ID              VARCHAR2(40),
    "STREAMVERSION" NUMBER(10),
    "TYPE"          NVARCHAR2(128),
    JSONDATA        NCLOB,
    JSONMETA        NCLOB
);

CREATE OR REPLACE TYPE STREAMNEWMESSAGES IS TABLE OF STREAMNEWMESSAGE;

CREATE OR REPLACE TYPE STREAMAPPENDED AS OBJECT
(
    CURRENTVERSION NUMBER(10),
    CURRENTPOSITION INT
);

CREATE OR REPLACE PROCEDURE STREAM_SETMETA(
    P_StreamId IN CHAR,
    P_MaxAge IN NUMBER,
    P_MaxCount IN NUMBER)
    IS
    V_InternalId NUMBER(10);
BEGIN
    SELECT STREAMS.IdInternal INTO V_InternalId FROM STREAMS
    WHERE STREAMS.ID = P_StreamId
        FOR UPDATE;

    IF (NOT (V_InternalId IS NULL))
    THEN
        UPDATE STREAMS
        SET STREAMS.MaxAge = P_MaxAge,
            STREAMS.MaxCount = P_MaxCount
        WHERE STREAMS.IdInternal = V_InternalId;
    END IF;
END;

CREATE OR REPLACE PROCEDURE STREAM_READ(
    P_StreamId IN CHAR,
    P_Count    IN  NUMBER,
    P_Version  IN  NUMBER,
    P_Forwards IN  NUMBER,
    P_Prefetch IN  NUMBER,
    oStreamInfo OUT SYS_REFCURSOR,
    oStreamEvents OUT SYS_REFCURSOR
)
    IS
    V_StreamIdInternal NUMBER(10);
    V_StreamIdOriginal NVARCHAR2(1000);
BEGIN
    SELECT STREAMS.IdInternal, STREAMS.IdOriginal
    INTO V_StreamIdInternal, v_StreamIdOriginal
    FROM STREAMS
    WHERE STREAMS.ID = P_StreamId;

    OPEN oStreamInfo FOR
        SELECT STREAMS.Version,
               STREAMS.Position,
               STREAMS.MaxAge  as max_age
        FROM STREAMS
        WHERE STREAMS.IdInternal = V_StreamIdInternal;

    OPEN oStreamEvents FOR
        SELECT V_StreamIdOriginal AS StreamId,
               STREAMEVENTS.ID,
               STREAMEVENTS.STREAMVERSION,
               STREAMEVENTS.Position,
               STREAMEVENTS.Created,
               STREAMEVENTS.Type,
               STREAMEVENTS.JSONMETA,
               (CASE P_Prefetch
                    WHEN 1 THEN STREAMEVENTS.JSONDATA
                    ELSE NULL END)
        FROM STREAMEVENTS
        WHERE
                CASE
                    WHEN P_Forwards = 1 AND STREAMEVENTS.StreamVersion >= P_Version AND STREAMEVENTS.StreamIdInternal = V_StreamIdInternal
                        THEN 1
                    WHEN P_Forwards = 0 AND STREAMEVENTS.StreamVersion <= P_version AND STREAMEVENTS.StreamIdInternal = V_StreamIdInternal
                        THEN 1
                    ELSE 0
                    END = 1
        ORDER BY (CASE
                      WHEN P_forwards = 1
                          THEN STREAMEVENTS.StreamVersion
                      ELSE   STREAMEVENTS.StreamVersion * -1
            END)
            OFFSET 0 ROWS FETCH FIRST P_count ROWS ONLY
    ;
END;

CREATE OR REPLACE PROCEDURE STREAM_ENSUREAPPENDEVENTSIDEMPOTENT(
    P_StreamInternalId          IN NUMBER,
    P_StartPos          IN INT,
    P_CheckLength       IN NUMBER,
    NewStreamMessages   IN STREAMNEWMESSAGES)
    IS
    
    e_wrong_version EXCEPTION;
    PRAGMA exception_init( e_wrong_version, -20001 );
        
    TYPE ArrayString IS TABLE OF VARCHAR2(40);

    V_EXISTING_EVENT_IDS ArrayString;
    V_MESSAGE_COUNT NUMBER;

BEGIN

    V_MESSAGE_COUNT := NewStreamMessages.count;

    SELECT STREAMEVENTS.ID
        BULK COLLECT INTO V_EXISTING_EVENT_IDS
    FROM STREAMEVENTS
    WHERE STREAMEVENTS.STREAMIDINTERNAL = P_StreamInternalId AND STREAMEVENTS.StreamVersion >= P_StartPos
    ORDER BY STREAMEVENTS.StreamVersion DESC
        OFFSET 0 ROWS FETCH FIRST V_MESSAGE_COUNT ROWS ONLY;

    IF (P_CheckLength = 1 AND NewStreamMessages.count > V_EXISTING_EVENT_IDS.count)
    THEN
        RAISE e_wrong_version;
    END IF;

    FOR i IN NewStreamMessages.FIRST .. NewStreamMessages.LAST
        LOOP
            IF (NewStreamMessages(i).ID <> V_EXISTING_EVENT_IDS(i))
            THEN
                RAISE e_wrong_version;
            END IF;
        END LOOP;
END;

CREATE OR REPLACE FUNCTION STREAM_APPEND_EXPECTEDVERSION(
    P_StreamId           in CHAR,
    P_ExpectedVersion    in NUMBER,
    P_NewStreamMessages in STREAMNEWMESSAGES)
    RETURN STREAMAPPENDED
AS
    e_wrong_version EXCEPTION;
    PRAGMA exception_init( e_wrong_version, -20001 );

    e_wrong_expected_version EXCEPTION;
    PRAGMA exception_init( e_wrong_expected_version, -20002 );

    V_StreamVersion    NUMBER(10);
    V_StreamIdInternal  NUMBER(10);
    V_LatestPosition     INT;
    V_LatestVersion   NUMBER(10);
    V_NewMessage      STREAMNEWMESSAGE;
BEGIN
    BEGIN
        SELECT STREAMS.IdInternal, STREAMS.Version, STREAMS.Position
        INTO V_StreamIdInternal, V_StreamVersion, V_LatestPosition
        FROM STREAMS
        WHERE STREAMS.Id = P_StreamId
            FOR UPDATE;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RAISE e_wrong_version;
    END;

    /* Stream should exist */
    IF (V_StreamIdInternal IS NULL)
    THEN
        RAISE e_wrong_version;
    END IF;

    /* Expected version should match current version */
    IF (V_StreamVersion != P_ExpectedVersion)
    THEN
        /* IDEMPOTENCY */
        /* If it doesn't, maybe these message were already appended at the expected version */
        /* This will throw if they were not.. */
        STREAM_ENSUREAPPENDEVENTSIDEMPOTENT(
                V_StreamIdInternal,
                P_ExpectedVersion,
                1,
                P_NewStreamMessages);

        /* .. or return to allow us to confirm */
        RETURN STREAMAPPENDED(V_LatestVersion, V_LatestPosition);
    END IF;

    V_LatestVersion := V_StreamVersion;
    FOR V_i IN P_NewStreamMessages.FIRST .. P_NewStreamMessages.LAST
        LOOP
            V_LatestVersion := V_LatestVersion + 1;
            V_NewMessage := P_NewStreamMessages(V_i);
            INSERT INTO STREAMEVENTS (STREAMIDINTERNAL, STREAMVERSION, ID, TYPE, JSONDATA, JSONMETA)
            VALUES (V_StreamIdInternal, V_LatestVersion, V_NewMessage.Id, V_NewMessage.Type, V_NewMessage.JsonData, V_NewMessage.JsonMeta)
            RETURNING STREAMEVENTS.Position INTO V_LatestPosition;
        END LOOP;

    UPDATE STREAMS
    SET STREAMS.Version = V_LatestVersion,
        STREAMS.Position = V_LatestPosition
    WHERE STREAMS.IdInternal = V_StreamIdInternal;


    /* MsSql implementation also returns maxCount = dbo.Streams.MaxCount */
    RETURN STREAMAPPENDED(V_LatestVersion, V_LatestPosition);


END;

CREATE OR REPLACE FUNCTION STREAM_APPEND_NOSTREAM(
    P_StreamId           in CHAR,
    P_MetaStreamId           in CHAR,
    P_StreamIdOriginal   in NVARCHAR2,
    P_NewStreamMessages in STREAMNEWMESSAGES)
    RETURN STREAMAPPENDED
AS
    AppendedResult STREAMAPPENDED;

    V_StreamIdInternal NUMBER(10);
    V_Created NUMBER(10);
    V_MaxAge NUMBER(10);
    V_MaxCount NUMBER(10);
BEGIN
    BEGIN
        SELECT STREAMS.MaxAge, STREAMS.MaxCount
        INTO V_MaxAge, V_Maxcount
        FROM STREAMS
        WHERE STREAMS.ID = P_MetaStreamId;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
    END;
    
    BEGIN

        INSERT INTO STREAMS (Id, IdOriginal, MaxAge, MaxCount)
        VALUES (P_StreamId, P_StreamIdOriginal, V_MaxAge, V_MaxCount)
        RETURNING STREAMS.IDINTERNAL into V_StreamIdInternal;

        V_Created := sql%rowcount;

    EXCEPTION
        /* Allow insert to fail */
        WHEN dup_val_on_index THEN NULL;
    END;

    /* IDEMPOTENCY */
    /* Insert failed but could still contain the messages to be persisted */
    IF (V_Created IS NULL)
    THEN
        /* IDEMPOTENCY */
        /* If it doesn't, maybe these message were already appended at the expected version */
        /* This will throw if they were not.. */
        STREAM_ENSUREAPPENDEVENTSIDEMPOTENT(
                V_StreamIdInternal,
                0,
                1,
                P_NewStreamMessages);

        /* .. or return to allow us to confirm */
        SELECT STREAMAPPENDED(STREAMS.Version, STREAMS.Position)
        INTO AppendedResult
        FROM STREAMS
        WHERE STREAMS.IdInternal = V_StreamIdInternal;

        RETURN AppendedResult;

    END IF;

    /* Continue with append with expected version zero */
    RETURN STREAM_APPEND_EXPECTEDVERSION(P_StreamId, 0, P_NewStreamMessages);

END;

CREATE OR REPLACE FUNCTION STREAM_APPEND_ANYVERSION(
    P_StreamId           in CHAR,
    P_MetaStreamId           in CHAR,
    P_StreamIdOriginal   in NVARCHAR2,
    P_NewStreamMessages in STREAMNEWMESSAGES)
    RETURN STREAMAPPENDED
AS
    V_StreamIdInternal NUMBER(10);
    V_Created NUMBER(10);
    V_MaxAge NUMBER(10);
    V_MaxCount NUMBER(10);
    V_LatestPosition INT;
    V_LatestVersion NUMBER(10);
    V_NewMessage      STREAMNEWMESSAGE;
BEGIN
    BEGIN
        SELECT STREAMS.MaxAge, STREAMS.MaxCount
        INTO V_MaxAge, V_Maxcount
        FROM STREAMS
        WHERE STREAMS.ID = P_MetaStreamId;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
    END;
    
    BEGIN

        INSERT INTO STREAMS (Id, IdOriginal, MaxAge, MaxCount)
        VALUES (P_StreamId, P_StreamIdOriginal, V_MaxAge, V_MaxCount)
        RETURNING STREAMS.IDINTERNAL into V_StreamIdInternal;

        V_Created := sql%rowcount;

    EXCEPTION
        /* Allow insert to fail */
        WHEN dup_val_on_index THEN NULL;
    END;

    SELECT STREAMS.IdInternal, STREAMS.Version, STREAMS.Position
    INTO V_StreamIdInternal, V_LatestVersion, V_LatestPosition
    FROM STREAMS
    WHERE STREAMS.Id = P_StreamId
        FOR UPDATE;

    IF (P_NewStreamMessages.count <= 0)
    THEN
        RETURN STREAMAPPENDED(V_LatestVersion, V_LatestPosition);
    END IF;

    FOR V_i IN P_NewStreamMessages.FIRST .. P_NewStreamMessages.LAST
        LOOP
            V_LatestVersion := V_LatestVersion + 1;
            V_NewMessage := P_NewStreamMessages(V_i);
            INSERT INTO STREAMEVENTS (STREAMIDINTERNAL, STREAMVERSION, ID, TYPE, JSONDATA, JSONMETA)
            VALUES (V_StreamIdInternal, V_LatestVersion, V_NewMessage.Id, V_NewMessage.Type, V_NewMessage.JsonData, V_NewMessage.JsonMeta)
            RETURNING STREAMEVENTS.Position INTO V_LatestPosition;
        END LOOP;

    UPDATE STREAMS
    SET STREAMS.Version = V_LatestVersion,
        STREAMS.Position = V_LatestPosition
    WHERE STREAMS.IdInternal = V_StreamIdInternal;

    /* MsSql implementation also returns maxCount = dbo.Streams.MaxCount */
    RETURN STREAMAPPENDED(V_LatestVersion, V_LatestPosition);

END;









