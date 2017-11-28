CREATE UNIQUE INDEX IX_Messages_StreamIdInternal_Revision
  ON Messages (StreamIdInternal, StreamVersion);
