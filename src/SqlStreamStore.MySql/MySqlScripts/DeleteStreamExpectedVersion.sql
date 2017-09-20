START TRANSACTION;

DELETE FROM Messages
      WHERE Messages.StreamIdInternal = ?streamIdInternal;

DELETE FROM Streams
      WHERE Streams.Id = ?streamId;

COMMIT;