     SELECT Messages.StreamVersion
       FROM Messages
      WHERE Messages.StreamIdInternal = ?streamIdInternal
   ORDER BY Messages.Position DESC
      LIMIT 1