CREATE OR REPLACE FUNCTION __schema__.read_any_transactions_in_progress(
    _datname NAME,
    _txids   BIGINT[]
)
  RETURNS BOOLEAN
AS $F$
BEGIN
  RETURN (
  SELECT EXISTS(
    SELECT 1 
    FROM pg_stat_activity AS activity
    INNER JOIN (
        SELECT txid_snapshot_xip(txid_current_snapshot()) AS txid
    ) AS in_progress_txs 
    ON activity.backend_xid::TEXT::BIGINT = in_progress_txs.txid
    WHERE datname = _datname AND in_progress_txs.txid = ANY(_txids))
  );
END;
$F$
LANGUAGE 'plpgsql';