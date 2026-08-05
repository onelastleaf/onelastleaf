use super::*;

pub(super) async fn send_replica_reject<S>(
    channel: &mut SessionChannel<S>,
    transfer_id: &str,
    code: ReplicaTransferRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: Option<u64>,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send_progress(
            sync_envelope::Payload::ReplicaTransferReject(ReplicaTransferReject {
                transfer_id: transfer_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            reply_to,
            "replica_reject_send",
        )
        .await?;
    Ok(())
}

pub(super) async fn send_blob_reject<S>(
    channel: &mut SessionChannel<S>,
    transfer_id: &str,
    code: BlobTransferRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: Option<u64>,
) -> Result<(), RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send_progress(
            sync_envelope::Payload::BlobTransferReject(BlobTransferReject {
                transfer_id: transfer_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            reply_to,
            "blob_reject_send",
        )
        .await?;
    Ok(())
}

pub(super) async fn reject_round<S, T>(
    channel: &mut SessionChannel<S>,
    round_id: &str,
    code: SyncRoundRejectCode,
    message: &'static str,
    correlation_id: &str,
    reply_to: u64,
) -> Result<T, RoundError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    channel
        .send_progress(
            sync_envelope::Payload::RoundReject(SyncRoundReject {
                round_id: round_id.to_owned(),
                code: code as i32,
                message: message.to_owned(),
            }),
            correlation_id,
            Some(reply_to),
            "round_reject_send",
        )
        .await?;
    Err(RoundError::Rejected(message.to_owned()))
}
