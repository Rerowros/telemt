use super::*;
use crate::web::session::{ConveyorError, GetUpOffer, GetUpReject};

/// Handles one bounded legacy or explicitly negotiated conveyor uplink.
pub(super) async fn handle_up(
    mut request: Request<RequestBody>,
    runtime: Arc<WebProcessRuntime>,
    vhost: Arc<WebRuntimeVhost>,
    token_hash: crate::web::manager::TokenHash,
) -> HttpResponse {
    if !matches!(*request.method(), Method::POST | Method::PUT) || !binary_content_type(&request) {
        return serve_decoy(request, vhost, true, &runtime).await;
    }
    let Some(sequence) = canonical_u64_header(&request, "x-up-seq").filter(|value| *value != 0)
    else {
        return serve_decoy(request, vhost, true, &runtime).await;
    };
    let Ok(session) = runtime.get_session(token_hash, &vhost.host) else {
        return serve_decoy(request, vhost, true, &runtime).await;
    };
    if session.carrier().uses_websocket() {
        return serve_decoy(request, vhost, true, &runtime).await;
    }
    if let Some(trace) = request_trace(&request) {
        trace.set_route(TraceRoute::Uplink);
        trace.bind_identity(session.trace_identity());
    }
    let Some(lane_id) = carrier_lane(&request, session.carrier()) else {
        return serve_decoy(request, vhost, true, &runtime).await;
    };

    let confirmed = if request.headers().contains_key("x-telemt-up-confirmed") {
        let Some(value) = canonical_u64_header(&request, "x-telemt-up-confirmed") else {
            return serve_decoy(request, vhost, true, &runtime).await;
        };
        Some(value)
    } else {
        None
    };
    if let Some(parts) = request.extensions_mut().remove::<get::GetUpParts>() {
        return handle_get_up(
            request,
            runtime,
            vhost,
            session,
            get::GetUpOperation {
                lane_id,
                sequence,
                confirmed,
            },
            parts,
        )
        .await;
    }
    let claim = match session.claim_conveyor(lane_id, sequence, confirmed) {
        Ok(claim) => claim,
        Err(ConveyorError::Stale | ConveyorError::Mode) => {
            return carrier_empty(StatusCode::CONFLICT);
        }
        Err(ConveyorError::Manager(
            ManagerError::Backpressure | ManagerError::Concurrent | ManagerError::Limit,
        )) => {
            return service_unavailable();
        }
        Err(_) => return serve_decoy(request, vhost, true, &runtime).await,
    };
    let limit = session.limits().max_body_bytes;
    let CollectedBody {
        request,
        body,
        _body_budget,
    } = match collect_body(
        request,
        &runtime,
        Duration::from_secs(session.timeouts().body_secs),
        limit,
        false,
    )
    .await
    {
        Ok(result) => result,
        Err(CollectBodyError::Limit) => return service_unavailable(),
        Err(CollectBodyError::Invalid(request)) => {
            return serve_decoy(request, vhost, true, &runtime).await;
        }
    };
    if let Some(trace) = request_trace(&request) {
        trace.record_frames(TraceDirection::Request, &body, session.limits());
    }
    let result = if let Some(claim) = claim {
        let _deadline_lease = match super::request_deadline(&request) {
            Some(deadline) => {
                match deadline.lease_for(Duration::from_secs(session.timeouts().body_secs)) {
                    Some(lease) => Some(lease),
                    None => return service_unavailable(),
                }
            }
            None => None,
        };
        claim.process(&body).await
    } else {
        match lane_id {
            Some(lane_id) => session.process_up_lane(lane_id, sequence, &body),
            None => session.process_up(sequence, &body),
        }
        .map_err(ConveyorError::Manager)
    };
    match result {
        Ok(ack) => {
            let mut response = carrier_empty(StatusCode::NO_CONTENT);
            insert_header(
                &mut response,
                HeaderName::from_static("x-up-ack"),
                &ack.to_string(),
            );
            response
        }
        Err(ConveyorError::Stale | ConveyorError::Mode) => carrier_empty(StatusCode::CONFLICT),
        Err(ConveyorError::Manager(
            ManagerError::Backpressure | ManagerError::Concurrent | ManagerError::Limit,
        )) => service_unavailable(),
        Err(_) => serve_decoy(request, vhost, true, &runtime).await,
    }
}

/// Handles one strict GET uplink fragment: intermediate parts only enter
/// per-session reassembly while the final part drives the canonical claim
/// and frame application exactly like one POST body.
async fn handle_get_up(
    request: Request<RequestBody>,
    runtime: Arc<WebProcessRuntime>,
    vhost: Arc<WebRuntimeVhost>,
    session: Arc<crate::web::session::WebSession>,
    operation: get::GetUpOperation,
    parts: get::GetUpParts,
) -> HttpResponse {
    let part = parts.part;
    let (body, _budget) = match session.offer_get_up(
        operation.lane_id,
        operation.sequence,
        operation.confirmed,
        parts.part,
        parts.total,
        parts.data,
    ) {
        Ok(GetUpOffer::Pending(part)) => {
            let mut response = carrier_empty(StatusCode::NO_CONTENT);
            insert_header(&mut response, get::up_part_header_name(), &part.to_string());
            return response;
        }
        Ok(GetUpOffer::Duplicate) => {
            // The sequence already applied: acknowledge the replay exactly like
            // the canonical duplicate path without reprocessing any frames.
            let mut response = carrier_empty(StatusCode::NO_CONTENT);
            insert_header(&mut response, get::up_part_header_name(), &part.to_string());
            if part == parts.total - 1 {
                insert_header(
                    &mut response,
                    HeaderName::from_static("x-up-ack"),
                    &operation.sequence.to_string(),
                );
            }
            return response;
        }
        Ok(GetUpOffer::Complete { body, budget }) => (body, budget),
        Err(GetUpReject::Busy) => return service_unavailable(),
        Err(GetUpReject::Decoy) => {
            return serve_decoy(request, vhost, true, &runtime).await;
        }
    };
    if let Some(trace) = request_trace(&request) {
        trace.record_frames(TraceDirection::Request, &body, session.limits());
    }
    let claim =
        match session.claim_conveyor(operation.lane_id, operation.sequence, operation.confirmed) {
            Ok(claim) => claim,
            Err(ConveyorError::Stale | ConveyorError::Mode) => {
                return carrier_empty(StatusCode::CONFLICT);
            }
            Err(ConveyorError::Manager(
                ManagerError::Backpressure | ManagerError::Concurrent | ManagerError::Limit,
            )) => {
                return service_unavailable();
            }
            Err(_) => return serve_decoy(request, vhost, true, &runtime).await,
        };
    let result = if let Some(claim) = claim {
        let _deadline_lease = match super::request_deadline(&request) {
            Some(deadline) => {
                match deadline.lease_for(Duration::from_secs(session.timeouts().body_secs)) {
                    Some(lease) => Some(lease),
                    None => {
                        return service_unavailable();
                    }
                }
            }
            None => None,
        };
        claim.process(&body).await
    } else {
        match operation.lane_id {
            Some(lane_id) => session.process_up_lane(lane_id, operation.sequence, &body),
            None => session.process_up(operation.sequence, &body),
        }
        .map_err(ConveyorError::Manager)
    };
    match result {
        Ok(ack) => {
            let mut response = carrier_empty(StatusCode::NO_CONTENT);
            insert_header(
                &mut response,
                HeaderName::from_static("x-up-ack"),
                &ack.to_string(),
            );
            insert_header(&mut response, get::up_part_header_name(), &part.to_string());
            response
        }
        Err(ConveyorError::Stale | ConveyorError::Mode) => carrier_empty(StatusCode::CONFLICT),
        Err(ConveyorError::Manager(
            ManagerError::Backpressure | ManagerError::Concurrent | ManagerError::Limit,
        )) => service_unavailable(),
        Err(_) => serve_decoy(request, vhost, true, &runtime).await,
    }
}
