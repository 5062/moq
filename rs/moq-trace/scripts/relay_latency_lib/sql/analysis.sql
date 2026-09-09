CREATE VIEW failed_object_traces AS
SELECT DISTINCT trace_id
FROM moq_object_phase
WHERE edge = 'done' AND outcome <> 'success';

CREATE VIEW object_lifecycles AS
SELECT
    start.* EXCLUDE (ctf_timestamp_ns, timestamp_ns),
    start.ctf_timestamp_ns AS start_ctf_timestamp_ns,
    start.timestamp_ns AS start_ns,
    finish.ctf_timestamp_ns AS end_ctf_timestamp_ns,
    finish.timestamp_ns AS end_ns,
    finish.stream_offset_end,
    finish.payload_bytes
FROM moq_object_start AS start
JOIN moq_object_end AS finish USING (trace_id)
ANTI JOIN failed_object_traces USING (trace_id);

CREATE VIEW packet_lifecycles AS
SELECT
    start.* EXCLUDE (ctf_timestamp_ns, timestamp_ns),
    start.ctf_timestamp_ns AS start_ctf_timestamp_ns,
    start.timestamp_ns AS start_ns,
    finish.ctf_timestamp_ns AS end_ctf_timestamp_ns,
    finish.timestamp_ns AS end_ns,
    finish.outcome
FROM quic_packet_start AS start
JOIN quic_packet_end AS finish USING (trace_id);

CREATE VIEW object_phase_intervals AS
WITH paired AS (
    SELECT starts.trace_id, starts.span_id, starts.phase,
           starts.ctf_timestamp_ns, starts.timestamp_ns AS start_ns,
           finishes.timestamp_ns AS end_ns, finishes.outcome
    FROM moq_object_phase AS starts
    JOIN moq_object_phase AS finishes USING (trace_id, span_id, phase)
    WHERE starts.edge = 'start' AND finishes.edge = 'done'
)
SELECT trace_id, span_id, phase,
       row_number() OVER (
           PARTITION BY trace_id, phase ORDER BY ctf_timestamp_ns, start_ns, span_id
       ) - 1 AS occurrence,
       start_ns, end_ns, outcome
FROM paired;

CREATE VIEW packet_phase_intervals AS
WITH paired AS (
    SELECT starts.trace_id, starts.span_id, starts.phase,
           starts.ctf_timestamp_ns, starts.timestamp_ns AS start_ns,
           finishes.timestamp_ns AS end_ns, finishes.outcome
    FROM quic_packet_phase AS starts
    JOIN quic_packet_phase AS finishes USING (trace_id, span_id, phase)
    WHERE starts.edge = 'start' AND finishes.edge = 'done'
)
SELECT trace_id, span_id, phase,
       row_number() OVER (
           PARTITION BY trace_id, phase ORDER BY ctf_timestamp_ns, start_ns, span_id
       ) - 1 AS occurrence,
       start_ns, end_ns, outcome
FROM paired;
