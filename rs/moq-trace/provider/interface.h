#ifndef MOQ_TRACE_INTERFACE_H
#define MOQ_TRACE_INTERFACE_H

#include <stdbool.h>
#include <stdint.h>

enum moq_trace_direction {
	MOQ_TRACE_DIRECTION_RX,
	MOQ_TRACE_DIRECTION_TX,
};

enum moq_trace_protocol {
	MOQ_TRACE_PROTOCOL_MOQ_TRANSPORT,
};

enum moq_trace_edge {
	MOQ_TRACE_EDGE_START,
	MOQ_TRACE_EDGE_DONE,
};

enum moq_trace_packet_space {
	MOQ_TRACE_PACKET_SPACE_INITIAL,
	MOQ_TRACE_PACKET_SPACE_HANDSHAKE,
	MOQ_TRACE_PACKET_SPACE_ZERO_RTT,
	MOQ_TRACE_PACKET_SPACE_DATA,
};

enum moq_trace_object_phase {
	MOQ_TRACE_OBJECT_PHASE_HEADER_PARSE,
	MOQ_TRACE_OBJECT_PHASE_CREATE,
	MOQ_TRACE_OBJECT_PHASE_PAYLOAD_READ,
	MOQ_TRACE_OBJECT_PHASE_FRAME_COMMIT,
	MOQ_TRACE_OBJECT_PHASE_CLONE,
	MOQ_TRACE_OBJECT_PHASE_HEADER_ENCODE,
	MOQ_TRACE_OBJECT_PHASE_PAYLOAD_WRITE,
};

enum moq_trace_object_outcome {
	MOQ_TRACE_OBJECT_OUTCOME_SUCCESS,
	MOQ_TRACE_OBJECT_OUTCOME_FAILED,
	MOQ_TRACE_OBJECT_OUTCOME_ABANDONED,
};

enum moq_trace_packet_phase {
	MOQ_TRACE_PACKET_PHASE_HEADER_PARSE,
	MOQ_TRACE_PACKET_PHASE_ROUTING,
	MOQ_TRACE_PACKET_PHASE_SCHEDULING,
	MOQ_TRACE_PACKET_PHASE_HEADER_UNPROTECT,
	MOQ_TRACE_PACKET_PHASE_PAYLOAD_DECRYPT,
	MOQ_TRACE_PACKET_PHASE_FRAME_PROCESS,
	MOQ_TRACE_PACKET_PHASE_FRAME_ENCODE,
	MOQ_TRACE_PACKET_PHASE_PACKET_ENCRYPT,
};

enum moq_trace_packet_outcome {
	MOQ_TRACE_PACKET_OUTCOME_SUCCESS,
	MOQ_TRACE_PACKET_OUTCOME_MALFORMED,
	MOQ_TRACE_PACKET_OUTCOME_AUTHENTICATION_FAILED,
	MOQ_TRACE_PACKET_OUTCOME_DROPPED,
	MOQ_TRACE_PACKET_OUTCOME_ABANDONED,
};

enum moq_trace_socket_outcome {
	MOQ_TRACE_SOCKET_OUTCOME_SUCCESS,
	MOQ_TRACE_SOCKET_OUTCOME_PENDING,
	MOQ_TRACE_SOCKET_OUTCOME_WOULD_BLOCK,
	MOQ_TRACE_SOCKET_OUTCOME_CONNECTION_RESET,
	MOQ_TRACE_SOCKET_OUTCOME_ERROR,
	MOQ_TRACE_SOCKET_OUTCOME_ABANDONED,
};

struct moq_trace_moq_object_start {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint64_t logical_group;
	uint64_t logical_frame;
	uint8_t has_session_id;
	uint64_t session_id;
	uint8_t has_connection_id;
	uint64_t connection_id;
	uint8_t direction;
	uint8_t protocol;
	uint64_t track_alias;
	uint64_t group_id;
	uint64_t object_id;
	uint8_t has_stream_id;
	uint64_t stream_id;
	uint8_t has_stream_offset_start;
	uint64_t stream_offset_start;
	uint64_t sample_rate;
};

struct moq_trace_moq_object_end {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t has_stream_offset_end;
	uint64_t stream_offset_end;
	uint64_t payload_bytes;
};

struct moq_trace_moq_object_phase {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t phase;
	uint8_t edge;
	uint8_t has_outcome;
	uint8_t outcome;
};

struct moq_trace_quic_packet_start {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint64_t connection_id;
	uint8_t direction;
	uint8_t has_packet_number;
	uint64_t packet_number;
	uint8_t has_packet_space;
	uint8_t packet_space;
	uint8_t has_byte_len;
	uint64_t byte_len;
	uint64_t sample_rate;
};

struct moq_trace_quic_packet_end {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t has_packet_number;
	uint64_t packet_number;
	uint8_t has_packet_space;
	uint8_t packet_space;
	uint8_t has_byte_len;
	uint64_t byte_len;
	uint8_t outcome;
};

struct moq_trace_quic_packet_phase {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t phase;
	uint8_t edge;
	uint8_t has_outcome;
	uint8_t outcome;
};

struct moq_trace_quic_stream_frame {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint64_t stream_id;
	uint64_t offset_start;
	uint64_t offset_end;
	uint8_t outcome;
};

struct moq_trace_udp_socket_start {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t has_connection_id;
	uint64_t connection_id;
	uint8_t direction;
	uint64_t sample_rate;
};

struct moq_trace_udp_socket_end {
	uint64_t timestamp_ns;
	uint64_t trace_id;
	uint8_t outcome;
	uint64_t buffers;
	uint64_t datagrams;
	uint64_t bytes;
};

bool moq_trace_moq_object_start_enabled(void);
void moq_trace_moq_object_start(const struct moq_trace_moq_object_start *event);
bool moq_trace_moq_object_end_enabled(void);
void moq_trace_moq_object_end(const struct moq_trace_moq_object_end *event);
bool moq_trace_moq_object_phase_enabled(void);
void moq_trace_moq_object_phase(const struct moq_trace_moq_object_phase *event);
bool moq_trace_quic_packet_start_enabled(void);
void moq_trace_quic_packet_start(const struct moq_trace_quic_packet_start *event);
bool moq_trace_quic_packet_end_enabled(void);
void moq_trace_quic_packet_end(const struct moq_trace_quic_packet_end *event);
bool moq_trace_quic_packet_phase_enabled(void);
void moq_trace_quic_packet_phase(const struct moq_trace_quic_packet_phase *event);
bool moq_trace_quic_stream_frame_enabled(void);
void moq_trace_quic_stream_frame(const struct moq_trace_quic_stream_frame *event);
bool moq_trace_udp_socket_start_enabled(void);
void moq_trace_udp_socket_start(const struct moq_trace_udp_socket_start *event);
bool moq_trace_udp_socket_end_enabled(void);
void moq_trace_udp_socket_end(const struct moq_trace_udp_socket_end *event);
void moq_trace_provider_init(void);

#endif
