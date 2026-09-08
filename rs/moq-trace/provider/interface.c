#define LTTNG_UST_TRACEPOINT_CREATE_PROBES
#define LTTNG_UST_TRACEPOINT_DEFINE
#include "events.h"

#if defined(__has_attribute)
#if __has_attribute(retain)
#define MOQ_TRACE_RETAIN __attribute__((used, retain))
#endif
#endif
#ifndef MOQ_TRACE_RETAIN
#define MOQ_TRACE_RETAIN __attribute__((used))
#endif

/* Keep these references alive when release linking performs section GC. */
static void *volatile tracepoints[] MOQ_TRACE_RETAIN = {
		&lttng_ust_tracepoint_ptr_moq_trace___moq_object_start,
		&lttng_ust_tracepoint_ptr_moq_trace___moq_object_end,
		&lttng_ust_tracepoint_ptr_moq_trace___moq_object_phase,
		&lttng_ust_tracepoint_ptr_moq_trace___quic_packet_start,
		&lttng_ust_tracepoint_ptr_moq_trace___quic_packet_end,
		&lttng_ust_tracepoint_ptr_moq_trace___quic_packet_phase,
		&lttng_ust_tracepoint_ptr_moq_trace___quic_stream_frame,
		&lttng_ust_tracepoint_ptr_moq_trace___udp_socket_start,
		&lttng_ust_tracepoint_ptr_moq_trace___udp_socket_end,
	};

void moq_trace_provider_init(void) {
	(void) tracepoints;
}

bool moq_trace_moq_object_start_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, moq_object_start); }
void moq_trace_moq_object_start(const struct moq_trace_moq_object_start *event) { lttng_ust_tracepoint(moq_trace, moq_object_start, event); }
bool moq_trace_moq_object_end_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, moq_object_end); }
void moq_trace_moq_object_end(const struct moq_trace_moq_object_end *event) { lttng_ust_tracepoint(moq_trace, moq_object_end, event); }
bool moq_trace_moq_object_phase_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, moq_object_phase); }
void moq_trace_moq_object_phase(const struct moq_trace_moq_object_phase *event) { lttng_ust_tracepoint(moq_trace, moq_object_phase, event); }
bool moq_trace_quic_packet_start_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, quic_packet_start); }
void moq_trace_quic_packet_start(const struct moq_trace_quic_packet_start *event) { lttng_ust_tracepoint(moq_trace, quic_packet_start, event); }
bool moq_trace_quic_packet_end_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, quic_packet_end); }
void moq_trace_quic_packet_end(const struct moq_trace_quic_packet_end *event) { lttng_ust_tracepoint(moq_trace, quic_packet_end, event); }
bool moq_trace_quic_packet_phase_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, quic_packet_phase); }
void moq_trace_quic_packet_phase(const struct moq_trace_quic_packet_phase *event) { lttng_ust_tracepoint(moq_trace, quic_packet_phase, event); }
bool moq_trace_quic_stream_frame_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, quic_stream_frame); }
void moq_trace_quic_stream_frame(const struct moq_trace_quic_stream_frame *event) { lttng_ust_tracepoint(moq_trace, quic_stream_frame, event); }
bool moq_trace_udp_socket_start_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, udp_socket_start); }
void moq_trace_udp_socket_start(const struct moq_trace_udp_socket_start *event) { lttng_ust_tracepoint(moq_trace, udp_socket_start, event); }
bool moq_trace_udp_socket_end_enabled(void) { return lttng_ust_tracepoint_enabled(moq_trace, udp_socket_end); }
void moq_trace_udp_socket_end(const struct moq_trace_udp_socket_end *event) { lttng_ust_tracepoint(moq_trace, udp_socket_end, event); }
