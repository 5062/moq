use moq_relay::*;

use anyhow::Context;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: moq_native::jemalloc::tikv_jemallocator::Jemalloc = moq_native::jemalloc::tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	// TODO: It would be nice to remove this and rely on feature flags only.
	// However, some dependency is pulling in `ring` and I don't know why, so meh for now.
	rustls::crypto::aws_lc_rs::default_provider()
		.install_default()
		.expect("failed to install default crypto provider");

	let mut config = Config::load()?;

	config.client.quic.max_streams.get_or_insert(DEFAULT_MAX_STREAMS);
	config.server.quic.max_streams.get_or_insert(DEFAULT_MAX_STREAMS);

	let mtls_enabled = !config.server.tls.root.is_empty();

	#[allow(unused_mut)]
	let mut server = config.server.init()?;
	let client = config.client.clone().init()?;

	// `None` for a stream-only server (no QUIC); any other error is real.
	let addr = match server.local_addr() {
		Ok(addr) => Some(addr),
		Err(moq_native::Error::NoBackend(_)) => None,
		Err(err) => return Err(err).context("failed to resolve the QUIC bind address"),
	};

	#[cfg(feature = "iroh")]
	let (server, client) = match config.iroh.bind(&config.client.quic).await? {
		Some(iroh) => (server.with_iroh(iroh.clone()), client.with_iroh(iroh)),
		None => (server, client),
	};

	// Reject configs where neither JWT nor mTLS can authenticate anyone.
	if config.auth.is_empty() {
		anyhow::ensure!(
			mtls_enabled,
			"no auth-key, auth-key-dir, public path, or server tls.root configured; \
			 nobody can authenticate"
		);
		tracing::warn!("no JWT/public auth configured; only mTLS peers will be accepted");
	}

	let auth = if config.auth.is_empty() {
		// mTLS-only: no JWT/public source, but `--auth-mtls-tier` still applies.
		Auth::default().with_mtls_tier(config.auth.mtls_tier.clone())
	} else {
		config.auth.init(&config.client.tls).await?
	};

	let cache = config.cache.init()?;
	let cluster = Cluster::new(config.cluster)?
		.with_cache(cache)
		.with_client(client)
		.with_client_tls(config.client.tls.build()?);
	// Keep the producer alive for the whole run: its publish task stops when
	// the last clone drops. The cluster only needs the counter registry.
	let stats = config.stats.build(cluster.origin.clone());
	let trace = config.trace.build()?;
	let cluster = cluster.with_stats(stats.registry().clone());

	// Internal (ops) listener (plain HTTP, opt-in via `--internal-listen`) for
	// /metrics + /health, separate from the customer-facing web server. No-op
	// when unconfigured.
	let internal = Internal::new(config.internal, cluster.stats.clone());

	// Create a web server too. mTLS for HTTPS is opt-in via `--web-https-root`.
	let web = Web::new(auth.clone(), cluster.clone(), server.certificates(), config.web);

	match addr {
		Some(addr) => tracing::info!(%addr, "listening"),
		None => tracing::info!("listening (stream transports only)"),
	}

	#[cfg(unix)]
	// Notify systemd that we're ready after all initialization is complete
	let _ = sd_notify::notify(&[sd_notify::NotifyState::Ready]);

	#[cfg(feature = "jemalloc")]
	let jemalloc = moq_native::jemalloc::run();
	#[cfg(not(feature = "jemalloc"))]
	let jemalloc = std::future::pending::<anyhow::Result<()>>();

	let server_run = async {
		tokio::select! {
			Err(err) = cluster.clone().run() => Err(err).context("cluster failed"),
			Err(err) = web.run() => Err(err).context("web server failed"),
			Err(err) = internal.run() => Err(err).context("internal server failed"),
			Err(err) = serve(server, cluster, auth) => Err(err).context("server failed"),
			Err(err) = jemalloc => Err(err).context("jemalloc profiler failed"),
			else => Ok(()),
		}
	};
	#[cfg(feature = "trace")]
	{
		let shutdown = async {
			if let Err(err) = tokio::signal::ctrl_c().await {
				tracing::warn!(%err, "failed to listen for interrupt");
			}
		};
		run_until_shutdown(trace, server_run, shutdown).await
	}
	#[cfg(not(feature = "trace"))]
	{
		let _trace = trace;
		server_run.await
	}
}

#[cfg(feature = "trace")]
async fn run_until_shutdown<F, S>(trace: Trace, server: F, shutdown: S) -> anyhow::Result<()>
where
	F: std::future::Future<Output = anyhow::Result<()>>,
	S: std::future::Future<Output = ()>,
{
	tokio::pin!(server);
	tokio::pin!(shutdown);
	let result = tokio::select! {
		result = &mut server => result,
		() = &mut shutdown => Ok(()),
	};
	anyhow::ensure!(trace.flush(), "failed to flush relay trace");
	result
}

async fn serve(mut server: moq_native::Server, cluster: Cluster, auth: Auth) -> anyhow::Result<()> {
	let mut conn_id = 0;

	while let Some(request) = server.accept().await {
		let conn = Connection {
			id: conn_id,
			request,
			cluster: cluster.clone(),
			auth: auth.clone(),
		};

		conn_id += 1;
		tokio::spawn(async move {
			if let Err(err) = conn.run().await {
				tracing::warn!(%err, "connection closed");
			}
		});
	}

	anyhow::bail!("stopped accepting connections")
}

#[cfg(all(test, feature = "trace"))]
mod tests {
	use super::*;

	#[tokio::test]
	async fn shutdown_flushes_trace_writer() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let mut config = TraceConfig::default();
		config.path = Some(path.clone());
		let trace = config.build().unwrap();
		let object = moq_trace::global().object(moq_trace::ObjectContext::new(
			moq_trace::Direction::Tx,
			moq_trace::ObjectIdentity::new(1, 2, 3),
			moq_trace::LogicalId::new(4, 5),
		));
		object.finish();

		run_until_shutdown(
			trace,
			std::future::pending::<anyhow::Result<()>>(),
			std::future::ready(()),
		)
		.await
		.unwrap();

		let output = std::fs::read_to_string(path).unwrap();
		assert_eq!(output.lines().count(), 3);
		assert!(output.lines().next().unwrap().contains(r#""type":"trace_header""#));
		assert!(output.ends_with('\n'));
	}
}
