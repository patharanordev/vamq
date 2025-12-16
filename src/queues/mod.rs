use tracing::warn;

pub mod ingress;
pub mod wsg_pub;

pub fn create_consumer(address: &str) -> Option<zmq::Socket> {
    let maybe_pull: Option<zmq::Socket> = if address.is_empty() {
        None
    } else {
        match ingress::bind_pull(address) {
            Ok(c) => Some(c),
            Err(e) => {
                warn!(
                    "Cannot bind consumer, event loop will still service WS/OpenAI only: {:?}",
                    e
                );
                None
            }
        }
    };
    if maybe_pull.is_none() {
        warn!("ingress disabled; event loop will still service WS/OpenAI only");
    }

    maybe_pull
}
