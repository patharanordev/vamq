use anyhow::Result;
use zmq::{Context, PULL, Socket};

pub fn bind_pull(addr: &str) -> Result<Socket> {
    let ctx = Context::new();
    let sock = ctx.socket(PULL)?;
    // High Water Mark (HWM) — avoid memory blowups and fast-close
    sock.set_rcvhwm(512)?;
    // Avoid blocking on shutdown
    sock.set_linger(0)?;
    sock.bind(addr)?;
    Ok(sock)
}
