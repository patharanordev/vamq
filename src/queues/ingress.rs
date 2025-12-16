use anyhow::Result;
use zmq::{Context, PULL, Socket};

pub fn bind_pull(addr: &str) -> Result<Socket> {
    let ctx = Context::new();
    let sock = ctx.socket(PULL)?;
    // Reasonable HWM and fast-close
    sock.set_rcvhwm(512)?;
    sock.set_linger(0)?;
    sock.bind(addr)?;
    Ok(sock)
}
