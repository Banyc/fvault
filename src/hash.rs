use std::{
    io,
    pin::{Pin, pin},
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug)]
pub struct HashRead;
#[derive(Debug)]
pub struct HashWrite;

#[derive(Debug)]
pub struct HashStream<S, Rw> {
    stream: S,
    hasher: blake3::Hasher,
    _rw: Rw,
}
impl<S, Rw> HashStream<S, Rw> {
    pub fn new(stream: S, rw: Rw) -> Self {
        Self {
            stream,
            hasher: blake3::Hasher::new(),
            _rw: rw,
        }
    }
    pub fn finalize(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}
impl<S> AsyncWrite for HashStream<S, HashWrite>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let n = ready!(pin!(&mut self.stream).poll_write(cx, buf))?;
        self.hasher.update(&buf[..n]);
        Ok(n).into()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        pin!(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        pin!(&mut self.stream).poll_shutdown(cx)
    }
}
impl<S> AsyncRead for HashStream<S, HashRead>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start = buf.filled().len();
        ready!(pin!(&mut self.stream).poll_read(cx, buf))?;
        self.hasher.update(&buf.filled()[start..]);
        Ok(()).into()
    }
}
