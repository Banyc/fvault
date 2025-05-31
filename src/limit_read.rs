use std::{
    io,
    pin::{Pin, pin},
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf};

#[derive(Debug)]
pub struct LimitReader<R> {
    reader: R,
    limit: usize,
    pos: usize,
}
impl<R> LimitReader<R> {
    pub fn new(reader: R, limit: usize) -> Self {
        Self {
            reader,
            limit,
            pos: 0,
        }
    }
}
impl<R> AsyncRead for LimitReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.limit - self.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut limit_buf = buf.take(remaining);
        let buf_filled_start = limit_buf.filled().len();
        let res = pin!(&mut self.reader).poll_read(cx, &mut limit_buf);
        let buf_filled_end = limit_buf.filled().len();
        let filled = buf_filled_end - buf_filled_start;
        self.pos += filled;
        buf.advance(filled);
        res
    }
}
