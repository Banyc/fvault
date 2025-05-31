use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const BUF_SIZE: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct CopyConfig {
    pub limit: Option<usize>,
}
pub async fn copy_2<R, W>(reader: &mut R, writer: &mut W, config: &CopyConfig) -> CopyResult
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut amt = 0;
    let mut buf: Vec<u8> = vec![0; BUF_SIZE];
    loop {
        let buf_size = match config.limit {
            Some(limit) => limit - amt,
            None => buf.len(),
        };
        if buf_size == 0 {
            break;
        }
        let buf = &mut buf[..buf_size];
        let n = match reader.read(buf).await {
            Ok(n) => n,
            Err(e) => return dbg!(CopyResult { amt, res: Err(e) }),
        };
        let is_eof = n == 0;
        if is_eof {
            break;
        }
        let buf = &buf[..n];
        match writer.write_all(buf).await {
            Ok(_) => (),
            Err(e) => return dbg!(CopyResult { amt, res: Err(e) }),
        };
        amt += n;
    }
    CopyResult { amt, res: Ok(()) }
}
#[derive(Debug)]
pub struct CopyResult {
    pub amt: usize,
    pub res: io::Result<()>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basics() {
        let a = vec![0, 1, 2, 3];
        {
            let mut b = vec![];
            let mut r = io::Cursor::new(&a[..]);
            let mut w = io::Cursor::new(&mut b);
            let config = CopyConfig { limit: None };
            copy_2(&mut r, &mut w, &config).await;
            assert_eq!(a, b);
        }
        {
            let mut b = vec![];
            let mut r = io::Cursor::new(&a[..]);
            let mut w = io::Cursor::new(&mut b);
            let config = CopyConfig { limit: Some(2) };
            copy_2(&mut r, &mut w, &config).await;
            assert_eq!(b, [0, 1]);
        }
    }
}
