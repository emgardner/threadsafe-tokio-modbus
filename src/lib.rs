use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_modbus::client::{Client, Context, Reader, Writer};
use tokio_modbus::prelude::SlaveContext;
use tokio_modbus::{Address, Quantity, Request, Response, Result, Slave};

#[derive(Debug, Clone)]
pub struct ThreadSafeContext {
    inner: Arc<Mutex<Context>>,
    timeout: Option<Duration>,
    retries: Option<usize>,
}

macro_rules! retry_modbus {
    ($self:ident, |$ctx:ident| $body:expr) => {{
        $self
            .with_retries(|| async {
                let mut $ctx = $self.inner.lock().await;
                $body
            })
            .await
    }};
}

impl ThreadSafeContext {
    pub fn new(context: Context) -> Self {
        Self {
            inner: Arc::new(Mutex::new(context)),
            timeout: None,
            retries: None,
        }
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout
    }

    pub fn retries(&self) -> Option<usize> {
        self.retries
    }

    pub fn set_retries(&mut self, retries: Option<usize>) {
        self.retries = retries
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            timeout: self.timeout,
            retries: self.retries,
        }
    }

    async fn with_retries<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let retries = self.retries.unwrap_or(1);

        for attempt in 0..retries {
            let fut = f();
            let result = if let Some(dur) = self.timeout {
                match tokio::time::timeout(dur, fut).await {
                    Ok(r) => r,
                    Err(_) => {
                        if attempt + 1 == retries {
                            return Err(tokio_modbus::Error::Transport(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Operation timed out",
                            )));
                        }
                        continue;
                    }
                }
            } else {
                fut.await
            };

            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if attempt + 1 == retries {
                        return Err(err);
                    }
                }
            }
        }

        unreachable!()
    }
    pub async fn set_slave(&self, slave: Slave) {
        let mut ctx = self.inner.lock().await;
        ctx.set_slave(slave);
    }

    pub async fn call(&mut self, request: Request<'_>) -> Result<Response> {
        retry_modbus!(self, |ctx| ctx.call(request.clone()).await)
    }

    pub async fn disconnect(&mut self) -> std::io::Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.disconnect().await
    }

    pub async fn read_coils(&mut self, addr: Address, cnt: Quantity) -> Result<Vec<bool>> {
        retry_modbus!(self, |ctx| ctx.read_coils(addr, cnt).await)
    }

    pub async fn read_discrete_inputs(
        &mut self,
        addr: Address,
        cnt: Quantity,
    ) -> Result<Vec<bool>> {
        retry_modbus!(self, |ctx| ctx.read_discrete_inputs(addr, cnt).await)
    }

    pub async fn read_holding_registers(
        &mut self,
        addr: Address,
        cnt: Quantity,
    ) -> Result<Vec<u16>> {
        retry_modbus!(self, |ctx| ctx.read_holding_registers(addr, cnt).await)
    }

    pub async fn read_input_registers(&mut self, addr: Address, cnt: Quantity) -> Result<Vec<u16>> {
        retry_modbus!(self, |ctx| ctx.read_input_registers(addr, cnt).await)
    }

    pub async fn read_write_multiple_registers(
        &mut self,
        read_addr: Address,
        read_count: Quantity,
        write_addr: Address,
        write_data: &[u16],
    ) -> Result<Vec<u16>> {
        retry_modbus!(self, |ctx| {
            ctx.read_write_multiple_registers(read_addr, read_count, write_addr, write_data)
                .await
        })
    }

    pub async fn write_single_coil(&mut self, addr: Address, coil: bool) -> Result<()> {
        retry_modbus!(self, |ctx| ctx.write_single_coil(addr, coil).await)
    }

    pub async fn write_single_register(&mut self, addr: Address, word: u16) -> Result<()> {
        retry_modbus!(self, |ctx| ctx.write_single_register(addr, word).await)
    }

    pub async fn write_multiple_coils(&mut self, addr: Address, coils: &[bool]) -> Result<()> {
        retry_modbus!(self, |ctx| ctx.write_multiple_coils(addr, coils).await)
    }

    pub async fn write_multiple_registers(&mut self, addr: Address, words: &[u16]) -> Result<()> {
        retry_modbus!(self, |ctx| ctx.write_multiple_registers(addr, words).await)
    }

    pub async fn masked_write_register(
        &mut self,
        addr: Address,
        and_mask: u16,
        or_mask: u16,
    ) -> Result<()> {
        retry_modbus!(self, |ctx| ctx
            .masked_write_register(addr, and_mask, or_mask)
            .await)
    }
}
