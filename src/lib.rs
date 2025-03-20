use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_modbus::client::{Client, Context, Reader, Writer};
use tokio_modbus::slave::SlaveContext;
use tokio_modbus::{Address, Quantity, Request, Response, Result, Slave};

#[derive(Debug, Clone)]
pub struct ThreadSafeContext {
    inner: Arc<Mutex<Context>>,
}

impl ThreadSafeContext {
    pub fn new(context: Context) -> Self {
        Self {
            inner: Arc::new(Mutex::new(context)),
        }
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub async fn set_slave(&self, slave: Slave) {
        let mut ctx = self.inner.lock().await;
        ctx.set_slave(slave);
    }

    pub async fn call(&mut self, request: Request<'_>) -> Result<Response> {
        let mut ctx = self.inner.lock().await;
        ctx.call(request).await
    }

    pub async fn disconnect(&mut self) -> std::io::Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.disconnect().await
    }

    pub async fn read_coils(&mut self, addr: Address, cnt: Quantity) -> Result<Vec<bool>> {
        let mut ctx = self.inner.lock().await;
        ctx.read_coils(addr, cnt).await
    }

    pub async fn read_discrete_inputs(
        &mut self,
        addr: Address,
        cnt: Quantity,
    ) -> Result<Vec<bool>> {
        let mut ctx = self.inner.lock().await;
        ctx.read_discrete_inputs(addr, cnt).await
    }

    pub async fn read_holding_registers(
        &mut self,
        addr: Address,
        cnt: Quantity,
    ) -> Result<Vec<u16>> {
        let mut ctx = self.inner.lock().await;
        ctx.read_holding_registers(addr, cnt).await
    }

    pub async fn read_input_registers(&mut self, addr: Address, cnt: Quantity) -> Result<Vec<u16>> {
        let mut ctx = self.inner.lock().await;
        ctx.read_input_registers(addr, cnt).await
    }

    pub async fn read_write_multiple_registers(
        &mut self,
        read_addr: Address,
        read_count: Quantity,
        write_addr: Address,
        write_data: &[u16],
    ) -> Result<Vec<u16>> {
        let mut ctx = self.inner.lock().await;
        ctx.read_write_multiple_registers(read_addr, read_count, write_addr, write_data)
            .await
    }

    pub async fn write_single_coil(&mut self, addr: Address, coil: bool) -> Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.write_single_coil(addr, coil).await
    }

    pub async fn write_single_register(&mut self, addr: Address, word: u16) -> Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.write_single_register(addr, word).await
    }

    pub async fn write_multiple_coils(&mut self, addr: Address, coils: &[bool]) -> Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.write_multiple_coils(addr, coils).await
    }

    pub async fn write_multiple_registers(&mut self, addr: Address, words: &[u16]) -> Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.write_multiple_registers(addr, words).await
    }

    pub async fn masked_write_register(
        &mut self,
        addr: Address,
        and_mask: u16,
        or_mask: u16,
    ) -> Result<()> {
        let mut ctx = self.inner.lock().await;
        ctx.masked_write_register(addr, and_mask, or_mask).await
    }
}


