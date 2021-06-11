use std::{cmp::Eq, collections::HashMap, future::Future, hash::Hash, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

use tokio::{sync::RwLock, time::sleep};

/// This struct works as an async barrier like OnceCell, but locks disappear from memory after a given time, and there are different callbacks for read and write
pub struct OnceBarrier<T>
where T: Hash + Eq + Clone + Send + Sync + 'static {
    delay: Duration,
    inner: Arc<RwLock<HashMap<T, Arc<RwLock<()>>>>>,
}

impl<T> OnceBarrier<T>
where T: Hash + Eq + Clone + Send + Sync + 'static {
    pub fn new(delay: Duration) -> Self {
        OnceBarrier {
            delay,
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn can_read(&self, key: &T) -> bool {
        let lock = self.inner.read().await;
        if let Some(rw) = lock.get(key) {
            rw.read().await;
            true
        }
        else {
            false
        }
    }

    pub async fn get<Read, Write, ReadFut, WriteFut, Out>(&self, key: T, read_callback: Read, write_callback: Write) -> Out
    where
        // at the moment it's impossible to express the lifetime  of the future, until HRTB is stable
        // therefore we need to pass the owned value T instead of &T
        Read: (FnOnce(&T) -> ReadFut) + Unpin,
        Write: (FnOnce(&T) -> WriteFut) + Unpin,
        ReadFut: Future<Output=Out> + Unpin,
        WriteFut: Future<Output=Out> + Unpin,
    {
        // check if entry exists
        if self.can_read(&key).await {
            let w = Wrapper::new(&key, read_callback);
            return w.await;
        }

        // entry still doesn't exists
        let mut lock = self.inner.write().await;
        if lock.get(&key).is_some() {
            // we are late, drop write lock to dequeue and retry
            drop(lock);
            if self.can_read(&key).await {
                let w = Wrapper::new(&key, read_callback);
                return w.await;
            }
            else {
                unreachable!();
            }
        }

        // create the entry and write-lock it
        let inner = Arc::new(RwLock::new(()));
        lock.insert(key.clone(), Arc::clone(&inner));
        let temp = inner.write().await;
        drop(lock);

        // create the file
        let w = Wrapper::new(&key, write_callback);
        let res = w.await;
        // delete the entry after given time
        let delay = self.delay;
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            sleep(delay).await;
            let mut lock = inner.write().await;
            lock.remove(&key);
        });
        // free write lock and return
        drop(temp);
        res
    }
}

struct Wrapper<'a, T, F, Fut>
where
    F: (FnOnce(&'a T) -> Fut) + Unpin,
    Fut: Future + Unpin + 'a,
{
    param: &'a T,
    inner: Option<F>,
    temp: Option<Fut>,
}

impl<'a, T, F, Fut> Wrapper<'a, T, F, Fut>
where
    F: (FnOnce(&'a T) -> Fut) + Unpin,
    Fut: Future + Unpin + 'a,
{
    fn new(param: &'a T, inner: F) -> Self {
        Wrapper {
            param,
            inner: Some(inner),
            temp: None,
        }
    }
}

impl<'a, T, F, Fut> Future for Wrapper<'a, T, F, Fut>
where
    F: (FnOnce(&'a T) -> Fut) + Unpin,
    Fut: Future + Unpin + 'a,
{
    type Output = Fut::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(f) = this.inner.take() {
            this.temp = Some(f(this.param));
        }
        if let Some(f) = this.temp.as_mut() {
            Pin::new(f).poll(cx)
        }
        else {
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::OnceBarrier;

    #[tokio::test]
    async fn it_works() {
        let ob = OnceBarrier::new(Duration::from_secs(1));
        println!("{}", ob.get(String::from("foo"), |foo| Box::pin(async move { println!("read {}", foo); 1_u8 }), |foo| Box::pin(async move { println!("write {}", foo); 2_u8 })).await);
        println!("{}", ob.get(String::from("foo"), |foo| Box::pin(async move { println!("read {}", foo); 1_u8 }), |foo| Box::pin(async move { println!("write {}", foo); 2_u8 })).await);
    }
}
