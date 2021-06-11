use std::{cmp::Eq, collections::HashMap, future::Future, hash::Hash, sync::Arc, time::Duration};

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

    pub async fn get<'a, Read, ReadFut, Write, WriteFut, Out>(&self, key: T, read_callback: Read, write_callback: Write) -> Out
    where
        Read: Fn(&'a T) -> ReadFut,
        ReadFut: Future<Output=Out> + 'a,
        Write: Fn(&'a T) -> WriteFut,
        WriteFut: Future<Output=Out> + 'a,
    {
        // check if entry exists
        if self.can_read(&key).await {
            return read_callback(&key).await;
        }

        // entry still doesn't exists
        let mut lock = self.inner.write().await;
        if lock.get(&key).is_some() {
            // we are late, drop write lock to dequeue and retry
            drop(lock);
            if self.can_read(&key).await {
                return read_callback(&key).await;
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

        // call write callback
        let res = write_callback(&key).await;
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::OnceBarrier;

    #[tokio::test]
    async fn it_works() {
        let ob = OnceBarrier::new(Duration::from_secs(1));
        println!("{}", ob.get(String::from("foo"), |foo| async move { println!("read {}", foo); 1_u8 }, |foo| async move { println!("write {}", foo); 2_u8 }).await);
        println!("{}", ob.get(String::from("foo"), |foo| async move { println!("read {}", foo); 1_u8 }, |foo| async move { println!("write {}", foo); 2_u8 }).await);
    }
}
