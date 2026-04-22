use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct ConnectionLimiter {
    inner: Arc<Mutex<ConnectionLimiterState>>,
}

#[derive(Debug)]
struct ConnectionLimiterState {
    max_connections: usize,
    max_connections_per_ip: usize,
    total_connections: usize,
    connections_per_ip: HashMap<IpAddr, usize>,
}

#[derive(Debug)]
pub struct ConnectionPermit {
    limiter: ConnectionLimiter,
    ip: IpAddr,
    released: bool,
}

impl ConnectionLimiter {
    pub fn new(max_connections: usize, max_connections_per_ip: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ConnectionLimiterState {
                max_connections,
                max_connections_per_ip,
                total_connections: 0,
                connections_per_ip: HashMap::new(),
            })),
        }
    }

    pub fn try_acquire(&self, ip: IpAddr) -> Result<ConnectionPermit, String> {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        if state.max_connections > 0 && state.total_connections >= state.max_connections {
            return Err("Reached the maximum number of broker connections".to_string());
        }

        let per_ip = state
            .connections_per_ip
            .get(&ip)
            .copied()
            .unwrap_or_default();
        if state.max_connections_per_ip > 0 && per_ip >= state.max_connections_per_ip {
            return Err(format!(
                "Reached the maximum number of connections for address {}",
                ip
            ));
        }

        state.total_connections += 1;
        state.connections_per_ip.insert(ip, per_ip + 1);

        Ok(ConnectionPermit {
            limiter: self.clone(),
            ip,
            released: false,
        })
    }

    fn release(&self, ip: IpAddr) {
        let mut state = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        state.total_connections = state.total_connections.saturating_sub(1);

        if let Some(count) = state.connections_per_ip.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.connections_per_ip.remove(&ip);
            }
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        if !self.released {
            self.limiter.release(self.ip);
            self.released = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_limit() {
        let limiter = ConnectionLimiter::new(1, 0);
        let _permit = limiter.try_acquire("127.0.0.1".parse().unwrap()).unwrap();
        assert!(limiter.try_acquire("127.0.0.2".parse().unwrap()).is_err());
    }

    #[test]
    fn test_per_ip_limit() {
        let limiter = ConnectionLimiter::new(0, 1);
        let _permit = limiter.try_acquire("127.0.0.1".parse().unwrap()).unwrap();
        assert!(limiter.try_acquire("127.0.0.1".parse().unwrap()).is_err());
        assert!(limiter.try_acquire("127.0.0.2".parse().unwrap()).is_ok());
    }

    #[test]
    fn test_release_on_drop() {
        let limiter = ConnectionLimiter::new(1, 1);
        {
            let _permit = limiter.try_acquire("127.0.0.1".parse().unwrap()).unwrap();
        }
        assert!(limiter.try_acquire("127.0.0.1".parse().unwrap()).is_ok());
    }

    #[test]
    fn test_global_limit_and_per_ip_limit_can_coexist() {
        let limiter = ConnectionLimiter::new(2, 1);
        let _permit_a = limiter.try_acquire("127.0.0.1".parse().unwrap()).unwrap();
        let _permit_b = limiter.try_acquire("127.0.0.2".parse().unwrap()).unwrap();

        assert!(limiter.try_acquire("127.0.0.1".parse().unwrap()).is_err());
        assert!(limiter.try_acquire("127.0.0.3".parse().unwrap()).is_err());
    }
}
