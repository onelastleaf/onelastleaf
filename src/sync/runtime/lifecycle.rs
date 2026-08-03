use super::*;

impl SyncRuntime {
    pub(crate) async fn start(
        config: &ResolvedNodeConfig,
        identities: Arc<IdentityCoordinator>,
        replica: Arc<ReplicaRuntime>,
        logger: Arc<NodeLogger>,
    ) -> Result<Arc<Self>, SyncError> {
        let listener = match config.listen {
            Some(address) => Some(TcpListener::bind(address).await.map_err(|error| {
                SyncError::Unavailable(format!("cannot bind sync listener {address}: {error}"))
            })?),
            None => None,
        };
        let psk = config
            .network_key
            .as_ref()
            .map(derive_noise_psk)
            .map(Arc::new);
        let bindings = replica
            .sync_peer_bindings()
            .await
            .map_err(|_| SyncError::Store)?;
        let target_states = config
            .connect
            .iter()
            .map(|target| (target.to_string(), PeerConnectionState::Pending))
            .collect();
        let (shutdown, _) = watch::channel(false);
        let runtime = Arc::new(Self {
            identities,
            replica,
            logger,
            psk,
            configured_targets: config.connect.clone(),
            target_states: RwLock::new(target_states),
            bindings: RwLock::new(bindings),
            sessions: Mutex::new(HashMap::new()),
            session_changed: Notify::new(),
            shutdown,
            accepting_tasks: AtomicBool::new(true),
            tasks: StdMutex::new(Vec::new()),
        });

        if let Some(listener) = listener {
            let weak = Arc::downgrade(&runtime);
            let shutdown = runtime.shutdown.subscribe();
            runtime.spawn(async move {
                run_listener(weak, listener, shutdown).await;
            });
        }
        for target in runtime.configured_targets.clone() {
            let weak = Arc::downgrade(&runtime);
            let shutdown = runtime.shutdown.subscribe();
            runtime.spawn(async move {
                run_outbound(weak, target, shutdown).await;
            });
        }
        Ok(runtime)
    }

    pub(super) fn spawn(self: &Arc<Self>, future: impl Future<Output = ()> + Send + 'static) {
        if !self.accepting_tasks.load(Ordering::Acquire) {
            return;
        }
        let handle = tokio::spawn(future);
        let mut tasks = self
            .tasks
            .lock()
            .expect("sync task registry lock is poisoned");
        tasks.retain(|task| !task.is_finished());
        if self.accepting_tasks.load(Ordering::Acquire) {
            tasks.push(handle);
        } else {
            handle.abort();
        }
    }

    pub(crate) async fn status(&self) -> Vec<PeerStatus> {
        let bindings = self.bindings.read().await.clone();
        let sessions = self.sessions.lock().await;
        let target_states = self.target_states.read().await;
        let mut represented = BTreeSet::new();
        let mut statuses = Vec::new();
        for target in &self.configured_targets {
            let target = target.to_string();
            let binding = bindings
                .iter()
                .find(|binding| binding.connect_targets.iter().any(|known| known == &target));
            if let Some(binding) = binding {
                represented.insert(binding.identity.node_id());
            }
            let active = binding.and_then(|binding| sessions.get(&binding.identity.node_id()));
            statuses.push(PeerStatus {
                connect_target: Some(target.clone()),
                node: binding.map(|binding| binding.identity.to_proto()),
                connection_state: active.map_or_else(
                    || {
                        target_states
                            .get(&target)
                            .copied()
                            .unwrap_or(PeerConnectionState::Pending) as i32
                    },
                    |_| PeerConnectionState::Ready as i32,
                ),
                direction: active.map_or(PeerConnectionDirection::Outbound as i32, |session| {
                    session.direction.to_proto() as i32
                }),
            });
        }
        for binding in bindings {
            if represented.contains(&binding.identity.node_id()) {
                continue;
            }
            let active = sessions.get(&binding.identity.node_id());
            statuses.push(PeerStatus {
                connect_target: active.and_then(|session| session.connect_target.clone()),
                node: Some(binding.identity.to_proto()),
                connection_state: active.map_or(PeerConnectionState::Pending as i32, |_| {
                    PeerConnectionState::Ready as i32
                }),
                direction: active.map_or(PeerConnectionDirection::Inbound as i32, |session| {
                    session.direction.to_proto() as i32
                }),
            });
        }
        statuses
    }
}
